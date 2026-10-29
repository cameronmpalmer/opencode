import { describe, expect } from "bun:test"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionExecution } from "@opencode/core/session/execution"
import { reconcileStaleArchiveState } from "@opencode/core/session/family-archive"
import { SessionProjector } from "@opencode/core/session/projector"
import {
  FamilyArchiveMigrationTable,
  SessionArchiveGuardTable,
  SessionRunLeaseTable,
  SessionTable,
} from "@opencode/core/session/sql"
import { SessionStore } from "@opencode/core/session/store"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Cause, Effect, Exit, Fiber } from "effect"
import { eq } from "drizzle-orm"
import { testEffect } from "./lib/effect"
import { globalProjectNode } from "./lib/project"

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([Database.node, Bus.node, SessionProjector.node, SessionStore.node, Session.node]),
    [
      Bus.node.replace(Bus.configured({ persist: true })),
      Project.node.replace(globalProjectNode),
      SessionExecution.node.replace(SessionExecution.noopLayer),
    ],
  ),
)

const seed = Effect.gen(function* () {
  const db = (yield* Database.Service).db
  yield* db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
    .onConflictDoNothing()
    .run()
  return db
})

const addSession = (
  db: Database.Interface["db"],
  id: Session.ID,
  values: Partial<typeof SessionTable.$inferInsert> = {},
) =>
  db
    .insert(SessionTable)
    .values({
      id,
      project_id: Project.ID.global,
      slug: id,
      directory: "/project",
      version: "1",
      time_created: 1,
      time_updated: 1,
      ...values,
    })
    .run()
    .pipe(Effect.orDie)

const addLease = (db: Database.Interface["db"], id: Session.ID, heartbeat: number) =>
  db
    .insert(SessionRunLeaseTable)
    .values({ session_id: id, lease_id: "lease", heartbeat_at: heartbeat })
    .run()
    .pipe(Effect.orDie)

const row = (db: Database.Interface["db"], id: Session.ID) =>
  db.select().from(SessionTable).where(eq(SessionTable.id, id)).get().pipe(Effect.orDie)

const guardCount = (db: Database.Interface["db"]) =>
  db
    .select()
    .from(SessionArchiveGuardTable)
    .all()
    .pipe(
      Effect.orDie,
      Effect.map((rows) => rows.length),
    )

const migrationRows = (db: Database.Interface["db"]) =>
  db.select().from(FamilyArchiveMigrationTable).all().pipe(Effect.orDie)

describe("family archive service", () => {
  it.effect("archives a root and its descendants", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const db = yield* seed
      const root = Session.ID.make("ses_root")
      const first = Session.ID.make("ses_child_a")
      const second = Session.ID.make("ses_child_b")
      yield* addSession(db, root)
      yield* addSession(db, first, { parent_id: root })
      yield* addSession(db, second, { parent_id: root })

      expect(yield* session.archiveSubtree(root, { archivedAt: 500 })).toEqual({
        root_id: root,
        archived_ids: [root, first, second],
        already_archived_ids: [],
        cancelled_ids: [],
        archive_time: 500,
      })

      for (const id of [root, first, second]) {
        expect(yield* row(db, id)).toMatchObject({
          time_archived: 500,
          family_archive_id: root,
          archive_cause: id === root ? "user" : "cascade",
          archive_state: null,
          archive_state_at: null,
        })
      }
      expect(yield* migrationRows(db)).toEqual([
        { root_session_id: root, root_archived_at: 500, applied_at: expect.any(Number) },
      ])
      expect(yield* guardCount(db)).toBe(0)
    }),
  )

  it.effect("archives a single session with an explicit time", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const db = yield* seed
      const root = Session.ID.make("ses_single")
      yield* addSession(db, root)

      yield* session.setArchived(root, 700)

      expect(yield* row(db, root)).toMatchObject({
        time_archived: 700,
        family_archive_id: null,
        archive_cause: "user",
        archive_state: null,
        archive_state_at: null,
      })
      expect(yield* guardCount(db)).toBe(0)
    }),
  )

  it.effect("unarchives the family and clears the fields", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const db = yield* seed
      const root = Session.ID.make("ses_unarchive_root")
      const first = Session.ID.make("ses_unarchive_a")
      const second = Session.ID.make("ses_unarchive_b")
      yield* addSession(db, root)
      yield* addSession(db, first, { parent_id: root })
      yield* addSession(db, second, { parent_id: root })
      yield* session.archiveSubtree(root)

      expect(yield* session.unarchiveSubtree(root)).toEqual({
        root_id: root,
        restored_ids: [root, first, second],
        still_archived_ids: [],
      })
      for (const id of [root, first, second]) {
        expect(yield* row(db, id)).toMatchObject({
          time_archived: null,
          family_archive_id: null,
          archive_cause: null,
          archive_state: null,
          archive_state_at: null,
        })
      }
      expect(yield* migrationRows(db)).toEqual([
        { root_session_id: root, root_archived_at: expect.any(Number), applied_at: expect.any(Number) },
      ])
      expect(yield* guardCount(db)).toBe(0)
    }),
  )

  it.effect("leaves independently archived children alone on unarchive", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const db = yield* seed
      const root = Session.ID.make("ses_independent_root")
      const cascade = Session.ID.make("ses_independent_child")
      const independent = Session.ID.make("ses_independent_member")
      yield* addSession(db, root)
      yield* addSession(db, cascade, { parent_id: root })
      yield* addSession(db, independent, {
        parent_id: root,
        time_archived: 1,
        family_archive_id: independent,
        archive_cause: "user",
      })

      yield* session.archiveSubtree(root)
      expect(yield* row(db, independent)).toMatchObject({
        time_archived: 1,
        family_archive_id: independent,
        archive_cause: "user",
      })

      expect(yield* session.unarchiveSubtree(root)).toEqual({
        root_id: root,
        restored_ids: [root, cascade],
        still_archived_ids: [independent],
      })
      expect(yield* row(db, root)).toMatchObject({
        time_archived: null,
        family_archive_id: null,
        archive_cause: null,
      })
      expect(yield* row(db, cascade)).toMatchObject({
        time_archived: null,
        family_archive_id: null,
        archive_cause: null,
      })
      expect(yield* row(db, independent)).toMatchObject({
        time_archived: 1,
        family_archive_id: independent,
        archive_cause: "user",
      })
    }),
  )

  it.effect("rejects archiving a direct family member", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const db = yield* seed
      const root = Session.ID.make("ses_family_root")
      const child = Session.ID.make("ses_family_child")
      yield* addSession(db, root, { time_archived: 1, family_archive_id: root, archive_cause: "user" })
      yield* addSession(db, child, {
        parent_id: root,
        time_archived: 1,
        family_archive_id: root,
        archive_cause: "cascade",
      })

      const subtree = yield* Effect.flip(session.archiveSubtree(child))
      expect(subtree).toBeInstanceOf(Session.FamilyMemberError)
      expect(subtree).toMatchObject({ sessionID: child, familyRootID: root })

      const direct = yield* Effect.flip(session.setArchived(child, 123))
      expect(direct).toBeInstanceOf(Session.FamilyMemberError)
      expect(direct).toMatchObject({ sessionID: child, familyRootID: root })
    }),
  )

  it.effect("rejects an archive time before creation", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const db = yield* seed
      const root = Session.ID.make("ses_old_time")
      yield* addSession(db, root)

      const error = yield* Effect.flip(session.archiveSubtree(root, { archivedAt: 0 }))
      expect(error).toBeInstanceOf(Session.InvalidArchiveTimeError)
      expect(error).toMatchObject({ sessionID: root, time: 0 })
    }),
  )

  it.effect("rejects archive when a member is already archiving", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const db = yield* seed
      const root = Session.ID.make("ses_archiving_root")
      const child = Session.ID.make("ses_archiving_child")
      yield* addSession(db, root)
      yield* addSession(db, child, { parent_id: root, archive_state: "archiving", archive_state_at: 1 })

      const error = yield* Effect.flip(session.archiveSubtree(root))
      expect(error).toBeInstanceOf(Session.ArchiveInProgressError)
      expect(error).toMatchObject({ sessionID: root, sessionIDs: [child] })
    }),
  )

  it.live("blocks archive on a live lease and cleans up the markers", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const db = yield* seed
      const root = Session.ID.make("ses_lease_root")
      const child = Session.ID.make("ses_lease_child")
      yield* addSession(db, root)
      yield* addSession(db, child, { parent_id: root })
      yield* addLease(db, child, Date.now())

      const error = yield* Effect.flip(session.archiveSubtree(root, { waitTimeoutMs: 10 }))
      expect(error).toBeInstanceOf(Session.ArchiveBlockedError)
      expect(error).toMatchObject({ sessionID: root, sessionIDs: [child] })

      expect(yield* guardCount(db)).toBe(0)
      const rows = yield* db.select().from(SessionTable).all()
      expect(rows.every((entry) => entry.archive_state === null)).toBe(true)
      const lease = yield* db.select().from(SessionRunLeaseTable).get()
      expect(lease?.cancel_requested).toBe(0)
    }),
  )

  it.live("marks a fully archived family while the cancellation path runs", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const db = yield* seed
      const root = Session.ID.make("ses_fully_root")
      const child = Session.ID.make("ses_fully_child")
      yield* addSession(db, root, { time_archived: 100, family_archive_id: root, archive_cause: "user" })
      yield* addSession(db, child, {
        parent_id: root,
        time_archived: 100,
        family_archive_id: root,
        archive_cause: "cascade",
      })
      yield* addLease(db, child, Date.now())

      const first = yield* session
        .archiveSubtree(root, { waitTimeoutMs: 300 })
        .pipe(Effect.exit, Effect.forkScoped({ startImmediately: true }))
      yield* Effect.sleep("50 millis")

      const marked = yield* db.select().from(SessionTable).all()
      expect(marked.every((entry) => entry.archive_state === "archiving")).toBe(true)

      const concurrent = yield* Effect.flip(session.archiveSubtree(root))
      expect(concurrent).toBeInstanceOf(Session.ArchiveInProgressError)
      expect(concurrent).toMatchObject({ sessionID: root })

      const exit = yield* Fiber.join(first)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(Session.ArchiveBlockedError)

      const cleared = yield* db.select().from(SessionTable).all()
      expect(cleared.every((entry) => entry.archive_state === null)).toBe(true)
      expect(yield* guardCount(db)).toBe(0)
    }),
  )

  it.live("removes an orphan migration snapshot when the attempt fails", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const db = yield* seed
      const root = Session.ID.make("ses_orphan_root")
      const child = Session.ID.make("ses_orphan_child")
      yield* addSession(db, root)
      yield* addSession(db, child, { parent_id: root })
      // Simulate a prior attempt that inserted the snapshot before failing.
      yield* db
        .insert(FamilyArchiveMigrationTable)
        .values({ root_session_id: root, root_archived_at: 123, applied_at: 123 })
        .run()
        .pipe(Effect.orDie)
      yield* addLease(db, child, Date.now())

      const error = yield* Effect.flip(session.archiveSubtree(root, { waitTimeoutMs: 10 }))
      expect(error).toBeInstanceOf(Session.ArchiveBlockedError)
      expect(yield* migrationRows(db)).toEqual([])
    }),
  )

  it.effect("reconciles stale guards and archiving markers", () =>
    Effect.gen(function* () {
      const db = yield* seed
      const root = Session.ID.make("ses_stale_root")
      yield* addSession(db, root, { archive_state: "archiving", archive_state_at: 123 })
      yield* db.insert(SessionArchiveGuardTable).values({ session_id: root }).run().pipe(Effect.orDie)

      yield* reconcileStaleArchiveState(db)

      expect(yield* guardCount(db)).toBe(0)
      expect(yield* row(db, root)).toMatchObject({ archive_state: null, archive_state_at: null })
    }),
  )
})
