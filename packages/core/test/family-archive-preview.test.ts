import { describe, expect } from "bun:test"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionExecution } from "@opencode/core/session/execution"
import { effectiveFamilyKeySql, loadSubtree, orderSubtree } from "@opencode/core/session/family-archive"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionImportExemptTable, SessionRunLeaseTable, SessionTable } from "@opencode/core/session/sql"
import { SessionStore } from "@opencode/core/session/store"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { eq } from "drizzle-orm"
import { Effect, Layer } from "effect"
import { testEffect } from "./lib/effect"
import { globalProjectNode } from "./lib/project"

const activeID = Session.ID.make("ses_active")
const execution = Layer.succeed(
  SessionExecution.Service,
  SessionExecution.Service.of({
    active: Effect.succeed(new Set([activeID])),
    isActive: (id) => Effect.succeed(id === activeID),
    resume: () => Effect.void,
    wake: () => Effect.void,
    interrupt: () => Effect.succeed(false),
    awaitIdle: () => Effect.void,
  }),
)

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([Database.node, Bus.node, SessionProjector.node, SessionStore.node, Session.node]),
    [
      Bus.node.replace(Bus.configured({ persist: true })),
      Project.node.replace(globalProjectNode),
      SessionExecution.node.replace(execution),
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
  Effect.gen(function* () {
    // Test seeds may insert a child under an already archived parent. The
    // production import path uses the same short-lived exemption.
    yield* db
      .insert(SessionImportExemptTable)
      .values({ session_id: id, expires_at: Date.now() + 60_000 })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* db
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
    yield* db
      .delete(SessionImportExemptTable)
      .where(eq(SessionImportExemptTable.session_id, id))
      .run()
      .pipe(Effect.orDie)
  })

const addLease = (db: Database.Interface["db"], id: Session.ID, heartbeat: number) =>
  db
    .insert(SessionRunLeaseTable)
    .values({ session_id: id, lease_id: "lease", heartbeat_at: heartbeat })
    .run()
    .pipe(Effect.orDie)

describe("family archive helpers", () => {
  it.effect("resolves the effective family key for every shape", () =>
    Effect.gen(function* () {
      const db = yield* seed
      const root = Session.ID.make("ses_p")
      const child = Session.ID.make("ses_c")
      const grandchild = Session.ID.make("ses_g")
      const independent = Session.ID.make("ses_i")
      const legacy = Session.ID.make("ses_l")
      const legacyGrandchild = Session.ID.make("ses_g2")
      const standalone = Session.ID.make("ses_u")

      yield* addSession(db, root, { time_archived: 1, family_archive_id: root, archive_cause: "user" })
      yield* addSession(db, child, {
        parent_id: root,
        time_archived: 1,
        family_archive_id: root,
        archive_cause: "cascade",
      })
      yield* addSession(db, grandchild, {
        parent_id: child,
        time_archived: 1,
        family_archive_id: root,
        archive_cause: "cascade",
      })
      yield* addSession(db, independent, {
        parent_id: root,
        time_archived: 1,
        family_archive_id: independent,
        archive_cause: "user",
      })
      yield* addSession(db, legacy, { parent_id: root, time_archived: 1 })
      yield* addSession(db, legacyGrandchild, { parent_id: legacy })
      yield* addSession(db, standalone, { parent_id: root, time_archived: 1, archive_cause: "user" })

      const rows = yield* db.all<{ session_id: string; family_key: string }>(effectiveFamilyKeySql)
      expect(new Map(rows.map((row) => [row.session_id, row.family_key]))).toEqual(
        new Map([
          [root, root],
          [child, root],
          [grandchild, root],
          [independent, independent],
          [legacy, root],
          [legacyGrandchild, root],
          [standalone, standalone],
        ]),
      )
    }),
  )

  it.effect("previews subtree activity from live leases and the execution coordinator", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const db = yield* seed
      const root = Session.ID.make("ses_preview_root")
      const leased = Session.ID.make("ses_lease")
      yield* addSession(db, root)
      yield* addSession(db, leased, { parent_id: root })
      yield* addSession(db, activeID, { parent_id: root })
      yield* addLease(db, leased, Date.now())
      // A stale lease is excluded by the heartbeat cutoff.
      yield* addLease(db, root, Date.now() - 120_000)

      expect(yield* session.archivePreview(root)).toEqual({
        root_id: root,
        descendant_count: 2,
        active_count: 2,
        active_session_ids: [leased, activeID].sort(),
      })

      const rows = yield* loadSubtree(db, root)
      expect(orderSubtree(rows).map((row) => row.id)).toEqual([root, activeID, leased])
    }),
  )

  it.effect("rejects previewing a direct family member", () =>
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

      const error = yield* Effect.flip(session.archivePreview(child))
      expect(error).toBeInstanceOf(Session.FamilyMemberError)
      expect(error).toMatchObject({
        sessionID: child,
        familyRootID: root,
      })
    }),
  )
})
