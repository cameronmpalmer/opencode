import { describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionArchiveGuardTable, SessionRunLeaseTable, SessionTable } from "@opencode-ai/core/session/sql"
import { Database } from "@opencode-ai/core/database/database"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { and, eq, sql } from "drizzle-orm"
import { Effect } from "effect"
import { Project } from "@/project/project"
import { Session as SessionNs } from "@/session/session"
import { testEffect } from "../lib/effect"

const it = testEffect(
  LayerNode.compile(
    LayerNode.group([SessionNs.node, SessionProjector.node, Project.node, CrossSpawnSpawner.node, Database.node]),
    [[SessionExecution.node, SessionExecution.noopLayer]],
  ),
)

const seedArchiveFields = (
  db: Database.Interface["db"],
  sessionID: SessionNs.Info["id"],
  fields: Partial<typeof SessionTable.$inferInsert>,
) =>
  db
    .transaction(
      (tx) =>
        Effect.gen(function* () {
          yield* tx.insert(SessionArchiveGuardTable).values({ session_id: sessionID }).onConflictDoNothing().run()
          yield* tx.update(SessionTable).set(fields).where(eq(SessionTable.id, sessionID)).run()
          yield* tx.delete(SessionArchiveGuardTable).where(eq(SessionArchiveGuardTable.session_id, sessionID)).run()
        }),
      { behavior: "immediate" },
    )
    .pipe(Effect.orDie)

const stopLeaseOnCancel = (db: Database.Interface["db"], sessionID: SessionNs.Info["id"], leaseID: string) =>
  Effect.gen(function* () {
    while (true) {
      const lease = yield* db
        .select()
        .from(SessionRunLeaseTable)
        .where(and(eq(SessionRunLeaseTable.session_id, sessionID), eq(SessionRunLeaseTable.lease_id, leaseID)))
        .get()
        .pipe(Effect.orDie)
      if (!lease) return
      if (lease.cancel_requested === 1) {
        yield* db
          .delete(SessionRunLeaseTable)
          .where(and(eq(SessionRunLeaseTable.session_id, sessionID), eq(SessionRunLeaseTable.lease_id, leaseID)))
          .run()
          .pipe(Effect.orDie)
        return
      }
      yield* Effect.sleep("10 millis")
    }
  }).pipe(Effect.forkScoped)

const expectNoGuards = (db: Database.Interface["db"]) =>
  db
    .select()
    .from(SessionArchiveGuardTable)
    .all()
    .pipe(Effect.orDie)
    .pipe(
      Effect.flatMap((guards) =>
        Effect.sync(() => {
          expect(guards).toEqual([])
        }),
      ),
    )

const seedFamily = (session: SessionNs.Interface) =>
  Effect.gen(function* () {
    const P = yield* session.create({ title: "P" })
    const C = yield* session.create({ parentID: P.id, title: "C" })
    const G = yield* session.create({ parentID: C.id, title: "G" })
    return { P, C, G }
  })

const readFamily = (
  session: SessionNs.Interface,
  family: { P: { id: SessionNs.Info["id"] }; C: { id: SessionNs.Info["id"] }; G: { id: SessionNs.Info["id"] } },
) =>
  Effect.gen(function* () {
    return {
      P: yield* session.get(family.P.id),
      C: yield* session.get(family.C.id),
      G: yield* session.get(family.G.id),
    }
  })

describe("family archive", () => {
  it.instance("archive archives the whole family and unarchive restores every cascade member", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const family = yield* seedFamily(session)
      const archivedAt = family.P.time.created

      const result = yield* session.archiveSubtree({ sessionID: family.P.id, archivedAt })

      expect(result.archive_time).toBe(archivedAt)
      expect(result.archived_ids).toEqual(expect.arrayContaining([family.P.id, family.C.id, family.G.id]))

      const rows = yield* readFamily(session, family)
      expect(rows.P.time.archived).toBe(archivedAt)
      expect(rows.C.time.archived).toBe(archivedAt)
      expect(rows.G.time.archived).toBe(archivedAt)
      expect(rows.P.archiveCause).toBe("user")
      expect(rows.C.archiveCause).toBe("cascade")
      expect(rows.G.archiveCause).toBe("cascade")
      expect(rows.C.familyArchiveID).toBe(family.P.id)
      expect(rows.P.archiveState).toBeUndefined()
      expect(rows.C.archiveState).toBeUndefined()

      const unarchive = yield* session.unarchiveSubtree({ sessionID: family.P.id })
      expect(unarchive.restored_ids).toEqual(expect.arrayContaining([family.P.id, family.C.id, family.G.id]))
      expect(unarchive.still_archived_ids).toEqual([])

      const restored = yield* readFamily(session, family)
      expect(restored.P.time.archived).toBeUndefined()
      expect(restored.C.time.archived).toBeUndefined()
      expect(restored.G.time.archived).toBeUndefined()
      expect(restored.G.familyArchiveID).toBeUndefined()
      expect(restored.G.archiveCause).toBeUndefined()
      const { db } = yield* Database.Service
      yield* expectNoGuards(db)
    }),
  )

  it.instance(
    "archive of an already-archived root preserves its migration cause, clears markers, and cancels a live lease",
    () =>
      Effect.gen(function* () {
        const session = yield* SessionNs.Service
        const { db } = yield* Database.Service
        const family = yield* seedFamily(session)
        const rootTime = family.P.time.created + 1_000

        yield* seedArchiveFields(db, family.P.id, {
          time_archived: rootTime,
          family_archive_id: family.P.id,
          archive_cause: "migration",
        })
        yield* seedArchiveFields(db, family.C.id, {
          time_archived: rootTime,
          family_archive_id: family.P.id,
          archive_cause: "cascade",
        })

        yield* db
          .insert(SessionRunLeaseTable)
          .values({ session_id: family.C.id, lease_id: "lease-1", heartbeat_at: Date.now() })
          .run()
          .pipe(Effect.orDie)

        yield* stopLeaseOnCancel(db, family.C.id, "lease-1")

        const result = yield* session.archiveSubtree({ sessionID: family.P.id, waitTimeoutMs: 2_000 })

        expect(result.archive_time).toBe(rootTime)
        expect(result.cancelled_ids).toContain(family.C.id)
        expect(result.already_archived_ids).toEqual(expect.arrayContaining([family.P.id, family.C.id]))

        const rows = yield* readFamily(session, family)
        expect(rows.P.archiveCause).toBe("migration")
        expect(rows.P.time.archived).toBe(rootTime)
        expect(rows.C.time.archived).toBe(rootTime)
        expect(rows.G.time.archived).toBe(rootTime)
        expect(rows.G.archiveCause).toBe("cascade")
        expect(rows.P.archiveState).toBeUndefined()
        expect(rows.P.archiveStateAt).toBeUndefined()
        expect(rows.G.archiveState).toBeUndefined()

        const leases = yield* db.select().from(SessionRunLeaseTable).all().pipe(Effect.orDie)
        expect(leases).toEqual([])
      }),
  )

  it.instance("unarchive leaves an independently archived child and its family archived", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const family = yield* seedFamily(session)

      yield* session.archiveSubtree({ sessionID: family.C.id })
      const independent = yield* session.get(family.C.id)
      expect(independent.archiveCause).toBe("user")
      expect(independent.familyArchiveID).toBe(family.C.id)

      yield* session.archiveSubtree({ sessionID: family.P.id })
      const unarchive = yield* session.unarchiveSubtree({ sessionID: family.P.id })

      expect(unarchive.restored_ids).toEqual([family.P.id])
      expect(unarchive.still_archived_ids).toEqual(expect.arrayContaining([family.C.id, family.G.id]))

      const rows = yield* readFamily(session, family)
      expect(rows.P.time.archived).toBeUndefined()
      expect(rows.C.time.archived).not.toBeUndefined()
      expect(rows.G.time.archived).not.toBeUndefined()
      expect(rows.C.archiveCause).toBe("user")
      expect(rows.G.familyArchiveID).toBe(family.C.id)
    }),
  )

  it.instance("rejects a direct archive of a cascade member with the family root", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const family = yield* seedFamily(session)
      yield* session.archiveSubtree({ sessionID: family.P.id })

      const exit = yield* Effect.exit(session.archiveSubtree({ sessionID: family.C.id }))
      expect(exit._tag).toBe("Failure")
      if (exit._tag === "Failure") {
        expect(JSON.stringify(exit.cause)).toContain("SessionFamilyMemberError")
        expect(JSON.stringify(exit.cause)).toContain(family.P.id)
      }
    }),
  )

  it.instance("preview reports descendants and a running target", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const family = yield* seedFamily(session)
      const preview = yield* session.archivePreview({
        sessionID: family.P.id,
        active: Effect.succeed(new Set([family.P.id])),
      })

      expect(preview.root_id).toBe(family.P.id)
      expect(preview.descendant_count).toBe(2)
      expect(preview.active_count).toBe(1)
      expect(preview.active_session_ids).toEqual([family.P.id])
    }),
  )

  it.instance("archive and unarchive change archive fields only while a guard row exists", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const { db } = yield* Database.Service
      yield* db
        .run(
          sql.raw(`
            CREATE TRIGGER test_archive_guard
            BEFORE UPDATE ON session
            WHEN (
              OLD.time_archived IS NOT NEW.time_archived OR
              OLD.family_archive_id IS NOT NEW.family_archive_id OR
              OLD.archive_cause IS NOT NEW.archive_cause OR
              OLD.archive_state IS NOT NEW.archive_state OR
              OLD.archive_state_at IS NOT NEW.archive_state_at
            ) AND NOT EXISTS (SELECT 1 FROM session_archive_guard WHERE session_id = NEW.id)
            BEGIN
              SELECT RAISE(ABORT, 'archive guard required');
            END
          `),
        )
        .pipe(Effect.orDie)
      yield* Effect.addFinalizer(() => db.run(sql.raw("DROP TRIGGER IF EXISTS test_archive_guard")).pipe(Effect.orDie))

      const family = yield* seedFamily(session)
      yield* session.archiveSubtree({ sessionID: family.P.id })
      yield* session.unarchiveSubtree({ sessionID: family.P.id })

      const rows = yield* readFamily(session, family)
      expect(rows.P.time.archived).toBeUndefined()
      expect(rows.C.archiveState).toBeUndefined()
    }),
  )

  it.instance("a live lease timeout fails closed, clears markers, and restores cancel_requested", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const { db } = yield* Database.Service
      const family = yield* seedFamily(session)
      yield* db
        .insert(SessionRunLeaseTable)
        .values({ session_id: family.C.id, lease_id: "lease-timeout", heartbeat_at: Date.now(), cancel_requested: 0 })
        .run()
        .pipe(Effect.orDie)

      const exit = yield* Effect.exit(session.archiveSubtree({ sessionID: family.P.id, waitTimeoutMs: 60 }))
      expect(exit._tag).toBe("Failure")
      if (exit._tag === "Failure") {
        expect(JSON.stringify(exit.cause)).toContain("SessionArchiveBlockedError")
      }

      const rows = yield* readFamily(session, family)
      expect(rows.P.time.archived).toBeUndefined()
      expect(rows.C.time.archived).toBeUndefined()
      expect(rows.G.time.archived).toBeUndefined()
      expect(rows.P.archiveState).toBeUndefined()
      expect(rows.P.archiveStateAt).toBeUndefined()
      expect(rows.C.archiveState).toBeUndefined()

      const lease = yield* db
        .select()
        .from(SessionRunLeaseTable)
        .where(eq(SessionRunLeaseTable.session_id, family.C.id))
        .get()
        .pipe(Effect.orDie)
      expect(lease?.cancel_requested).toBe(0)
      yield* expectNoGuards(db)
    }),
  )

  it.instance("a timeout restores an already-cancelled lease to its recorded value", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const { db } = yield* Database.Service
      const family = yield* seedFamily(session)
      yield* db
        .insert(SessionRunLeaseTable)
        .values({ session_id: family.C.id, lease_id: "lease-prior", heartbeat_at: Date.now(), cancel_requested: 1 })
        .run()
        .pipe(Effect.orDie)

      const exit = yield* Effect.exit(session.archiveSubtree({ sessionID: family.P.id, waitTimeoutMs: 60 }))
      expect(exit._tag).toBe("Failure")

      const lease = yield* db
        .select()
        .from(SessionRunLeaseTable)
        .where(eq(SessionRunLeaseTable.session_id, family.C.id))
        .get()
        .pipe(Effect.orDie)
      expect(lease?.cancel_requested).toBe(1)
      const rows = yield* readFamily(session, family)
      expect(rows.P.archiveState).toBeUndefined()
      yield* expectNoGuards(db)
    }),
  )

  it.instance("archiving an already archived family is idempotent", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const family = yield* seedFamily(session)
      const first = yield* session.archiveSubtree({ sessionID: family.P.id })
      const second = yield* session.archiveSubtree({ sessionID: family.P.id })

      expect(second.archived_ids).toEqual([])
      expect(second.already_archived_ids).toEqual(expect.arrayContaining([family.P.id, family.C.id, family.G.id]))
      expect(second.archive_time).toBe(first.archive_time)
    }),
  )

  it.instance("a nested independent owner keeps its own archive time for its descendants", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const family = yield* seedFamily(session)
      const childTime = family.C.time.created + 100

      yield* session.setArchived({ sessionID: family.C.id, time: childTime })
      const result = yield* session.archiveSubtree({
        sessionID: family.P.id,
        archivedAt: family.P.time.created,
      })

      expect(result.archive_time).toBe(family.P.time.created)
      const rows = yield* readFamily(session, family)
      expect(rows.P.time.archived).toBe(family.P.time.created)
      expect(rows.C.time.archived).toBe(childTime)
      expect(rows.G.time.archived).toBe(childTime)
      expect(rows.G.familyArchiveID).toBe(family.C.id)
    }),
  )

  it.instance("rejects an archive time before session creation and clamps a future time", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const family = yield* seedFamily(session)

      const before = yield* Effect.exit(
        session.archiveSubtree({ sessionID: family.P.id, archivedAt: family.P.time.created - 1 }),
      )
      expect(before._tag).toBe("Failure")
      if (before._tag === "Failure") {
        expect(JSON.stringify(before.cause)).toContain("SessionInvalidArchiveTimeError")
      }

      const started = Date.now()
      const result = yield* session.archiveSubtree({
        sessionID: family.P.id,
        archivedAt: Date.now() + 60_000,
      })
      const finished = Date.now()
      expect(result.archive_time).toBeGreaterThanOrEqual(started)
      expect(result.archive_time).toBeLessThanOrEqual(finished)
    }),
  )

  it.instance("rejects unarchiving a cascade member and names the family root", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const family = yield* seedFamily(session)
      yield* session.archiveSubtree({ sessionID: family.P.id })

      const exit = yield* Effect.exit(session.unarchiveSubtree({ sessionID: family.C.id }))
      expect(exit._tag).toBe("Failure")
      if (exit._tag === "Failure") {
        expect(JSON.stringify(exit.cause)).toContain("SessionFamilyMemberError")
        expect(JSON.stringify(exit.cause)).toContain(family.P.id)
      }
    }),
  )

  it.instance("rejects archive and unarchive while a member is archiving", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const { db } = yield* Database.Service
      const family = yield* seedFamily(session)
      const rootTime = family.P.time.created + 500
      yield* seedArchiveFields(db, family.P.id, {
        time_archived: rootTime,
        family_archive_id: family.P.id,
        archive_cause: "user",
        archive_state: "archiving",
        archive_state_at: Date.now(),
      })

      const archiveExit = yield* Effect.exit(session.archiveSubtree({ sessionID: family.P.id }))
      expect(archiveExit._tag).toBe("Failure")
      if (archiveExit._tag === "Failure") {
        expect(JSON.stringify(archiveExit.cause)).toContain("SessionArchiveInProgressError")
      }

      const unarchiveExit = yield* Effect.exit(session.unarchiveSubtree({ sessionID: family.P.id }))
      expect(unarchiveExit._tag).toBe("Failure")
      if (unarchiveExit._tag === "Failure") {
        expect(JSON.stringify(unarchiveExit.cause)).toContain("SessionArchiveInProgressError")
      }
      yield* expectNoGuards(db)
    }),
  )

  it.instance("a fully archived family with a live lease cancels, preserves fields, and completes", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const { db } = yield* Database.Service
      const family = yield* seedFamily(session)
      const rootTime = family.P.time.created + 750
      yield* seedArchiveFields(db, family.P.id, {
        time_archived: rootTime,
        family_archive_id: family.P.id,
        archive_cause: "user",
      })
      yield* seedArchiveFields(db, family.C.id, {
        time_archived: rootTime,
        family_archive_id: family.P.id,
        archive_cause: "cascade",
      })
      yield* seedArchiveFields(db, family.G.id, {
        time_archived: rootTime,
        family_archive_id: family.P.id,
        archive_cause: "cascade",
      })
      yield* db
        .insert(SessionRunLeaseTable)
        .values({ session_id: family.C.id, lease_id: "lease-live", heartbeat_at: Date.now() })
        .run()
        .pipe(Effect.orDie)
      yield* stopLeaseOnCancel(db, family.C.id, "lease-live")

      const result = yield* session.archiveSubtree({ sessionID: family.P.id, waitTimeoutMs: 2_000 })
      expect(result.archived_ids).toEqual([])
      expect(result.cancelled_ids).toContain(family.C.id)

      const rows = yield* readFamily(session, family)
      expect(rows.P.time.archived).toBe(rootTime)
      expect(rows.C.time.archived).toBe(rootTime)
      expect(rows.G.time.archived).toBe(rootTime)
      expect(rows.P.archiveCause).toBe("user")
      expect(rows.C.archiveCause).toBe("cascade")
      expect(rows.P.archiveState).toBeUndefined()
      expect(rows.C.archiveState).toBeUndefined()
      yield* expectNoGuards(db)
    }),
  )

  it.instance("a final-transaction failure clears markers and surfaces the original error", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const { db } = yield* Database.Service
      const family = yield* seedFamily(session)
      yield* db
        .run(
          sql.raw(`
            CREATE TRIGGER test_final_fault
            BEFORE INSERT ON family_archive_migration
            WHEN NEW.root_session_id = '${family.P.id}'
            BEGIN
              SELECT RAISE(ABORT, 'injected final failure');
            END
          `),
        )
        .pipe(Effect.orDie)
      yield* Effect.addFinalizer(() => db.run(sql.raw("DROP TRIGGER IF EXISTS test_final_fault")).pipe(Effect.orDie))

      const exit = yield* Effect.exit(session.archiveSubtree({ sessionID: family.P.id }))
      expect(exit._tag).toBe("Failure")

      const rows = yield* readFamily(session, family)
      expect(rows.P.time.archived).toBeUndefined()
      expect(rows.C.time.archived).toBeUndefined()
      expect(rows.G.time.archived).toBeUndefined()
      expect(rows.P.archiveState).toBeUndefined()
      expect(rows.P.archiveStateAt).toBeUndefined()
      expect(rows.C.archiveState).toBeUndefined()
      yield* expectNoGuards(db)
    }),
  )
})
