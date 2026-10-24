import { describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionRunLeaseTable, SessionTable } from "@opencode-ai/core/session/sql"
import { Database } from "@opencode-ai/core/database/database"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { eq } from "drizzle-orm"
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
  it.instance("archive cancels children, archives descendants, and unarchive restores only cascade members", () =>
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

        yield* db
          .update(SessionTable)
          .set({ time_archived: rootTime, family_archive_id: family.P.id, archive_cause: "migration" })
          .where(eq(SessionTable.id, family.P.id))
          .run()
          .pipe(Effect.orDie)
        yield* db
          .update(SessionTable)
          .set({ time_archived: rootTime, family_archive_id: family.P.id, archive_cause: "cascade" })
          .where(eq(SessionTable.id, family.C.id))
          .run()
          .pipe(Effect.orDie)

        yield* db
          .insert(SessionRunLeaseTable)
          .values({ session_id: family.C.id, lease_id: "lease-1", heartbeat_at: Date.now() })
          .run()
          .pipe(Effect.orDie)

        yield* Effect.forkScoped(
          Effect.gen(function* () {
            yield* Effect.sleep("40 millis")
            yield* db
              .delete(SessionRunLeaseTable)
              .where(eq(SessionRunLeaseTable.session_id, family.C.id))
              .run()
              .pipe(Effect.orDie)
          }),
        )

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
      const preview = yield* session.archivePreview({ sessionID: family.P.id })

      expect(preview.root_id).toBe(family.P.id)
      expect(preview.descendant_count).toBe(2)
      expect(preview.active_count).toBe(0)
      expect(preview.active_session_ids).toEqual([])
    }),
  )
})
