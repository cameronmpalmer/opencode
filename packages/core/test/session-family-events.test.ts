import { describe, expect } from "bun:test"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Location } from "@opencode/core/location"
import { Project } from "@opencode/core/project"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionArchiveGuardTable, SessionTable } from "@opencode/core/session/sql"
import { SessionStore } from "@opencode/core/session/store"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Effect } from "effect"
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
const location = Location.Ref.make({ directory: AbsolutePath.make("/project") })

describe("family archive session events", () => {
  it.effect("archives with family fields and maps them back", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const bus = yield* Bus.Service
      const { db } = yield* Database.Service
      const created = yield* session.create({ location })

      yield* db
        .insert(SessionArchiveGuardTable)
        .values({ session_id: created.id })
        .onConflictDoNothing()
        .run()
        .pipe(Effect.orDie)

      yield* bus.publish(SessionEvent.Archived, {
        sessionID: created.id,
        familyArchiveID: created.id,
        archiveCause: "user",
        archiveState: null,
        archiveStateAt: null,
      })

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, created.id)).get()
      expect(row).toMatchObject({
        family_archive_id: created.id,
        archive_cause: "user",
        archive_state: null,
        archive_state_at: null,
      })
      expect(row?.time_archived).toBeDefined()

      const archived = yield* session.get(created.id)
      expect(archived.time.archived).toBeDefined()
      expect(archived.familyArchiveID).toBe(created.id)
      expect(archived.archiveCause).toBe("user")
      expect(archived.archiveState).toBeUndefined()
      expect(archived.archiveStateAt).toBeUndefined()

      yield* db
        .delete(SessionArchiveGuardTable)
        .where(eq(SessionArchiveGuardTable.session_id, created.id))
        .run()
        .pipe(Effect.orDie)
    }),
  )

  it.effect("unarchives and clears time and family fields", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const bus = yield* Bus.Service
      const { db } = yield* Database.Service
      const created = yield* session.create({ location })

      yield* db
        .insert(SessionArchiveGuardTable)
        .values({ session_id: created.id })
        .onConflictDoNothing()
        .run()
        .pipe(Effect.orDie)

      yield* bus.publish(SessionEvent.Archived, {
        sessionID: created.id,
        familyArchiveID: created.id,
        archiveCause: "cascade",
        archiveState: "archiving",
        archiveStateAt: 123,
      })

      const archivedRow = yield* db.select().from(SessionTable).where(eq(SessionTable.id, created.id)).get()
      expect(archivedRow).toMatchObject({
        family_archive_id: created.id,
        archive_cause: "cascade",
        archive_state: "archiving",
        archive_state_at: 123,
      })

      yield* bus.publish(SessionEvent.Unarchived, { sessionID: created.id })

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, created.id)).get()
      expect(row).toMatchObject({
        time_archived: null,
        family_archive_id: null,
        archive_cause: null,
        archive_state: null,
        archive_state_at: null,
      })

      const restored = yield* session.get(created.id)
      expect(restored.time.archived).toBeUndefined()
      expect(restored.familyArchiveID).toBeUndefined()
      expect(restored.archiveCause).toBeUndefined()
      expect(restored.archiveState).toBeUndefined()
      expect(restored.archiveStateAt).toBeUndefined()

      yield* db
        .delete(SessionArchiveGuardTable)
        .where(eq(SessionArchiveGuardTable.session_id, created.id))
        .run()
        .pipe(Effect.orDie)
    }),
  )
})
