import { describe, expect } from "bun:test"
import { Cause, Effect, Exit } from "effect"
import { eq } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { EventV2 } from "@opencode-ai/core/event"
import { EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionArchiveGuardTable, SessionTable } from "@opencode-ai/core/session/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { WorkspaceV2 } from "@opencode-ai/core/workspace"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(LayerNode.group([Database.node, EventV2.node, SessionProjector.node])))
const projectID = Project.ID.global
const sessionID = SessionSchema.ID.make("ses_a")

const info = (input: Partial<SessionV1.SessionInfo> = {}): SessionV1.SessionInfo => ({
  id: sessionID,
  slug: "test",
  projectID,
  directory: "/project",
  title: "old title",
  version: "test",
  time: { created: 1, updated: 1 },
  ...input,
})

const seedProject = Effect.gen(function* () {
  const db = (yield* Database.Service).db
  yield* db
    .insert(ProjectTable)
    .values({ id: projectID, worktree: AbsolutePath.make("/project"), sandboxes: [] })
    .onConflictDoNothing()
    .run()
  return db
})

const seedArchivedRow = (values: Partial<typeof SessionTable.$inferInsert> = {}) =>
  Effect.gen(function* () {
    const db = yield* seedProject
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: projectID,
        slug: "test",
        directory: "/project",
        title: "old title",
        version: "test",
        time_created: 1,
        time_updated: 1,
        ...values,
      })
      .run()
    return db
  })

const projectUpdated = (values: Partial<SessionV1.SessionInfo>) =>
  Effect.gen(function* () {
    const events = yield* EventV2.Service
    return yield* events.publish(SessionV1.Event.Updated, { sessionID, info: info(values) })
  })

describe("family archive projector", () => {
  it.effect("session.updated preserves family fields and never nulls them from an old payload", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow({
        time_archived: 1000,
        family_archive_id: sessionID,
        archive_cause: "user",
      })

      yield* projectUpdated({})

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.family_archive_id).toBe(sessionID)
      expect(row?.archive_cause).toBe("user")
      expect(row?.time_archived).toBe(1000)
      expect(row?.title).toBe("old title")
      expect(yield* db.select().from(EventTable).all()).toHaveLength(1)
    }),
  )

  it.effect("an old payload preserves an archiving lock", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow({
        time_archived: 1000,
        family_archive_id: sessionID,
        archive_cause: "user",
        archive_state: "archiving",
        archive_state_at: 2000,
      })

      yield* projectUpdated({})

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.archive_state).toBe("archiving")
      expect(row?.archive_state_at).toBe(2000)
    }),
  )

  it.effect("an old payload does not reset unrelated session state", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow({
        cost: 42,
        tokens_input: 7,
        tokens_output: 8,
        tokens_reasoning: 9,
        tokens_cache_read: 10,
        tokens_cache_write: 11,
        share_url: "https://example.com/session",
        summary_additions: 3,
        summary_deletions: 4,
        summary_files: 5,
      })

      yield* projectUpdated({ title: "new title", time: { created: 1, updated: 2 } })

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.cost).toBe(42)
      expect(row?.tokens_input).toBe(7)
      expect(row?.tokens_output).toBe(8)
      expect(row?.tokens_reasoning).toBe(9)
      expect(row?.tokens_cache_read).toBe(10)
      expect(row?.tokens_cache_write).toBe(11)
      expect(row?.share_url).toBe("https://example.com/session")
      expect(row?.summary_additions).toBe(3)
      expect(row?.summary_deletions).toBe(4)
      expect(row?.summary_files).toBe(5)
    }),
  )

  it.effect("an old payload clears revert and workspace on omission", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow({
        revert: { messageID: SessionMessage.ID.make("msg_revert") },
        workspace_id: WorkspaceV2.ID.make("wrk_test"),
      })

      yield* projectUpdated({ title: "new title", time: { created: 1, updated: 2 } })

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.revert).toBeNull()
      expect(row?.workspace_id).toBeNull()
    }),
  )

  it.effect("refuses an archive-field update without a guard row", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow()

      const exit = yield* projectUpdated({ familyArchiveID: sessionID }).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (!Exit.isFailure(exit)) return
      const defect = exit.cause.reasons.find(Cause.isDieReason)?.defect
      expect(defect).toBeInstanceOf(SessionProjector.SessionArchiveGuardRequired)

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.family_archive_id).toBeNull()
      expect(yield* db.select().from(EventTable).all()).toHaveLength(0)
      expect(yield* db.select().from(EventSequenceTable).all()).toHaveLength(0)
    }),
  )

  it.effect("persists archive fields when the transaction holds a guard row", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow()
      yield* db.insert(SessionArchiveGuardTable).values({ session_id: sessionID }).run()

      yield* projectUpdated({
        familyArchiveID: sessionID,
        archiveCause: "user",
        archiveState: "archiving",
        archiveStateAt: 2000,
        time: { created: 1, updated: 2, archived: 1000 },
      })

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.family_archive_id).toBe(sessionID)
      expect(row?.archive_cause).toBe("user")
      expect(row?.archive_state).toBe("archiving")
      expect(row?.archive_state_at).toBe(2000)
      expect(row?.time_archived).toBe(1000)
    }),
  )

  it.effect("explicit null archive fields clear them when the transaction holds a guard row", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow({
        time_archived: 1000,
        family_archive_id: sessionID,
        archive_cause: "user",
        archive_state: "archiving",
        archive_state_at: 2000,
      })
      yield* db.insert(SessionArchiveGuardTable).values({ session_id: sessionID }).run()

      yield* projectUpdated({
        familyArchiveID: null,
        archiveCause: null,
        archiveState: null,
        archiveStateAt: null,
        time: { created: 1, updated: 1, archived: 1000 },
      })

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.family_archive_id).toBeNull()
      expect(row?.archive_cause).toBeNull()
      expect(row?.archive_state).toBeNull()
      expect(row?.archive_state_at).toBeNull()
      expect(row?.time_archived).toBe(1000)
    }),
  )

  it.effect("does not require a guard row when archive fields are unchanged", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow({
        time_archived: 1000,
        family_archive_id: sessionID,
        archive_cause: "user",
      })

      yield* projectUpdated({
        familyArchiveID: sessionID,
        archiveCause: "user",
        time: { created: 1, updated: 1, archived: 1000 },
      })

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.time_updated).toBe(1)
      expect(row?.family_archive_id).toBe(sessionID)
    }),
  )

  it.effect("authorizes archive fields and removes the guard inside one transaction", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow()

      yield* db
        .transaction((tx) =>
          Effect.gen(function* () {
            yield* tx.insert(SessionArchiveGuardTable).values({ session_id: sessionID }).run()
            yield* projectUpdated({
              familyArchiveID: sessionID,
              archiveCause: "user",
              time: { created: 1, updated: 1, archived: 1000 },
            })
            yield* tx.delete(SessionArchiveGuardTable).where(eq(SessionArchiveGuardTable.session_id, sessionID)).run()
          }),
        )
        .pipe(Effect.orDie)

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.family_archive_id).toBe(sessionID)
      expect(row?.archive_cause).toBe("user")
      expect(row?.time_archived).toBe(1000)
      expect(yield* db.select().from(SessionArchiveGuardTable).all()).toHaveLength(0)
    }),
  )

  it.effect("a guarded archive-field update with time.archived null clears the timestamp", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow({
        time_archived: 1000,
        family_archive_id: sessionID,
        archive_cause: "user",
      })
      yield* db.insert(SessionArchiveGuardTable).values({ session_id: sessionID }).run()

      yield* projectUpdated({ time: { created: 1, updated: 1, archived: null } })

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.time_archived).toBeNull()
      expect(row?.family_archive_id).toBe(sessionID)
    }),
  )

  it.effect("refuses a time.archived-only update without a guard row", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow({ time_archived: 1000 })

      const exit = yield* projectUpdated({ time: { created: 1, updated: 1, archived: null } }).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (!Exit.isFailure(exit)) return
      const defect = exit.cause.reasons.find(Cause.isDieReason)?.defect
      expect(defect).toBeInstanceOf(SessionProjector.SessionArchiveGuardRequired)
      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.time_archived).toBe(1000)
    }),
  )

  it.effect("a guard row for a different session does not authorize the update", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow()
      yield* db
        .insert(SessionArchiveGuardTable)
        .values({ session_id: SessionSchema.ID.make("ses_other") })
        .run()

      const exit = yield* projectUpdated({ familyArchiveID: sessionID }).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (!Exit.isFailure(exit)) return
      const defect = exit.cause.reasons.find(Cause.isDieReason)?.defect
      expect(defect).toBeInstanceOf(SessionProjector.SessionArchiveGuardRequired)
      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.family_archive_id).toBeNull()
    }),
  )

  it.effect("a null-to-null archive update does not require a guard row", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow()

      yield* projectUpdated({
        familyArchiveID: null,
        archiveCause: null,
        archiveState: null,
        archiveStateAt: null,
        time: { created: 1, updated: 1, archived: null },
      })

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.family_archive_id).toBeNull()
      expect(row?.archive_cause).toBeNull()
      expect(row?.archive_state).toBeNull()
      expect(row?.archive_state_at).toBeNull()
      expect(row?.time_archived).toBeNull()
      expect(yield* db.select().from(SessionArchiveGuardTable).all()).toHaveLength(0)
    }),
  )

  it.effect("session.created round-trips all family archive fields", () =>
    Effect.gen(function* () {
      const db = yield* seedProject
      const events = yield* EventV2.Service

      yield* events.publish(SessionV1.Event.Created, {
        sessionID,
        info: info({
          familyArchiveID: sessionID,
          archiveCause: "user",
          archiveState: "archiving",
          archiveStateAt: 2000,
          time: { created: 1, updated: 1, archived: 1000 },
        }),
      })

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.family_archive_id).toBe(sessionID)
      expect(row?.archive_cause).toBe("user")
      expect(row?.archive_state).toBe("archiving")
      expect(row?.archive_state_at).toBe(2000)
      expect(row?.time_archived).toBe(1000)
    }),
  )
})
