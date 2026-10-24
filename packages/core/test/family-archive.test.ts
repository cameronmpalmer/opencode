import { describe, expect, test } from "bun:test"
import { Cause, Effect, Exit, Logger } from "effect"
import { eq } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { EventV2 } from "@opencode-ai/core/event"
import { EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionSchema, effectiveFamilyKey } from "@opencode-ai/core/session/schema"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionArchivedError } from "@opencode-ai/core/session/error"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import {
  SessionArchiveGuardTable,
  SessionImportExemptTable,
  SessionTable,
  effectiveFamilyKeySql,
} from "@opencode-ai/core/session/sql"
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

  it.effect("projects a full session.updated projection of an archived row when guarded", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow({
        time_archived: 1000,
        family_archive_id: sessionID,
        archive_cause: "cascade",
      })
      yield* db.insert(SessionArchiveGuardTable).values({ session_id: sessionID }).run()

      yield* projectUpdated({
        familyArchiveID: sessionID,
        archiveCause: "cascade",
        archiveState: null,
        archiveStateAt: null,
        time: { created: 1, updated: 1, archived: 1000 },
      })

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()
      expect(row?.time_archived).toBe(1000)
      expect(row?.family_archive_id).toBe(sessionID)
      expect(row?.archive_cause).toBe("cascade")
      expect(row?.time_updated).toBe(1)
    }),
  )

  it.effect("rejects a non-archive session.updated on an archived row even with a guard and logs once", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow({
        time_archived: 1000,
        family_archive_id: sessionID,
        archive_cause: "cascade",
      })
      yield* db.insert(SessionArchiveGuardTable).values({ session_id: sessionID }).run()
      const events = yield* EventV2.Service
      const messages: Array<unknown> = []
      const logger = Logger.make((options) => messages.push(options.message))

      const exit = yield* events
        .publish(
          SessionV1.Event.Updated,
          {
            sessionID,
            info: info({ title: "changed", familyArchiveID: sessionID, time: { created: 1, updated: 1 } }),
          },
          { id: EventV2.ID.make("evt_archived_update") },
        )
        .pipe(Effect.provide(Logger.layer([logger])), Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (!Exit.isFailure(exit)) return
      expect(exit.cause.reasons.find(Cause.isDieReason)?.defect).toBeInstanceOf(SessionArchivedError)
      expect(yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).get()).toMatchObject({
        title: "old title",
        time_archived: 1000,
      })
      expect(yield* db.select().from(EventTable).all()).toHaveLength(0)
      expect(yield* db.select().from(EventSequenceTable).all()).toHaveLength(0)
      const logged = JSON.stringify(messages)
      expect(logged).toContain("evt_archived_update")
      expect(logged).toContain("session.updated")
      expect(logged).toContain(sessionID)
    }),
  )

  it.effect("rejects session.created under an archived ancestor without journaling the event", () =>
    Effect.gen(function* () {
      const db = yield* seedProject
      const root = SessionSchema.ID.make("ses_root")
      const child = SessionSchema.ID.make("ses_child")
      yield* db
        .insert(SessionTable)
        .values({
          id: root,
          project_id: projectID,
          slug: "root",
          directory: "/project",
          title: "root",
          version: "test",
          time_created: 1,
          time_updated: 1,
          time_archived: 1000,
          family_archive_id: root,
          archive_cause: "user",
        })
        .run()
      const events = yield* EventV2.Service

      const exit = yield* events
        .publish(SessionV1.Event.Created, { sessionID: child, info: info({ id: child, parentID: root }) })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (!Exit.isFailure(exit)) return
      expect(exit.cause.reasons.find(Cause.isDieReason)?.defect).toBeInstanceOf(SessionArchivedError)
      expect(yield* db.select().from(SessionTable).where(eq(SessionTable.id, child)).get()).toBeUndefined()
      expect(yield* db.select().from(EventTable).all()).toHaveLength(0)
      expect(yield* db.select().from(EventSequenceTable).all()).toHaveLength(0)
    }),
  )

  it.effect("rejects message.updated for an archived session without journaling the event", () =>
    Effect.gen(function* () {
      const db = yield* seedArchivedRow({ time_archived: 1000 })
      const events = yield* EventV2.Service

      const exit = yield* events
        .publish(SessionV1.Event.MessageUpdated, {
          sessionID,
          info: {
            id: SessionV1.MessageID.make("msg_blocked"),
            sessionID,
            role: "user",
            time: { created: 1 },
            agent: "build",
            model: {
              providerID: ProviderV2.ID.make("provider"),
              modelID: ModelV2.ID.make("model"),
            },
          },
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (!Exit.isFailure(exit)) return
      expect(exit.cause.reasons.find(Cause.isDieReason)?.defect).toBeInstanceOf(SessionArchivedError)
      expect(yield* db.select().from(EventTable).all()).toHaveLength(0)
      expect(yield* db.select().from(EventSequenceTable).all()).toHaveLength(0)
    }),
  )
})

describe("effective family key", () => {
  // Legacy family fixtures predate the trigger and are inserted as an import,
  // which is the supported way to restore rows under an archived ancestor.
  const allowFixtureInsert = (db: Database.Interface["db"], ids: SessionSchema.ID[]) =>
    db
      .insert(SessionImportExemptTable)
      .values(ids.map((session_id) => ({ session_id, expires_at: Date.now() + 60_000 })))
      .run()

  const row = (id: string, values: Partial<typeof SessionTable.$inferInsert> = {}) => ({
    id: SessionSchema.ID.make(id),
    project_id: projectID,
    slug: id,
    directory: "/project",
    title: id,
    version: "test",
    time_created: 1,
    time_updated: 1,
    time_archived: 1,
    ...values,
  })

  test("handles cascade, user, legacy, nested legacy, and missing roots", () => {
    const exists = (id: string) => id === "p"
    expect(
      effectiveFamilyKey({ id: "c", family_archive_id: "p", archive_cause: "cascade" }, { familyExists: exists }),
    ).toBe("p")
    expect(
      effectiveFamilyKey(
        { id: "u", family_archive_id: null, archive_cause: "user", time_archived: 1 },
        { familyExists: exists },
      ),
    ).toBe("u")
    expect(
      effectiveFamilyKey(
        { id: "g", family_archive_id: null, archive_cause: null },
        { familyExists: exists, nearestArchivedAncestorKey: "p" },
      ),
    ).toBe("p")
    expect(
      effectiveFamilyKey(
        { id: "x", family_archive_id: "missing", archive_cause: "cascade", time_archived: 1 },
        { familyExists: exists, nearestArchivedAncestorKey: "p" },
      ),
    ).toBe("x")
    expect(
      effectiveFamilyKey(
        { id: "y", family_archive_id: "missing", archive_cause: "cascade", time_archived: null },
        { familyExists: exists, nearestArchivedAncestorKey: "p" },
      ),
    ).toBe("p")
    expect(
      effectiveFamilyKey(
        { id: "o", family_archive_id: "missing", archive_cause: "cascade", time_archived: 1 },
        { familyExists: exists },
      ),
    ).toBe("o")
  })

  it.effect("SQL matches TypeScript for cascade, user, legacy, nested legacy, and missing roots", () =>
    Effect.gen(function* () {
      const db = yield* seedProject
      const P = SessionSchema.ID.make("ses_p")
      const C = SessionSchema.ID.make("ses_c")
      const L = SessionSchema.ID.make("ses_l")
      const U = SessionSchema.ID.make("ses_u")
      const O = SessionSchema.ID.make("ses_o")
      const X = SessionSchema.ID.make("ses_x")
      const Y = SessionSchema.ID.make("ses_y")
      const missing = SessionSchema.ID.make("ses_missing")
      yield* allowFixtureInsert(db, [C, L, U, X, Y])

      yield* db
        .insert(SessionTable)
        .values([
          row(P, { family_archive_id: P, archive_cause: "user" }),
          row(C, { parent_id: P, family_archive_id: P, archive_cause: "cascade" }),
          row(L, { parent_id: P }),
          row(U, { parent_id: P, archive_cause: "user" }),
          row(O, { family_archive_id: missing, archive_cause: "cascade" }),
          row(X, { parent_id: P, family_archive_id: missing, archive_cause: "cascade" }),
          row(Y, { parent_id: P, family_archive_id: missing, archive_cause: "cascade", time_archived: null }),
        ])
        .run()

      const rows = yield* db.all<{ session_id: string; family_key: string }>(effectiveFamilyKeySql)
      expect(rows).toHaveLength(7)
      const actual = new Map(rows.map((entry) => [entry.session_id, entry.family_key]))
      expect(actual).toEqual(
        new Map([
          [P, P],
          [C, P],
          [L, P],
          [U, U],
          [O, O],
          [X, X],
          [Y, P],
        ]),
      )

      const exists = (id: string) => id === P
      const expected = new Map([
        [
          P,
          effectiveFamilyKey(
            { id: P, family_archive_id: P, archive_cause: "user", time_archived: 1 },
            { familyExists: exists },
          ),
        ],
        [
          C,
          effectiveFamilyKey(
            { id: C, family_archive_id: P, archive_cause: "cascade", time_archived: 1 },
            { familyExists: exists, nearestArchivedAncestorKey: P },
          ),
        ],
        [
          L,
          effectiveFamilyKey(
            { id: L, family_archive_id: null, archive_cause: null, time_archived: 1 },
            { familyExists: exists, nearestArchivedAncestorKey: P },
          ),
        ],
        [
          U,
          effectiveFamilyKey(
            { id: U, family_archive_id: null, archive_cause: "user", time_archived: 1 },
            { familyExists: exists, nearestArchivedAncestorKey: P },
          ),
        ],
        [
          O,
          effectiveFamilyKey(
            { id: O, family_archive_id: missing, archive_cause: "cascade", time_archived: 1 },
            { familyExists: exists },
          ),
        ],
        [
          X,
          effectiveFamilyKey(
            { id: X, family_archive_id: missing, archive_cause: "cascade", time_archived: 1 },
            { familyExists: exists, nearestArchivedAncestorKey: P },
          ),
        ],
        [
          Y,
          effectiveFamilyKey(
            { id: Y, family_archive_id: missing, archive_cause: "cascade", time_archived: null },
            { familyExists: exists, nearestArchivedAncestorKey: P },
          ),
        ],
      ])
      expect(actual).toEqual(expected)
    }),
  )

  it.effect("SQL resolves a nested legacy chain recursively", () =>
    Effect.gen(function* () {
      const db = yield* seedProject
      const P = SessionSchema.ID.make("ses_p")
      const L = SessionSchema.ID.make("ses_l")
      const G = SessionSchema.ID.make("ses_g")
      yield* allowFixtureInsert(db, [L, G])

      yield* db
        .insert(SessionTable)
        .values([
          row(P, { family_archive_id: P, archive_cause: "user" }),
          row(L, { parent_id: P }),
          row(G, { parent_id: L }),
        ])
        .run()

      const rows = yield* db.all<{ session_id: string; family_key: string }>(effectiveFamilyKeySql)
      expect(rows).toHaveLength(3)
      expect(new Map(rows.map((entry) => [entry.session_id, entry.family_key]))).toEqual(
        new Map([
          [P, P],
          [L, P],
          [G, P],
        ]),
      )
    }),
  )

  it.effect("SQL uses the fallback ancestor for an all-null legacy chain", () =>
    Effect.gen(function* () {
      const db = yield* seedProject
      const P = SessionSchema.ID.make("ses_p")
      const L = SessionSchema.ID.make("ses_l")
      const G = SessionSchema.ID.make("ses_g")
      yield* allowFixtureInsert(db, [L, G])

      yield* db
        .insert(SessionTable)
        .values([row(P), row(L, { parent_id: P }), row(G, { parent_id: L, time_archived: null })])
        .run()

      const rows = yield* db.all<{ session_id: string; family_key: string }>(effectiveFamilyKeySql)
      expect(rows).toHaveLength(3)
      expect(new Map(rows.map((entry) => [entry.session_id, entry.family_key]))).toEqual(
        new Map([
          [P, P],
          [L, P],
          [G, P],
        ]),
      )
      expect(
        effectiveFamilyKey(
          { id: G, family_archive_id: null, archive_cause: null, time_archived: null },
          { familyExists: (id) => id === P, nearestArchivedAncestorKey: P },
        ),
      ).toBe(P)
    }),
  )

  it.effect("SQL terminates on a parent cycle", () =>
    Effect.gen(function* () {
      const db = yield* seedProject
      const A = SessionSchema.ID.make("ses_a")
      const B = SessionSchema.ID.make("ses_b")

      yield* db
        .insert(SessionTable)
        .values([row(A, { parent_id: B, time_archived: null }), row(B, { parent_id: A, time_archived: null })])
        .run()

      const rows = yield* db.all<{ session_id: string; family_key: string }>(effectiveFamilyKeySql)
      expect(rows).toHaveLength(2)
      expect(new Map(rows.map((entry) => [entry.session_id, entry.family_key]))).toEqual(
        new Map([
          [A, A],
          [B, B],
        ]),
      )
    }),
  )

  it.effect("SQL resolves an orphaned parent to its own id", () =>
    Effect.gen(function* () {
      const db = yield* seedProject
      const A = SessionSchema.ID.make("ses_a")
      const missingParent = SessionSchema.ID.make("ses_missing_parent")

      yield* db
        .insert(SessionTable)
        .values([row(A, { parent_id: missingParent })])
        .run()

      const rows = yield* db.all<{ session_id: string; family_key: string }>(effectiveFamilyKeySql)
      expect(rows).toHaveLength(1)
      expect(rows[0]).toEqual({ session_id: A, family_key: A })
    }),
  )
})
