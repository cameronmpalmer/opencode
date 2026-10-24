import { describe, expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Cause, Effect, Exit } from "effect"
import { sql } from "drizzle-orm"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import type { SqlClient as SqlClientService } from "effect/unstable/sql/SqlClient"

const run = <A, E>(effect: Effect.Effect<A, E, SqlClientService>) =>
  Effect.runPromise(
    effect.pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped),
  )

const makeDb = EffectDrizzleSqlite.makeWithDefaults()
type Db = Effect.Success<typeof makeDb>

const setup = Effect.gen(function* () {
  const db = yield* makeDb
  yield* DatabaseMigration.apply(db)
  yield* db.run(
    sql`INSERT INTO project (id, worktree, sandboxes, time_created, time_updated) VALUES ('prj', '/p', '[]', 1, 1)`,
  )
  return db
})

const addSession = (
  db: Db,
  id: string,
  values: { parent?: string | null; archived?: number | null; state?: string | null } = {},
) =>
  db
    .run(
      sql`
      INSERT INTO session (
        id, project_id, parent_id, slug, directory, title, version,
        time_created, time_updated, time_archived, archive_state
      ) VALUES (
        ${id}, 'prj', ${values.parent ?? null}, ${id}, '/p', ${id}, 'v',
        1, 1, ${values.archived ?? null}, ${values.state ?? null}
      )
    `,
    )
    .pipe(Effect.orDie)

const archive = (db: Db, id: string, values: { cause?: string; state?: string | null; at?: number | null } = {}) =>
  Effect.gen(function* () {
    yield* db.run(sql`INSERT INTO session_archive_guard (session_id) VALUES (${id})`).pipe(Effect.orDie)
    yield* db
      .run(
        sql`
        UPDATE session SET
          time_archived = ${values.at === undefined ? 1000 : values.at},
          family_archive_id = ${id},
          archive_cause = ${values.cause ?? "cascade"},
          archive_state = ${values.state ?? null},
          archive_state_at = ${values.state ? 1000 : null}
        WHERE id = ${id}
      `,
      )
      .pipe(Effect.orDie)
    yield* db.run(sql`DELETE FROM session_archive_guard WHERE session_id = ${id}`).pipe(Effect.orDie)
  })

const exempt = (db: Db, id: string, expiresAt = Date.now() + 60_000) =>
  db
    .run(sql`INSERT INTO session_import_exempt (session_id, expires_at) VALUES (${id}, ${expiresAt})`)
    .pipe(Effect.orDie)

const expectAbort = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.gen(function* () {
    const exit = yield* effect.pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (!Exit.isFailure(exit)) return
    expect(Cause.pretty(exit.cause)).toContain("SessionArchivedError")
  })

const addMessage = (db: Db, id: string, sessionID: string) =>
  db
    .run(
      sql`INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (${id}, ${sessionID}, 1, 1, '{}')`,
    )
    .pipe(Effect.orDie)

const addPart = (db: Db, id: string, messageID: string, sessionID: string) =>
  db.run(
    sql`INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (${id}, ${messageID}, ${sessionID}, 1, 1, '{}')`,
  )

const count = (db: Db, table: string) =>
  db.get<{ n: number }>(sql.raw(`SELECT COUNT(*) AS n FROM ${table}`)).pipe(
    Effect.orDie,
    Effect.map((row) => row?.n ?? 0),
  )

const seedScopedRows = (db: Db, id: string) =>
  Effect.gen(function* () {
    yield* db
      .run(
        sql`INSERT INTO session_input (id, session_id, prompt, delivery, admitted_seq, time_created) VALUES (${`in_${id}`}, ${id}, 'p', 'steer', 1, 1)`,
      )
      .pipe(Effect.orDie)
    yield* db
      .run(
        sql`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data) VALUES (${`sm_${id}`}, ${id}, 'user', 1, 1, 1, '{}')`,
      )
      .pipe(Effect.orDie)
    yield* db
      .run(
        sql`INSERT INTO todo (session_id, content, status, priority, position, time_created, time_updated) VALUES (${id}, 'c', 'pending', 'low', 1, 1, 1)`,
      )
      .pipe(Effect.orDie)
    yield* db
      .run(
        sql`INSERT INTO session_context_epoch (session_id, baseline, snapshot, baseline_seq) VALUES (${id}, '{}', '{}', 1)`,
      )
      .pipe(Effect.orDie)
    yield* db
      .run(
        sql`INSERT INTO session_share (session_id, id, secret, url, time_created, time_updated) VALUES (${id}, ${`s_${id}`}, 'x', 'u', 1, 1)`,
      )
      .pipe(Effect.orDie)
  })

describe("family archive triggers", () => {
  test("rejects child inserts under archived roots, deeper archived ancestors, and archiving parents", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* setup
        yield* addSession(db, "root")
        yield* addSession(db, "child", { parent: "root" })
        yield* archive(db, "root")

        yield* expectAbort(addSession(db, "under_root", { parent: "root" }))
        yield* expectAbort(addSession(db, "under_grandchild", { parent: "child" }))

        yield* addSession(db, "archiving")
        yield* archive(db, "archiving", { at: null, state: "archiving" })
        expect(
          yield* db.get<{ time_archived: number | null; archive_state: string | null }>(
            sql`SELECT time_archived, archive_state FROM session WHERE id = 'archiving'`,
          ),
        ).toEqual({ time_archived: null, archive_state: "archiving" })
        yield* expectAbort(addSession(db, "under_archiving", { parent: "archiving" }))

        yield* exempt(db, "exempt_child")
        yield* addSession(db, "exempt_child", { parent: "root" })
        expect(yield* count(db, "session")).toBe(4)
      }),
    )
  })

  test("rejects message and part inserts for archived and archiving sessions", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* setup
        yield* addSession(db, "archived")
        yield* addSession(db, "archiving")
        yield* addSession(db, "active")
        yield* addMessage(db, "msg_active", "active")
        yield* addMessage(db, "msg_archived", "archived")
        yield* addMessage(db, "msg_archiving", "archiving")
        yield* archive(db, "archived")
        yield* archive(db, "archiving", { at: null, state: "archiving" })

        yield* expectAbort(addMessage(db, "msg_blocked", "archived"))
        yield* expectAbort(addMessage(db, "msg_blocked_archiving", "archiving"))
        yield* expectAbort(addPart(db, "part_blocked", "msg_archived", "active"))
        yield* expectAbort(addPart(db, "part_blocked_archiving", "msg_archiving", "active"))
        yield* expectAbort(addPart(db, "part_missing", "msg_missing", "active"))

        // The part's own session_id is ignored; the message session decides.
        yield* addPart(db, "part_crossed", "msg_active", "archived")
        expect(yield* count(db, "part")).toBe(1)

        // A live message under an archived part.session_id is still blocked.
        yield* expectAbort(addPart(db, "part_wrong_way", "msg_archived", "active"))
      }),
    )
  })

  test("rejects inserts and updates in every session-scoped table", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* setup
        yield* addSession(db, "archived")
        yield* addSession(db, "active")
        yield* archive(db, "archived")

        const inserts: Record<string, ReturnType<typeof sql>> = {
          session_input: sql`INSERT INTO session_input (id, session_id, prompt, delivery, admitted_seq, time_created) VALUES ('in_1', 'archived', 'p', 'steer', 1, 1)`,
          session_message: sql`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data) VALUES ('sm_1', 'archived', 'user', 1, 1, 1, '{}')`,
          todo: sql`INSERT INTO todo (session_id, content, status, priority, position, time_created, time_updated) VALUES ('archived', 'c', 'pending', 'low', 1, 1, 1)`,
          session_context_epoch: sql`INSERT INTO session_context_epoch (session_id, baseline, snapshot, baseline_seq) VALUES ('archived', '{}', '{}', 1)`,
          session_share: sql`INSERT INTO session_share (session_id, id, secret, url, time_created, time_updated) VALUES ('archived', 's', 'x', 'u', 1, 1)`,
        }
        for (const statement of Object.values(inserts)) {
          yield* expectAbort(db.run(statement))
        }

        // Seed rows in a live session, then archive it and try to update them.
        yield* seedScopedRows(db, "active")
        yield* archive(db, "active")

        const updates: Array<ReturnType<typeof sql>> = [
          sql`UPDATE session_input SET prompt = 'changed' WHERE id = 'in_active'`,
          sql`UPDATE session_message SET data = '{"changed":true}' WHERE id = 'sm_active'`,
          sql`UPDATE todo SET content = 'changed' WHERE session_id = 'active' AND position = 1`,
          sql`UPDATE session_context_epoch SET baseline = '{"changed":true}' WHERE session_id = 'active'`,
          sql`UPDATE session_share SET url = 'changed' WHERE session_id = 'active'`,
        ]
        for (const statement of updates) {
          yield* expectAbort(db.run(statement))
        }
      }),
    )
  })

  test("rejects UPDATE moves between archived and active sessions for every session-scoped row", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* setup
        yield* addSession(db, "archived")
        yield* addSession(db, "active")
        yield* addSession(db, "fresh")
        yield* addMessage(db, "msg_archived", "archived")
        yield* addMessage(db, "msg_active", "active")
        yield* addPart(db, "part_archived", "msg_archived", "archived")
        yield* addPart(db, "part_active", "msg_active", "active")
        yield* seedScopedRows(db, "archived")
        yield* seedScopedRows(db, "active")
        yield* archive(db, "archived")

        const moves = [
          sql`UPDATE message SET session_id = 'fresh' WHERE id = 'msg_archived'`,
          sql`UPDATE message SET session_id = 'archived' WHERE id = 'msg_active'`,
          sql`UPDATE session_input SET session_id = 'fresh' WHERE id = 'in_archived'`,
          sql`UPDATE session_input SET session_id = 'archived' WHERE id = 'in_active'`,
          sql`UPDATE session_message SET session_id = 'fresh' WHERE id = 'sm_archived'`,
          sql`UPDATE session_message SET session_id = 'archived' WHERE id = 'sm_active'`,
          sql`UPDATE todo SET session_id = 'fresh' WHERE session_id = 'archived' AND position = 1`,
          sql`UPDATE todo SET session_id = 'archived' WHERE session_id = 'active' AND position = 1`,
          sql`UPDATE session_context_epoch SET session_id = 'fresh' WHERE session_id = 'archived'`,
          sql`UPDATE session_context_epoch SET session_id = 'archived' WHERE session_id = 'active'`,
          sql`UPDATE session_share SET session_id = 'fresh' WHERE session_id = 'archived'`,
          sql`UPDATE session_share SET session_id = 'archived' WHERE session_id = 'active'`,
        ]
        for (const statement of moves) {
          yield* expectAbort(db.run(statement))
        }
      }),
    )
  })

  test("rejects part UPDATEs that resolve to archived or missing messages", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* setup
        yield* addSession(db, "archived")
        yield* addSession(db, "active")
        yield* addMessage(db, "msg_archived", "archived")
        yield* addMessage(db, "msg_active", "active")
        yield* addPart(db, "part_archived", "msg_archived", "archived")
        yield* addPart(db, "part_active", "msg_active", "active")
        yield* archive(db, "archived")

        yield* expectAbort(db.run(sql`UPDATE part SET message_id = 'msg_active' WHERE id = 'part_archived'`))
        yield* expectAbort(db.run(sql`UPDATE part SET message_id = 'msg_archived' WHERE id = 'part_active'`))
        yield* expectAbort(db.run(sql`UPDATE part SET message_id = 'msg_missing' WHERE id = 'part_active'`))
        yield* expectAbort(db.run(sql`UPDATE part SET data = '{"changed":true}' WHERE id = 'part_archived'`))

        yield* db.run(sql`UPDATE part SET data = '{"changed":true}' WHERE id = 'part_active'`).pipe(Effect.orDie)
      }),
    )
  })

  test("rejects moves into and out of archived families, including combined archive and reparent", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* setup
        yield* addSession(db, "root")
        yield* addSession(db, "child", { parent: "root" })
        yield* addSession(db, "outside")
        yield* addSession(db, "other")
        yield* archive(db, "root")

        yield* expectAbort(db.run(sql`UPDATE session SET parent_id = 'root' WHERE id = 'outside'`))
        yield* expectAbort(db.run(sql`UPDATE session SET parent_id = NULL WHERE id = 'child'`))

        // The guard authorizes archive-field changes only; a combined archive
        // plus reparent still cannot escape the parent-move check.
        yield* db.run(sql`INSERT INTO session_archive_guard (session_id) VALUES ('outside')`).pipe(Effect.orDie)
        yield* expectAbort(
          db.run(sql`UPDATE session SET parent_id = 'other', time_archived = 2000 WHERE id = 'outside'`),
        )
        yield* db.run(sql`DELETE FROM session_archive_guard WHERE session_id = 'outside'`).pipe(Effect.orDie)

        yield* db.run(sql`UPDATE session SET parent_id = 'other' WHERE id = 'outside'`).pipe(Effect.orDie)

        expect(
          (yield* db.get<{ parent_id: string | null }>(sql`SELECT parent_id FROM session WHERE id = 'outside'`))
            ?.parent_id,
        ).toBe("other")
      }),
    )
  })

  test("allows only archive-field changes on an archived row and protects every other column", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* setup
        yield* addSession(db, "root")
        yield* archive(db, "root")

        yield* db.run(sql`INSERT INTO session_archive_guard (session_id) VALUES ('root')`).pipe(Effect.orDie)
        // Full projector-shaped update: every non-archive column is reassigned
        // to its current value while only archive fields change.
        yield* db
          .run(
            sql`
            UPDATE session SET
              id = id,
              project_id = project_id,
              workspace_id = workspace_id,
              parent_id = parent_id,
              slug = slug,
              directory = directory,
              path = path,
              title = title,
              version = version,
              share_url = share_url,
              summary_additions = summary_additions,
              summary_deletions = summary_deletions,
              summary_files = summary_files,
              summary_diffs = summary_diffs,
              metadata = metadata,
              cost = cost,
              tokens_input = tokens_input,
              tokens_output = tokens_output,
              tokens_reasoning = tokens_reasoning,
              tokens_cache_read = tokens_cache_read,
              tokens_cache_write = tokens_cache_write,
              revert = revert,
              permission = permission,
              agent = agent,
              model = model,
              time_created = time_created,
              time_updated = time_updated,
              time_compacting = time_compacting,
              archive_cause = 'user'
            WHERE id = 'root'
          `,
          )
          .pipe(Effect.orDie)

        expect(
          (yield* db.get<{ time_updated: number }>(sql`SELECT time_updated FROM session WHERE id = 'root'`))
            ?.time_updated,
        ).toBe(1)

        // The guard remains present on purpose: it authorizes archive-field
        // transitions only, never arbitrary non-archive changes.
        yield* expectAbort(db.run(sql`UPDATE session SET summary_files = 5 WHERE id = 'root'`))
        yield* expectAbort(db.run(sql`UPDATE session SET parent_id = 'other' WHERE id = 'root'`))
        yield* expectAbort(db.run(sql`UPDATE session SET title = 'changed' WHERE id = 'root'`))
        yield* expectAbort(db.run(sql`UPDATE session SET id = 'renamed' WHERE id = 'root'`))
      }),
    )
  })

  test("requires a guard or active import exemption for archive-field updates", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* setup
        yield* addSession(db, "target")

        yield* expectAbort(db.run(sql`UPDATE session SET archive_cause = 'user' WHERE id = 'target'`))

        yield* exempt(db, "target", Date.now() - 1_000)
        yield* expectAbort(db.run(sql`UPDATE session SET archive_cause = 'user' WHERE id = 'target'`))

        yield* db.run(sql`DELETE FROM session_import_exempt WHERE session_id = 'target'`).pipe(Effect.orDie)
        yield* exempt(db, "target")
        yield* db.run(sql`UPDATE session SET archive_cause = 'user' WHERE id = 'target'`).pipe(Effect.orDie)

        yield* db.run(sql`DELETE FROM session_import_exempt WHERE session_id = 'target'`).pipe(Effect.orDie)
        yield* archive(db, "target")

        expect(yield* count(db, "session_import_exempt")).toBe(0)
      }),
    )
  })

  test("an import exemption authorizes a child insert and is removed afterward", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* setup
        yield* addSession(db, "root")
        yield* archive(db, "root")
        yield* exempt(db, "imported")
        yield* addSession(db, "imported", { parent: "root" })
        yield* db.run(sql`DELETE FROM session_import_exempt WHERE session_id = 'imported'`).pipe(Effect.orDie)

        expect(yield* count(db, "session_import_exempt")).toBe(0)
        yield* expectAbort(addSession(db, "blocked", { parent: "root" }))
      }),
    )
  })
})
