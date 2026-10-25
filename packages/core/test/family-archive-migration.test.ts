import { describe, expect, test } from "bun:test"
import { Database as SQLite } from "bun:sqlite"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Cause, Effect, Exit } from "effect"
import { sql } from "drizzle-orm"
import { mkdtemp, rm } from "fs/promises"
import { tmpdir } from "os"
import { join } from "path"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { migrations } from "@opencode-ai/core/database/migration.gen"
import schema from "@opencode-ai/core/database/schema.gen"
import familyArchiveMigration from "@opencode-ai/core/database/migration/20261023000000_family_archive"
import familyArchiveBackfillMigration from "@opencode-ai/core/database/migration/20261023000001_family_archive_backfill"
import type { SqlClient as SqlClientService } from "effect/unstable/sql/SqlClient"
import { dryRun } from "../script/family-archive-dry-run"
import { reconcile, rollback } from "../script/family-archive-rollback"

const run = <A, E>(effect: Effect.Effect<A, E, SqlClientService>) =>
  Effect.runPromise(
    effect.pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped),
  )

const makeDb = EffectDrizzleSqlite.makeWithDefaults()
type Db = Effect.Success<typeof makeDb>

const runFile = <A, E>(filename: string, effect: Effect.Effect<A, E, SqlClientService>) =>
  Effect.runPromise(effect.pipe(Effect.provide(SqliteClient.layer({ filename, disableWAL: true })), Effect.scoped))

// Builds the current schema, seeds rows while the archive triggers do not yet
// exist, then applies the hand-written migration. That is the upgrade order the
// rollout uses for an existing database.
const seedExisting = (seed: (db: Db) => Effect.Effect<void>) =>
  Effect.gen(function* () {
    const db = yield* makeDb
    yield* db.transaction((tx) => schema.up(tx))
    yield* seed(db)
    yield* DatabaseMigration.applyOnly(db, [familyArchiveBackfillMigration])
    return db
  })

const withFileDatabase = async (seed: (db: Db) => Effect.Effect<void>) => {
  const dir = await mkdtemp(join(tmpdir(), "family-archive-migration-"))
  const filename = join(dir, "test.db")
  await runFile(
    filename,
    Effect.gen(function* () {
      const db = yield* makeDb
      yield* db.transaction((tx) => schema.up(tx))
      yield* seed(db)
      yield* DatabaseMigration.applyOnly(db, [familyArchiveBackfillMigration])
    }),
  )
  return {
    filename,
    cleanup: () => rm(dir, { recursive: true, force: true }),
  }
}

const seedProject = (db: Db) =>
  db
    .run(
      sql`INSERT INTO project (id, worktree, sandboxes, time_created, time_updated) VALUES ('prj', '/p', '[]', 1, 1)`,
    )
    .pipe(Effect.orDie)

const addSession = (
  db: Db,
  id: string,
  values: {
    parent?: string | null
    archived?: number | null
    family?: string | null
    cause?: string | null
    state?: string | null
    stateAt?: number | null
    updated?: number
    created?: number
  } = {},
) =>
  db
    .run(
      sql`
      INSERT INTO session (
        id, project_id, parent_id, slug, directory, title, version,
        time_created, time_updated, time_archived, family_archive_id,
        archive_cause, archive_state, archive_state_at
      ) VALUES (
        ${id}, 'prj', ${values.parent ?? null}, ${id}, '/p', ${id}, 'v',
        ${values.created ?? 1}, ${values.updated ?? 1}, ${values.archived ?? null},
        ${values.family ?? null}, ${values.cause ?? null}, ${values.state ?? null},
        ${values.stateAt ?? null}
      )
    `,
    )
    .pipe(Effect.orDie)

const addMessage = (db: Db, id: string, sessionID: string) =>
  db
    .run(
      sql`INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (${id}, ${sessionID}, 1, 1, '{}')`,
    )
    .pipe(Effect.orDie)

const row = (db: Db, id: string) =>
  db
    .get<{
      id: string
      time_archived: number | null
      family_archive_id: string | null
      archive_cause: string | null
      archive_state: string | null
      archive_state_at: number | null
      time_updated: number
    }>(
      sql`SELECT id, time_archived, family_archive_id, archive_cause, archive_state, archive_state_at, time_updated FROM session WHERE id = ${id}`,
    )
    .pipe(
      Effect.orDie,
      Effect.map((value) => {
        if (!value) throw new Error(`missing session ${id}`)
        return value
      }),
    )

const count = (db: Db, table: string) =>
  db.get<{ n: number }>(sql.raw(`SELECT COUNT(*) AS n FROM ${table}`)).pipe(
    Effect.orDie,
    Effect.map((value) => value?.n ?? 0),
  )

const guardCount = (db: Db) => count(db, "session_archive_guard")

const expectAbort = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.gen(function* () {
    const exit = yield* effect.pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (!Exit.isFailure(exit)) return
    expect(Cause.pretty(exit.cause)).toContain("SessionArchivedError")
  })

// Direct archive-field writes in tests stand in for the runtime handlers.
const guardedUpdate = (db: Db, ids: string[], statement: string) =>
  Effect.gen(function* () {
    for (const id of ids) {
      yield* db.run(sql`INSERT OR IGNORE INTO session_archive_guard (session_id) VALUES (${id})`).pipe(Effect.orDie)
    }
    yield* db.run(sql.raw(statement)).pipe(Effect.orDie)
    yield* db.run(sql`DELETE FROM session_archive_guard`).pipe(Effect.orDie)
  })

const tableInfo = (db: Db, table: string) =>
  db.all<{ name: string; notnull: number; pk: number; dflt_value: string | null }>(
    sql.raw(`PRAGMA table_info('${table}')`),
  )

const indexInfo = (db: Db, index: string) => db.all<{ name: string }>(sql.raw(`PRAGMA index_info('${index}')`))

const indexList = (db: Db, table: string) =>
  db.all<{ name: string; unique: number }>(sql.raw(`PRAGMA index_list('${table}')`))

const assertFamilyArchiveSchema = (db: Db) =>
  Effect.gen(function* () {
    const columns = yield* tableInfo(db, "session")
    const byName = new Map(columns.map((column) => [column.name, column]))
    for (const name of ["family_archive_id", "archive_cause", "archive_state", "archive_state_at"]) {
      expect(byName.get(name), name).toMatchObject({ notnull: 0, pk: 0 })
    }

    const tables = yield* db.all<{ name: string }>(
      sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('session_run_lease', 'session_import_exempt', 'session_archive_guard', 'family_archive_migration')`,
    )
    expect(tables.map((table) => table.name).sort()).toEqual([
      "family_archive_migration",
      "session_archive_guard",
      "session_import_exempt",
      "session_run_lease",
    ])

    const lease = yield* tableInfo(db, "session_run_lease")
    expect(lease.map((column) => column.name)).toEqual(["session_id", "lease_id", "heartbeat_at", "cancel_requested"])
    expect(lease.find((column) => column.name === "session_id")?.pk).toBe(1)
    expect(lease.find((column) => column.name === "lease_id")?.notnull).toBe(1)
    expect(lease.find((column) => column.name === "heartbeat_at")?.notnull).toBe(1)
    expect(lease.find((column) => column.name === "cancel_requested")).toMatchObject({
      notnull: 1,
      dflt_value: "0",
    })

    const exempt = yield* tableInfo(db, "session_import_exempt")
    expect(exempt.map((column) => column.name)).toEqual(["session_id", "expires_at"])
    expect(exempt.find((column) => column.name === "session_id")?.pk).toBe(1)
    expect(exempt.find((column) => column.name === "expires_at")?.notnull).toBe(1)

    const guard = yield* tableInfo(db, "session_archive_guard")
    expect(guard.map((column) => column.name)).toEqual(["session_id"])
    expect(guard[0]?.pk).toBe(1)

    const archiveMigration = yield* tableInfo(db, "family_archive_migration")
    expect(archiveMigration.map((column) => column.name)).toEqual(["root_session_id", "root_archived_at", "applied_at"])
    expect(archiveMigration.find((column) => column.name === "root_session_id")?.pk).toBe(1)
    expect(archiveMigration.find((column) => column.name === "root_archived_at")?.notnull).toBe(1)
    expect(archiveMigration.find((column) => column.name === "applied_at")?.notnull).toBe(1)

    const sessionIndexes = yield* indexList(db, "session")
    expect(sessionIndexes.find((index) => index.name === "session_family_archive_idx")?.unique).toBe(0)
    expect(sessionIndexes.find((index) => index.name === "session_archive_state_idx")?.unique).toBe(0)
    expect((yield* indexInfo(db, "session_family_archive_idx")).map((column) => column.name)).toEqual([
      "family_archive_id",
    ])
    expect((yield* indexInfo(db, "session_archive_state_idx")).map((column) => column.name)).toEqual(["archive_state"])

    const leaseIndexes = yield* indexList(db, "session_run_lease")
    expect(leaseIndexes.find((index) => index.name === "session_run_lease_heartbeat_idx")?.unique).toBe(0)
    expect((yield* indexInfo(db, "session_run_lease_heartbeat_idx")).map((column) => column.name)).toEqual([
      "heartbeat_at",
    ])
  })

const triggerNames = (db: Db) =>
  db.all<{ name: string }>(sql`SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name`).pipe(
    Effect.orDie,
    Effect.map((rows) => rows.map((row) => row.name)),
  )

describe("family archive migrations", () => {
  test("family archive columns, tables, and indexes exist on a fresh database", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)

        yield* assertFamilyArchiveSchema(db)

        const journal = yield* db.all<{ id: string }>(
          sql`SELECT id FROM migration WHERE id LIKE '20261023%' ORDER BY id`,
        )
        expect(journal.map((row) => row.id)).toEqual([familyArchiveMigration.id, familyArchiveBackfillMigration.id])
      }),
    )
  })

  test("upgrades an existing session table through both family archive migrations", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.run(sql`
          CREATE TABLE session (
            id text PRIMARY KEY,
            project_id text NOT NULL,
            workspace_id text,
            parent_id text,
            slug text NOT NULL,
            directory text NOT NULL,
            path text,
            title text NOT NULL,
            version text NOT NULL,
            share_url text,
            summary_additions integer,
            summary_deletions integer,
            summary_files integer,
            summary_diffs text,
            metadata text,
            cost real DEFAULT 0 NOT NULL,
            tokens_input integer DEFAULT 0 NOT NULL,
            tokens_output integer DEFAULT 0 NOT NULL,
            tokens_reasoning integer DEFAULT 0 NOT NULL,
            tokens_cache_read integer DEFAULT 0 NOT NULL,
            tokens_cache_write integer DEFAULT 0 NOT NULL,
            revert text,
            permission text,
            agent text,
            model text,
            time_created integer NOT NULL,
            time_updated integer NOT NULL,
            time_compacting integer,
            time_archived integer
          )
        `)
        expect((yield* tableInfo(db, "session")).map((column) => column.name)).not.toContain("family_archive_id")
        // The hand-written trigger migration references these tables, so the
        // partial-upgrade fixture must provide them.
        for (const table of [
          "message",
          "part",
          "session_input",
          "session_message",
          "todo",
          "session_context_epoch",
          "session_share",
        ]) {
          yield* db.run(sql`CREATE TABLE ${sql.identifier(table)} (id text, session_id text, message_id text)`)
        }

        yield* DatabaseMigration.applyOnly(db, [familyArchiveMigration, familyArchiveBackfillMigration])

        yield* assertFamilyArchiveSchema(db)

        const journal = yield* db.all<{ id: string }>(sql`SELECT id FROM migration ORDER BY id`)
        expect(journal.map((row) => row.id)).toEqual([familyArchiveMigration.id, familyArchiveBackfillMigration.id])
      }),
    )
  })

  test("installs every archive trigger and applying the migrations twice is a journal-gated no-op", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)

        const expected = [
          "message_family_archive_insert",
          "message_family_archive_update",
          "part_family_archive_insert",
          "part_family_archive_update",
          "session_context_epoch_family_archive_insert",
          "session_context_epoch_family_archive_update",
          "session_family_archive_insert",
          "session_family_archive_update",
          "session_input_family_archive_insert",
          "session_input_family_archive_update",
          "session_message_family_archive_insert",
          "session_message_family_archive_update",
          "session_share_family_archive_insert",
          "session_share_family_archive_update",
          "todo_family_archive_insert",
          "todo_family_archive_update",
        ]
        expect(yield* triggerNames(db)).toEqual(expected)

        yield* DatabaseMigration.apply(db)
        expect(yield* triggerNames(db)).toEqual(expected)
        const journal = yield* db.all<{ n: number }>(sql`SELECT COUNT(*) AS n FROM migration WHERE id LIKE '20261023%'`)
        expect(journal[0]?.n).toBe(2)
      }),
    )
  })

  test("registers the backfill after the generated migration and marks it fresh-schema", () => {
    const ids = migrations.map((migration) => migration.id)
    expect(ids.indexOf(familyArchiveMigration.id)).toBeGreaterThanOrEqual(0)
    expect(ids.indexOf(familyArchiveMigration.id)).toBeLessThan(ids.indexOf(familyArchiveBackfillMigration.id))
    expect(migrations.find((migration) => migration.id === familyArchiveBackfillMigration.id)).toMatchObject({
      runsOnFreshSchema: true,
    })
    expect(
      migrations.find((migration) => migration.id === familyArchiveMigration.id)?.runsOnFreshSchema,
    ).toBeUndefined()
  })
})

describe("family archive backfill", () => {
  test("stamps archived roots and 115 descendants, keeps data, preserves activity, and is idempotent", async () => {
    await run(
      Effect.gen(function* () {
        const direct = Array.from({ length: 100 }, (_, index) => `c${String(index).padStart(3, "0")}`)
        const recursive = Array.from({ length: 10 }, (_, index) => `g${String(index).padStart(3, "0")}`)
        const other = Array.from({ length: 5 }, (_, index) => `b${String(index).padStart(3, "0")}`)
        const active = new Set([...direct.slice(0, 12), ...recursive.slice(0, 6)])

        const db = yield* seedExisting((db) =>
          Effect.gen(function* () {
            yield* seedProject(db)
            yield* addSession(db, "root_a", { archived: 1000 })
            yield* addSession(db, "root_b", { archived: 1500 })
            for (const id of direct) {
              yield* addSession(db, id, { parent: "root_a", updated: active.has(id) ? 1001 : 10 })
            }
            for (const id of recursive) {
              yield* addSession(db, id, { parent: "c000", updated: active.has(id) ? 1002 : 10 })
            }
            for (const id of other) {
              yield* addSession(db, id, { parent: "root_b", updated: 10 })
            }
            yield* addMessage(db, "msg_a", "root_a")
          }),
        )

        const beforeSessions = yield* count(db, "session")
        const beforeMessages = yield* count(db, "message")

        for (const id of ["root_a", ...direct, ...recursive]) {
          const value = yield* row(db, id)
          expect(value.time_archived).toBe(1000)
          expect(value.family_archive_id).toBe("root_a")
        }
        for (const id of ["root_b", ...other]) {
          const value = yield* row(db, id)
          expect(value.time_archived).toBe(1500)
          expect(value.family_archive_id).toBe("root_b")
        }
        expect((yield* row(db, "root_a")).archive_cause).toBe("migration")
        expect((yield* row(db, "root_b")).archive_cause).toBe("migration")
        expect((yield* row(db, "c000")).archive_cause).toBe("migration")
        expect((yield* row(db, "c000")).time_updated).toBe(1001)
        expect((yield* row(db, "c050")).time_updated).toBe(10)
        expect(yield* count(db, "session")).toBe(beforeSessions)
        expect(yield* count(db, "message")).toBe(beforeMessages)
        expect(yield* guardCount(db)).toBe(0)

        const snapshots = yield* db
          .all<{
            root_session_id: string
            root_archived_at: number
          }>(sql`SELECT root_session_id, root_archived_at FROM family_archive_migration ORDER BY root_session_id`)
          .pipe(Effect.orDie)
        expect(snapshots).toEqual([
          { root_session_id: "root_a", root_archived_at: 1000 },
          { root_session_id: "root_b", root_archived_at: 1500 },
        ])

        const snapshot = yield* db
          .all<Record<string, unknown>>(sql`SELECT * FROM session ORDER BY id`)
          .pipe(Effect.orDie)
        yield* DatabaseMigration.applyOnly(db, [familyArchiveBackfillMigration])
        expect(
          yield* db.all<Record<string, unknown>>(sql`SELECT * FROM session ORDER BY id`).pipe(Effect.orDie),
        ).toEqual(snapshot)
        expect(yield* count(db, "session_archive_guard")).toBe(0)
      }),
    )
  })

  test("nested archived roots stay independent and unarchive each level correctly", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* seedExisting((db) =>
          Effect.gen(function* () {
            yield* seedProject(db)
            yield* addSession(db, "P", { archived: 1000 })
            yield* addSession(db, "C", { parent: "P", archived: 2000 })
            yield* addSession(db, "G", { parent: "C" })
          }),
        )

        expect(yield* row(db, "P")).toMatchObject({
          time_archived: 1000,
          family_archive_id: "P",
          archive_cause: "migration",
        })
        expect(yield* row(db, "C")).toMatchObject({
          time_archived: 2000,
          family_archive_id: "C",
          archive_cause: "migration",
        })
        expect(yield* row(db, "G")).toMatchObject({
          time_archived: 2000,
          family_archive_id: "C",
          archive_cause: "migration",
        })

        yield* guardedUpdate(
          db,
          ["P"],
          "UPDATE session SET time_archived = NULL, family_archive_id = NULL, archive_cause = NULL, archive_state = NULL, archive_state_at = NULL WHERE id = 'P'",
        )
        expect((yield* row(db, "P")).time_archived).toBe(null)
        expect((yield* row(db, "P")).family_archive_id).toBe(null)
        expect((yield* row(db, "C")).time_archived).toBe(2000)
        expect((yield* row(db, "G")).time_archived).toBe(2000)

        yield* guardedUpdate(
          db,
          ["C", "G"],
          "UPDATE session SET time_archived = NULL, family_archive_id = NULL, archive_cause = NULL, archive_state = NULL, archive_state_at = NULL WHERE id IN ('C', 'G')",
        )
        expect((yield* row(db, "C")).time_archived).toBe(null)
        expect((yield* row(db, "G")).time_archived).toBe(null)
      }),
    )
  })

  test("produces the same result regardless of seed order", async () => {
    const ids = ["a", "b", "c", "d", "e", "f"]
    const seed = (order: string[]) => (db: Db) =>
      Effect.gen(function* () {
        yield* seedProject(db)
        yield* addSession(db, "P", { archived: 1000 })
        yield* addSession(db, "C", { parent: "P", archived: 2000 })
        for (const id of order) {
          yield* addSession(db, id, { parent: id === "f" ? "C" : "P" })
        }
      })
    const read = (db: Db) =>
      Effect.gen(function* () {
        const rows = yield* db
          .all<{
            id: string
            time_archived: number | null
            family_archive_id: string | null
            archive_cause: string | null
          }>(sql`SELECT id, time_archived, family_archive_id, archive_cause FROM session ORDER BY id`)
          .pipe(Effect.orDie)
        return rows
      })

    const ascending = await run(
      Effect.gen(function* () {
        const db = yield* seedExisting(seed(ids))
        return yield* read(db)
      }),
    )
    const descending = await run(
      Effect.gen(function* () {
        const db = yield* seedExisting(seed([...ids].reverse()))
        return yield* read(db)
      }),
    )
    expect(descending).toEqual(ascending)
  })

  test("a legacy archive after the migration is rejected by the trigger", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* seedExisting((db) =>
          Effect.gen(function* () {
            yield* seedProject(db)
            yield* addSession(db, "root", { archived: 1000 })
            yield* addSession(db, "child", { parent: "root" })
            yield* addSession(db, "standalone")
          }),
        )

        yield* expectAbort(db.run(sql`UPDATE session SET time_archived = 3000 WHERE id = 'standalone'`))
        yield* expectAbort(
          db.run(
            sql`INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES ('late', 'child', 1, 1, '{}')`,
          ),
        )
        expect((yield* row(db, "standalone")).time_archived).toBe(null)
        expect(yield* count(db, "message")).toBe(0)
      }),
    )
  })
})

describe("family archive rollback", () => {
  const seedFamily = (db: Db) =>
    Effect.gen(function* () {
      yield* seedProject(db)
      yield* addSession(db, "P", { archived: 1000 })
      yield* addSession(db, "C", { parent: "P" })
      yield* addSession(db, "G", { parent: "C" })
      yield* addMessage(db, "msg_p", "P")
    })

  test("clears a migrated family, preserves the root archive time, and removes guards and snapshots", async () => {
    const { filename, cleanup } = await withFileDatabase(seedFamily)
    try {
      const db = new SQLite(filename)
      try {
        db.run("INSERT OR IGNORE INTO session_archive_guard (session_id) VALUES ('G')")
        db.run("UPDATE session SET archive_state = 'stale', archive_state_at = 42 WHERE id = 'G'")
        db.run("DELETE FROM session_archive_guard")

        const report = rollback(db)
        expect(report).toMatchObject({ roots: 1, descendants: 2, cleared: 3, skipped_snapshots: [] })

        const rows = db
          .query<
            {
              id: string
              time_archived: number | null
              family_archive_id: string | null
              archive_cause: string | null
              archive_state: string | null
              archive_state_at: number | null
            },
            []
          >(
            "SELECT id, time_archived, family_archive_id, archive_cause, archive_state, archive_state_at FROM session ORDER BY id",
          )
          .all()
        expect(rows).toEqual([
          {
            id: "C",
            time_archived: null,
            family_archive_id: null,
            archive_cause: null,
            archive_state: null,
            archive_state_at: null,
          },
          {
            id: "G",
            time_archived: null,
            family_archive_id: null,
            archive_cause: null,
            archive_state: null,
            archive_state_at: null,
          },
          {
            id: "P",
            time_archived: 1000,
            family_archive_id: null,
            archive_cause: null,
            archive_state: null,
            archive_state_at: null,
          },
        ])
        expect(db.query("SELECT COUNT(*) AS n FROM session_archive_guard").get()).toEqual({ n: 0 })
        expect(db.query("SELECT COUNT(*) AS n FROM family_archive_migration").get()).toEqual({ n: 0 })
        expect(db.query("SELECT COUNT(*) AS n FROM session").get()).toEqual({ n: 3 })
        expect(db.query("SELECT COUNT(*) AS n FROM message").get()).toEqual({ n: 1 })
      } finally {
        db.close()
      }
    } finally {
      await cleanup()
    }
  })

  test("skips user re-archived and time-changed rows and keeps their snapshots", async () => {
    const { filename, cleanup } = await withFileDatabase(seedFamily)
    try {
      const db = new SQLite(filename)
      try {
        db.run("INSERT OR IGNORE INTO session_archive_guard (session_id) VALUES ('C')")
        db.run(
          "UPDATE session SET time_archived = 5000, family_archive_id = 'C', archive_cause = 'user' WHERE id = 'C'",
        )
        db.run("DELETE FROM session_archive_guard")

        db.run("INSERT OR IGNORE INTO session_archive_guard (session_id) VALUES ('G')")
        db.run("UPDATE session SET time_archived = 4000 WHERE id = 'G'")
        db.run("DELETE FROM session_archive_guard")

        const report = rollback(db)
        expect(report.skipped_rows.sort()).toEqual(["C", "G"])
        expect(report.roots).toBe(1)
        expect(report.descendants).toBe(0)

        expect(
          db.query("SELECT time_archived, family_archive_id, archive_cause FROM session WHERE id = 'C'").get(),
        ).toEqual({
          time_archived: 5000,
          family_archive_id: "C",
          archive_cause: "user",
        })
        expect(
          db.query("SELECT time_archived, family_archive_id, archive_cause FROM session WHERE id = 'G'").get(),
        ).toEqual({
          time_archived: 4000,
          family_archive_id: "P",
          archive_cause: "migration",
        })
        expect(db.query("SELECT time_archived FROM session WHERE id = 'P'").get()).toEqual({ time_archived: 1000 })
        expect(db.query("SELECT COUNT(*) AS n FROM family_archive_migration").get()).toEqual({ n: 0 })
        expect(db.query("SELECT COUNT(*) AS n FROM session_archive_guard").get()).toEqual({ n: 0 })
      } finally {
        db.close()
      }
    } finally {
      await cleanup()
    }
  })

  test("keeps the snapshot when a family was unarchived after the migration", async () => {
    const { filename, cleanup } = await withFileDatabase(seedFamily)
    try {
      const db = new SQLite(filename)
      try {
        for (const id of ["P", "C", "G"]) {
          db.run("INSERT OR IGNORE INTO session_archive_guard (session_id) VALUES (?)", [id])
        }
        db.run(
          "UPDATE session SET time_archived = NULL, family_archive_id = NULL, archive_cause = NULL, archive_state = NULL, archive_state_at = NULL WHERE id IN ('P', 'C', 'G')",
        )
        db.run("DELETE FROM session_archive_guard")

        const report = rollback(db)
        expect(report.cleared).toBe(0)
        expect(report.skipped_snapshots).toEqual(["P"])
        expect(db.query("SELECT COUNT(*) AS n FROM family_archive_migration").get()).toEqual({ n: 1 })
        expect(db.query("SELECT time_archived FROM session WHERE id = 'P'").get()).toEqual({ time_archived: null })
        expect(db.query("SELECT COUNT(*) AS n FROM session_archive_guard").get()).toEqual({ n: 0 })
      } finally {
        db.close()
      }
    } finally {
      await cleanup()
    }
  })

  test("refuses to roll back while any session is archiving", async () => {
    const { filename, cleanup } = await withFileDatabase(seedFamily)
    try {
      const db = new SQLite(filename)
      try {
        db.run("INSERT OR IGNORE INTO session_archive_guard (session_id) VALUES ('C')")
        db.run("UPDATE session SET archive_state = 'archiving', archive_state_at = 1 WHERE id = 'C'")
        db.run("DELETE FROM session_archive_guard")

        expect(() => rollback(db)).toThrow(/archiving/)
        expect(db.query("SELECT time_archived, family_archive_id FROM session WHERE id = 'C'").get()).toEqual({
          time_archived: 1000,
          family_archive_id: "P",
        })
        expect(db.query("SELECT COUNT(*) AS n FROM family_archive_migration").get()).toEqual({ n: 1 })
        expect(db.query("SELECT COUNT(*) AS n FROM session_archive_guard").get()).toEqual({ n: 0 })
      } finally {
        db.close()
      }
    } finally {
      await cleanup()
    }
  })
})

describe("family archive reconciliation", () => {
  test("stamps a fully archived null/null root and archived descendants with an archived ancestor", async () => {
    const { filename, cleanup } = await withFileDatabase((db) =>
      Effect.gen(function* () {
        yield* seedProject(db)
        yield* addSession(db, "R", { archived: 3000 })
        yield* addSession(db, "P", { archived: 1000 })
        yield* addSession(db, "C", { parent: "P", archived: 2000 })
        yield* addSession(db, "G", { parent: "C", archived: 2500 })
        yield* addSession(db, "U", { parent: "P" })
      }),
    )
    try {
      // The backfill already migrated these rows; reset only the null/null rows
      // to simulate a pre-trigger writer that archived after the migration.
      const db = new SQLite(filename)
      try {
        db.run("UPDATE family_archive_migration SET root_archived_at = root_archived_at")
        for (const id of ["R", "P", "C", "G", "U"]) {
          db.run("INSERT OR IGNORE INTO session_archive_guard (session_id) VALUES (?)", [id])
        }
        db.run("UPDATE session SET family_archive_id = NULL, archive_cause = NULL WHERE id IN ('R', 'P', 'C', 'G')")
        db.run(
          "UPDATE session SET time_archived = NULL, family_archive_id = NULL, archive_cause = NULL, archive_state = NULL, archive_state_at = NULL WHERE id = 'U'",
        )
        db.run("DELETE FROM session_archive_guard")
        db.run("DELETE FROM family_archive_migration")

        const report = reconcile(db)
        expect(report.updated.sort()).toEqual(["C", "G", "P", "R"])
        expect(
          db.query("SELECT time_archived, family_archive_id, archive_cause FROM session WHERE id = 'R'").get(),
        ).toEqual({
          time_archived: 3000,
          family_archive_id: "R",
          archive_cause: "user",
        })
        expect(
          db.query("SELECT time_archived, family_archive_id, archive_cause FROM session WHERE id = 'P'").get(),
        ).toEqual({
          time_archived: 1000,
          family_archive_id: "P",
          archive_cause: "user",
        })
        expect(
          db.query("SELECT time_archived, family_archive_id, archive_cause FROM session WHERE id = 'C'").get(),
        ).toEqual({
          time_archived: 2000,
          family_archive_id: "P",
          archive_cause: "migration",
        })
        expect(
          db.query("SELECT time_archived, family_archive_id, archive_cause FROM session WHERE id = 'G'").get(),
        ).toEqual({
          time_archived: 2500,
          family_archive_id: "P",
          archive_cause: "migration",
        })
        expect(
          db.query("SELECT time_archived, family_archive_id, archive_cause FROM session WHERE id = 'U'").get(),
        ).toEqual({
          time_archived: null,
          family_archive_id: null,
          archive_cause: null,
        })
        expect(db.query("SELECT COUNT(*) AS n FROM session_archive_guard").get()).toEqual({ n: 0 })

        expect(reconcile(db).updated).toEqual([])
      } finally {
        db.close()
      }
    } finally {
      await cleanup()
    }
  })
})

describe("family archive dry run", () => {
  test("opens read-only and reports roots, descendants, and post-archive activity", async () => {
    const dir = await mkdtemp(join(tmpdir(), "family-archive-dry-run-"))
    const filename = join(dir, "test.db")
    try {
      await runFile(
        filename,
        Effect.gen(function* () {
          const db = yield* makeDb
          yield* db.transaction((tx) => schema.up(tx))
          yield* seedProject(db)
          yield* addSession(db, "root", { archived: 1000 })
          for (let index = 0; index < 20; index += 1) {
            yield* addSession(db, `c${index}`, {
              parent: "root",
              updated: index < 18 ? 1001 : 10,
            })
          }
          yield* addSession(db, "other", { parent: "c0" })
        }),
      )

      const report = dryRun(filename)
      expect(report.counts).toEqual({
        roots: 1,
        unarchived_descendants: 21,
        post_archive_activity: 18,
      })
      expect(report.roots[0]).toMatchObject({
        id: "root",
        time_archived: 1000,
        unarchived_descendants: 21,
        post_archive_activity: 18,
      })
      expect(report.post_archive_activity.map((row) => row.id)).toEqual(expect.arrayContaining(["c0", "c17"]))
      expect(report.unarchived_descendants.find((row) => row.id === "c18")?.post_archive_activity).toBe(false)
    } finally {
      await rm(dir, { recursive: true, force: true })
    }
  })
})
