import { describe, expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { migrations } from "@opencode-ai/core/database/migration.gen"
import familyArchiveMigration from "@opencode-ai/core/database/migration/20261023000000_family_archive"
import familyArchiveBackfillMigration from "@opencode-ai/core/database/migration/20261023000001_family_archive_backfill"
import type { SqlClient as SqlClientService } from "effect/unstable/sql/SqlClient"

const run = <A, E>(effect: Effect.Effect<A, E, SqlClientService>) =>
  Effect.runPromise(
    effect.pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped),
  )

const makeDb = EffectDrizzleSqlite.makeWithDefaults()
type Db = Effect.Success<typeof makeDb>

const tableInfo = (db: Db, table: string) =>
  db.all<{ name: string; notnull: number; pk: number; dflt_value: string | null }>(
    sql.raw(`PRAGMA table_info('${table}')`),
  )

const indexInfo = (db: Db, index: string) => db.all<{ name: string }>(sql.raw(`PRAGMA index_info('${index}')`))

const indexList = (db: Db, table: string) => db.all<{ name: string; unique: number }>(sql.raw(`PRAGMA index_list('${table}')`))

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
    expect(lease.map((column) => column.name)).toEqual([
      "session_id",
      "lease_id",
      "heartbeat_at",
      "cancel_requested",
    ])
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
    expect(archiveMigration.map((column) => column.name)).toEqual([
      "root_session_id",
      "root_archived_at",
      "applied_at",
    ])
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

describe("family archive migrations", () => {
  test("family archive columns, tables, and indexes exist on a fresh database", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)

        yield* assertFamilyArchiveSchema(db)

        const journal = yield* db.all<{ id: string }>(sql`SELECT id FROM migration WHERE id LIKE '20261023%' ORDER BY id`)
        expect(journal.map((row) => row.id)).toEqual([
          familyArchiveMigration.id,
          familyArchiveBackfillMigration.id,
        ])
      }),
    )
  })

  test("upgrades an existing session table through both family archive migrations", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.run(sql`CREATE TABLE session (id text PRIMARY KEY)`)
        expect((yield* tableInfo(db, "session")).map((column) => column.name)).toEqual(["id"])

        yield* DatabaseMigration.applyOnly(db, [familyArchiveMigration, familyArchiveBackfillMigration])

        yield* assertFamilyArchiveSchema(db)

        const journal = yield* db.all<{ id: string }>(sql`SELECT id FROM migration ORDER BY id`)
        expect(journal.map((row) => row.id)).toEqual([
          familyArchiveMigration.id,
          familyArchiveBackfillMigration.id,
        ])
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
    expect(migrations.find((migration) => migration.id === familyArchiveMigration.id)?.runsOnFreshSchema).toBeUndefined()
  })
})
