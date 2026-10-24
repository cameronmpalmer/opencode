import { describe, expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import type { SqlClient as SqlClientService } from "effect/unstable/sql/SqlClient"

const run = <A, E>(effect: Effect.Effect<A, E, SqlClientService>) =>
  Effect.runPromise(
    effect.pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped),
  )

const makeDb = EffectDrizzleSqlite.makeWithDefaults()

describe("family archive migrations", () => {
  test("family archive columns and tables exist after migrations", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)

        const columns = yield* db.all<{ name: string }>(sql`PRAGMA table_info(session)`)
        expect(columns.map((column) => column.name)).toEqual(
          expect.arrayContaining(["family_archive_id", "archive_cause", "archive_state", "archive_state_at"]),
        )

        const tables = yield* db.all<{ name: string }>(sql`SELECT name FROM sqlite_master WHERE type = 'table'`)
        expect(tables.map((table) => table.name)).toEqual(
          expect.arrayContaining([
            "session_run_lease",
            "session_import_exempt",
            "session_archive_guard",
            "family_archive_migration",
          ]),
        )

        const indexes = yield* db.all<{ name: string }>(
          sql`SELECT name FROM sqlite_master WHERE type = 'index' AND name IN ('session_family_archive_idx', 'session_archive_state_idx', 'session_run_lease_heartbeat_idx')`,
        )
        expect(indexes.map((index) => index.name).sort()).toEqual([
          "session_archive_state_idx",
          "session_family_archive_idx",
          "session_run_lease_heartbeat_idx",
        ])
      }),
    )
  })
})
