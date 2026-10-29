import { describe, expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { Database } from "@opencode/core/database/database"
import { DatabaseMigration } from "@opencode/core/database/migration"
import familyArchiveMigration from "@opencode/core/database/migration/20261028223037_family_archive"
import sessionPurgedMigration from "@opencode/core/database/migration/20261028223038_session_purged"
import { FamilyArchiveBackfill } from "@opencode/core/database/family-archive-backfill"
import { V1Migration } from "@opencode/core/database/v1-migration"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { FSUtil } from "@opencode/util/fs-util"
import { Global } from "@opencode/util/global"
import { Project } from "@opencode/core/project"
import { Cause, Effect, Exit, Layer, Scope } from "effect"
import { sql } from "drizzle-orm"
import path from "path"
import type { SqlClient } from "effect/sql/SqlClient"

const makeDb = EffectDrizzleSqlite.makeWithDefaults()
const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope | Global.Service | SqlClient>) =>
  Effect.runPromise(
    Effect.scoped(
      effect.pipe(
        Effect.provideService(Global.Service, Global.make({ data: path.join(process.cwd(), ".test-data") })),
        Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
      ),
    ),
  )

const createLegacyTables = Effect.fnUntraced(function* (db: Effect.Success<typeof makeDb>) {
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
      time_archived integer,
      family_archive_id text,
      archive_cause text,
      archive_state text,
      archive_state_at integer
    )
  `)
  yield* db.run(sql`
    CREATE TABLE message (
      id text PRIMARY KEY,
      session_id text NOT NULL,
      time_created integer NOT NULL,
      time_updated integer NOT NULL,
      data text NOT NULL
    )
  `)
  yield* db.run(sql`
    CREATE TABLE part (
      id text PRIMARY KEY,
      message_id text NOT NULL,
      session_id text NOT NULL,
      time_created integer NOT NULL,
      time_updated integer NOT NULL,
      data text NOT NULL
    )
  `)
})

const database = <A, E>(
  effect: Effect.Effect<A, E, Database.Service | Global.Service | Scope.Scope | Project.Service | FSUtil.Service>,
) =>
  run(
    Effect.gen(function* () {
      const db = yield* makeDb
      yield* DatabaseMigration.apply(db)
      yield* createLegacyTables(db)
      return yield* effect.pipe(
        Effect.provide(
          AppNodeBuilder.build(LayerNode.group([Project.node, Database.node, FSUtil.node]), [
            Database.node.replace(Layer.succeed(Database.Service, { db })),
          ]),
        ),
      )
    }),
  )

const addLegacySession = (db: Effect.Success<typeof makeDb>, id: string) =>
  db
    .run(
      sql`
      INSERT INTO session (
        id, project_id, slug, directory, title, version, time_created, time_updated,
        time_archived, family_archive_id, archive_cause, archive_state, archive_state_at
      ) VALUES (
        ${id}, 'global', ${id}, '/tmp/test', ${id}, '1', 1, 2,
        1000, ${id}, 'migration', 'archiving', 123
      )
    `,
    )
    .pipe(Effect.orDie)

const expectAbort = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.gen(function* () {
    const exit = yield* effect.pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (!Exit.isFailure(exit)) return
    expect(Cause.pretty(exit.cause)).toContain("SessionPurgedError")
  })

describe("family archive migration", () => {
  test("copies V1 family metadata onto session_v2 and records the archived root", async () => {
    await database(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        yield* addLegacySession(db, "ses_family")

        expect(yield* V1Migration.run()).toEqual({ status: "completed" })
        yield* FamilyArchiveBackfill.run()

        expect(
          yield* db.get(sql`
            SELECT family_archive_id, archive_cause, archive_state, archive_state_at
            FROM session_v2 WHERE id = 'ses_family'
          `),
        ).toEqual({
          family_archive_id: "ses_family",
          archive_cause: "migration",
          // The transient archiving marker is intentionally not copied; the
          // startup sweep clears stale markers and the archive service sets
          // them only for the duration of an operation.
          archive_state: null,
          archive_state_at: null,
        })
        expect(
          yield* db.get(sql`
            SELECT root_session_id, root_archived_at FROM family_archive_migration
          `),
        ).toEqual({ root_session_id: "ses_family", root_archived_at: 1000 })

        // A second run keeps the same values and does not add another snapshot.
        yield* FamilyArchiveBackfill.run()
        expect(yield* db.get(sql`SELECT COUNT(*) AS count FROM family_archive_migration`)).toEqual({ count: 1 })
        expect(
          yield* db.get(sql`SELECT family_archive_id, archive_state_at FROM session_v2 WHERE id = 'ses_family'`),
        ).toEqual({ family_archive_id: "ses_family", archive_state_at: null })
      }),
    )
  })

  test("rejects a child insert or move under a purged session on session_v2", async () => {
    await database(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        yield* db
          .run(sql`INSERT INTO session_purged (session_id, purged_at) VALUES ('ses_purged', 1)`)
          .pipe(Effect.orDie)

        yield* expectAbort(
          db.run(sql`
            INSERT INTO session_v2 (id, project_id, slug, directory, version, parent_id, time_created, time_updated)
            VALUES ('ses_late', 'global', 'late', '/tmp/test', '1', 'ses_purged', 1, 1)
          `),
        )

        yield* db
          .run(
            sql`
            INSERT INTO session_v2 (id, project_id, slug, directory, version, time_created, time_updated)
            VALUES ('ses_child', 'global', 'child', '/tmp/test', '1', 1, 1)
          `,
          )
          .pipe(Effect.orDie)
        yield* expectAbort(db.run(sql`UPDATE session_v2 SET parent_id = 'ses_purged' WHERE id = 'ses_child'`))
      }),
    )
  })

  test("does not run the backfill when the V1 migration failed", async () => {
    await database(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        yield* addLegacySession(db, "ses_family")
        expect(yield* V1Migration.run()).toEqual({ status: "completed" })

        expect(yield* FamilyArchiveBackfill.waitForMigration(Effect.succeed({ status: "completed" } as const))).toBe(
          true,
        )
        expect(
          yield* FamilyArchiveBackfill.waitForMigration(
            Effect.succeed({ status: "error", error: "V1 migration failed" } as const),
          ),
        ).toBe(false)

        yield* FamilyArchiveBackfill.runAfterMigration(
          Effect.succeed({ status: "error", error: "V1 migration failed" } as const),
        )

        expect(
          yield* db.get(sql`
            SELECT family_archive_id, archive_cause, archive_state, archive_state_at
            FROM session_v2 WHERE id = 'ses_family'
          `),
        ).toEqual({
          family_archive_id: null,
          archive_cause: null,
          archive_state: null,
          archive_state_at: null,
        })
      }),
    )
  })

  test("does not overwrite newer family metadata already on session_v2", async () => {
    await database(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        yield* addLegacySession(db, "ses_family")
        expect(yield* V1Migration.run()).toEqual({ status: "completed" })
        yield* db
          .run(sql`INSERT OR IGNORE INTO session_archive_guard (session_id) VALUES ('ses_family')`)
          .pipe(Effect.orDie)
        yield* db
          .run(sql`UPDATE session_v2 SET family_archive_id = 'ses_newer_root' WHERE id = 'ses_family'`)
          .pipe(Effect.orDie)
        yield* db
          .run(sql`DELETE FROM session_archive_guard WHERE session_id = 'ses_family'`)
          .pipe(Effect.orDie)

        yield* FamilyArchiveBackfill.run()

        expect(yield* db.get(sql`SELECT family_archive_id FROM session_v2 WHERE id = 'ses_family'`)).toEqual({
          family_archive_id: "ses_newer_root",
        })
      }),
    )
  })

  test("is a no-op on a fresh V2 database without a legacy session table", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)
        yield* FamilyArchiveBackfill.run().pipe(Effect.provideService(Database.Service, { db }))
        expect(yield* db.get(sql`SELECT COUNT(*) AS count FROM family_archive_migration`)).toEqual({ count: 0 })
      }),
    )
  })

  test("is a no-op when the legacy session table has no family columns", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)
        yield* db.run(sql`CREATE TABLE session (id text PRIMARY KEY)`)
        yield* FamilyArchiveBackfill.run().pipe(Effect.provideService(Database.Service, { db }))
        expect(yield* db.get(sql`SELECT COUNT(*) AS count FROM family_archive_migration`)).toEqual({ count: 0 })
      }),
    )
  })

  test("reapplies cleanly over family tables the V1 branch already created", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.run(sql`CREATE TABLE session_v2 (id text PRIMARY KEY, parent_id text)`)
        yield* db.run(sql`
          CREATE TABLE family_archive_migration (
            root_session_id text PRIMARY KEY,
            root_archived_at integer NOT NULL,
            applied_at integer NOT NULL
          )
        `)
        yield* db.run(sql`CREATE TABLE session_archive_guard (session_id text PRIMARY KEY)`)
        yield* db.run(sql`
          CREATE TABLE session_import_exempt (
            session_id text PRIMARY KEY,
            expires_at integer NOT NULL
          )
        `)
        yield* db.run(sql`
          CREATE TABLE session_run_lease (
            session_id text PRIMARY KEY,
            lease_id text NOT NULL,
            heartbeat_at integer NOT NULL,
            cancel_requested integer DEFAULT 0 NOT NULL
          )
        `)
        yield* db.run(sql`CREATE INDEX session_run_lease_heartbeat_idx ON session_run_lease (heartbeat_at)`)
        yield* db.run(sql`
          CREATE TABLE session_purged (
            session_id text PRIMARY KEY,
            purged_at integer NOT NULL
          )
        `)

        yield* DatabaseMigration.applyOnly(db, [familyArchiveMigration, sessionPurgedMigration])

        expect(
          yield* db.all(sql`
            SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'session_v2_purged%' ORDER BY name
          `),
        ).toEqual([{ name: "session_v2_purged_parent_insert" }, { name: "session_v2_purged_parent_update" }])
        expect(
          yield* db.all(sql`SELECT name FROM pragma_table_info('session_v2') WHERE name = 'archive_state'`),
        ).toEqual([{ name: "archive_state" }])
      }),
    )
  })
})
