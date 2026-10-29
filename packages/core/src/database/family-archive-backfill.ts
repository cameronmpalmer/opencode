export * as FamilyArchiveBackfill from "./family-archive-backfill.js"

import { sql } from "drizzle-orm"
import { Effect, Layer } from "effect"
import { Database } from "./database.js"
import { V1Migration } from "./v1-migration.js"
import { reconcileStaleArchiveState } from "../session/family-archive.js"

// The V1 import copies time_archived but not the family archive fields. This
// backfill runs after the import reaches phase "completed" so the legacy
// session rows it reads are the final ones. It is idempotent: the durable
// family fields are copied only when the session_v2 value is still null, so a
// newer value written by the archive service is never overwritten. The
// transient archive_state/archive_state_at markers are deliberately not copied;
// a stale "archiving" marker in the legacy table must not resurrect after the
// startup sweep.
const FAMILY_COLUMNS = ["family_archive_id", "archive_cause", "archive_state", "archive_state_at"] as const

export function run(): Effect.Effect<void, unknown, Database.Service> {
  return Effect.gen(function* () {
    const db = (yield* Database.Service).db
    const legacy = yield* db.get<{ name: string }>(
      sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'session'`,
    )
    // Fresh V2 databases have no legacy session table, so there is nothing to copy.
    if (!legacy) return
    const columns = new Set(
      (yield* db.all<{ name: string }>(sql`SELECT name FROM pragma_table_info('session')`)).map((row) => row.name),
    )
    // A legacy database that predates the family branch has no archive columns.
    if (!FAMILY_COLUMNS.every((column) => columns.has(column))) return
    yield* db.transaction((tx) =>
      Effect.gen(function* () {
        // The trigger requires a guard for every archive-field write. Collect
        // the rows this backfill will touch, authorize them, update, then
        // release only the guards this backfill created.
        yield* tx.run(sql`CREATE TEMP TABLE IF NOT EXISTS family_archive_backfill_ids (session_id text PRIMARY KEY)`)
        yield* tx.run(sql`DELETE FROM temp.family_archive_backfill_ids`)
        yield* tx.run(sql`
          INSERT OR IGNORE INTO temp.family_archive_backfill_ids (session_id)
          SELECT session_v2.id
          FROM session_v2
          JOIN session ON session.id = session_v2.id
          WHERE (session_v2.family_archive_id IS NULL AND session.family_archive_id IS NOT NULL)
             OR (session_v2.archive_cause IS NULL AND session.archive_cause IS NOT NULL)
        `)
        yield* tx.run(sql`
          INSERT OR IGNORE INTO session_archive_guard (session_id)
          SELECT session_id FROM temp.family_archive_backfill_ids
        `)
        yield* tx.run(sql`
          UPDATE session_v2
          SET
            family_archive_id = COALESCE(
              family_archive_id,
              (SELECT session.family_archive_id FROM session WHERE session.id = session_v2.id)
            ),
            archive_cause = COALESCE(
              archive_cause,
              (SELECT session.archive_cause FROM session WHERE session.id = session_v2.id)
            )
          WHERE EXISTS (SELECT 1 FROM session WHERE session.id = session_v2.id)
            AND (
              (family_archive_id IS NULL
                AND (SELECT session.family_archive_id FROM session WHERE session.id = session_v2.id) IS NOT NULL)
              OR (archive_cause IS NULL
                AND (SELECT session.archive_cause FROM session WHERE session.id = session_v2.id) IS NOT NULL)
            )
        `)
        yield* tx.run(sql`
          DELETE FROM session_archive_guard
          WHERE session_id IN (SELECT session_id FROM temp.family_archive_backfill_ids)
        `)
        yield* tx.run(sql`DROP TABLE temp.family_archive_backfill_ids`)
        // The V1 branch recorded a snapshot per archived root. A database that
        // already has them keeps its original applied_at.
        yield* tx.run(sql`
          INSERT OR IGNORE INTO family_archive_migration (root_session_id, root_archived_at, applied_at)
          SELECT id, time_archived, ${Date.now()}
          FROM session_v2
          WHERE time_archived IS NOT NULL AND family_archive_id = id
        `)
      }),
    )
  })
}

// Wait for the V1 import to finish before copying. A failed import leaves the
// backfill pending; the next startup retries both. Returns false when the
// import failed so the caller does not copy a partial import.
export const waitForMigration = (
  status: Effect.Effect<V1Migration.Status, never, Database.Service> = V1Migration.status(),
): Effect.Effect<boolean, never, Database.Service> =>
  Effect.gen(function* () {
    while (true) {
      const current = yield* status
      if (current.status === "completed") return true
      if (current.status === "error") {
        yield* Effect.logWarning("Skipped family archive backfill because the V1 migration failed")
        return false
      }
      yield* Effect.sleep("1 second")
    }
  })

export const runAfterMigration = (
  status: Effect.Effect<V1Migration.Status, never, Database.Service> = V1Migration.status(),
): Effect.Effect<void, unknown, Database.Service> =>
  Effect.gen(function* () {
    if (!(yield* waitForMigration(status))) return
    yield* run()
  })

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* Effect.gen(function* () {
      const db = (yield* Database.Service).db
      yield* reconcileStaleArchiveState(db).pipe(
        Effect.catchCause((cause) => Effect.logWarning("Family archive startup reconciliation failed", { cause })),
      )
      yield* runAfterMigration()
    }).pipe(
      Effect.catchCause((cause) => Effect.logWarning("Family archive backfill failed", { cause })),
      Effect.forkScoped({ startImmediately: true }),
    )
  }),
)
