export * as FamilyArchiveBackfill from "./family-archive-backfill.js"

import { sql } from "drizzle-orm"
import { Effect, Layer } from "effect"
import { Database } from "./database.js"
import { V1Migration } from "./v1-migration.js"
import { reconcileStaleArchiveState } from "../session/family-archive.js"

// The V1 import copies time_archived but not the family archive fields. This
// backfill runs after the import reaches phase "completed" so the legacy
// session rows it reads are the final ones. It is idempotent: every field is
// copied only when the session_v2 value is still null, so a newer value written
// by the archive service is never overwritten.
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
        // Phase 4.3 adds the archive guard trigger on session_v2. This UPDATE
        // changes archive fields, so that trigger will require a
        // session_archive_guard row (or an import exemption) for every affected
        // session. Add the guard insert here when the trigger lands.
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
            ),
            archive_state = COALESCE(
              archive_state,
              (SELECT session.archive_state FROM session WHERE session.id = session_v2.id)
            ),
            archive_state_at = COALESCE(
              archive_state_at,
              (SELECT session.archive_state_at FROM session WHERE session.id = session_v2.id)
            )
          WHERE EXISTS (SELECT 1 FROM session WHERE session.id = session_v2.id)
            AND (
              (family_archive_id IS NULL
                AND (SELECT session.family_archive_id FROM session WHERE session.id = session_v2.id) IS NOT NULL)
              OR (archive_cause IS NULL
                AND (SELECT session.archive_cause FROM session WHERE session.id = session_v2.id) IS NOT NULL)
              OR (archive_state IS NULL
                AND (SELECT session.archive_state FROM session WHERE session.id = session_v2.id) IS NOT NULL)
              OR (archive_state_at IS NULL
                AND (SELECT session.archive_state_at FROM session WHERE session.id = session_v2.id) IS NOT NULL)
            )
        `)
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
