import { Effect } from "effect"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20261028223037_family_archive",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`family_archive_migration\` (
          \`root_session_id\` text PRIMARY KEY,
          \`root_archived_at\` integer NOT NULL,
          \`applied_at\` integer NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`session_archive_guard\` (
          \`session_id\` text PRIMARY KEY
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`session_import_exempt\` (
          \`session_id\` text PRIMARY KEY,
          \`expires_at\` integer NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`session_purged\` (
          \`session_id\` text PRIMARY KEY,
          \`purged_at\` integer NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`session_run_lease\` (
          \`session_id\` text PRIMARY KEY,
          \`lease_id\` text NOT NULL,
          \`heartbeat_at\` integer NOT NULL,
          \`cancel_requested\` integer DEFAULT 0 NOT NULL
        );
      `)
      yield* tx.run(`ALTER TABLE \`session_v2\` ADD \`family_archive_id\` text;`)
      yield* tx.run(`ALTER TABLE \`session_v2\` ADD \`archive_cause\` text;`)
      yield* tx.run(`ALTER TABLE \`session_v2\` ADD \`archive_state\` text;`)
      yield* tx.run(`ALTER TABLE \`session_v2\` ADD \`archive_state_at\` integer;`)
      yield* tx.run(
        `CREATE INDEX IF NOT EXISTS \`session_run_lease_heartbeat_idx\` ON \`session_run_lease\` (\`heartbeat_at\`);`,
      )
      yield* tx.run(
        `CREATE INDEX IF NOT EXISTS \`session_v2_family_archive_idx\` ON \`session_v2\` (\`family_archive_id\`);`,
      )
      yield* tx.run(
        `CREATE INDEX IF NOT EXISTS \`session_v2_archive_state_idx\` ON \`session_v2\` (\`archive_state\`);`,
      )
    })
  },
}

export default migration
