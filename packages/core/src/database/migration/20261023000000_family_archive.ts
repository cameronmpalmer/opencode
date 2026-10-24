import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261023000000_family_archive",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`family_archive_migration\` (
          \`root_session_id\` text PRIMARY KEY,
          \`root_archived_at\` integer NOT NULL,
          \`applied_at\` integer NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`session_archive_guard\` (
          \`session_id\` text PRIMARY KEY
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`session_import_exempt\` (
          \`session_id\` text PRIMARY KEY,
          \`expires_at\` integer NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`session_run_lease\` (
          \`session_id\` text PRIMARY KEY,
          \`lease_id\` text NOT NULL,
          \`heartbeat_at\` integer NOT NULL,
          \`cancel_requested\` integer DEFAULT 0 NOT NULL
        );
      `)
      yield* tx.run(`ALTER TABLE \`session\` ADD \`family_archive_id\` text;`)
      yield* tx.run(`ALTER TABLE \`session\` ADD \`archive_cause\` text;`)
      yield* tx.run(`ALTER TABLE \`session\` ADD \`archive_state\` text;`)
      yield* tx.run(`ALTER TABLE \`session\` ADD \`archive_state_at\` integer;`)
      yield* tx.run(`CREATE INDEX \`session_run_lease_heartbeat_idx\` ON \`session_run_lease\` (\`heartbeat_at\`);`)
      yield* tx.run(`CREATE INDEX \`session_family_archive_idx\` ON \`session\` (\`family_archive_id\`);`)
      yield* tx.run(`CREATE INDEX \`session_archive_state_idx\` ON \`session\` (\`archive_state\`);`)
    })
  },
} satisfies DatabaseMigration.Migration
