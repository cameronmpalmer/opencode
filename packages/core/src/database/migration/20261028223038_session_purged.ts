import { Effect } from "effect"
import type { DatabaseMigration } from "../migration.js"

// Purge tombstones close the orphan window: a purge deletes parent and child
// rows in one transaction, so a new child inserted afterwards would otherwise
// have a parent_id that names no live row. A tombstone for every deleted
// session blocks that insert until the parent is imported again.
//
// These triggers target session_v2. The V1 family branch's triggers target the
// legacy session table and may still exist, so use distinct names.
const tombstonedMissingParent = `
  EXISTS (SELECT 1 FROM \`session_purged\` WHERE session_id = NEW.parent_id)
  AND NOT EXISTS (SELECT 1 FROM \`session_v2\` WHERE id = NEW.parent_id)
`

export default {
  id: "20261028223038_session_purged",
  runsOnFreshSchema: true,
  up(tx) {
    return Effect.gen(function* () {
      // The generated schema also knows this table, so a fresh install may have
      // created it before the hand-written migrations run.
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`session_purged\` (
          \`session_id\` text PRIMARY KEY,
          \`purged_at\` integer NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TRIGGER IF NOT EXISTS \`session_v2_purged_parent_insert\`
        BEFORE INSERT ON \`session_v2\`
        WHEN NEW.parent_id IS NOT NULL
          AND ${tombstonedMissingParent}
        BEGIN
          SELECT RAISE(ABORT, 'SessionPurgedError: cannot create a child under a purged session');
        END;
      `)
      yield* tx.run(`
        CREATE TRIGGER IF NOT EXISTS \`session_v2_purged_parent_update\`
        BEFORE UPDATE OF \`parent_id\` ON \`session_v2\`
        WHEN NEW.parent_id IS NOT NULL
          AND ${tombstonedMissingParent}
        BEGIN
          SELECT RAISE(ABORT, 'SessionPurgedError: cannot move a child under a purged session');
        END;
      `)
    })
  },
} satisfies DatabaseMigration.Migration
