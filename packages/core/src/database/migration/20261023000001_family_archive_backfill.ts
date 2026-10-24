import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

// Hand-written migration. Task A5 adds the archive triggers that reject writes to
// archived and archiving sessions from writers that predate the family archive.
const now = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`

const ancestorWalk = (parent: string) => `
  WITH RECURSIVE ancestors(id, parent_id, time_archived, archive_state) AS (
    SELECT s.id, s.parent_id, s.time_archived, s.archive_state
    FROM session s
    WHERE s.id = ${parent}
    UNION
    SELECT p.id, p.parent_id, p.time_archived, p.archive_state
    FROM session p
    JOIN ancestors a ON p.id = a.parent_id
  )
  SELECT 1 FROM ancestors
  WHERE ancestors.time_archived IS NOT NULL OR ancestors.archive_state = 'archiving'
`

const sessionScopedTables = ["session_input", "session_message", "todo", "session_context_epoch", "session_share"]

const nonArchiveColumnsChanged = `
  NEW.id IS NOT OLD.id
  OR NEW.project_id IS NOT OLD.project_id
  OR NEW.workspace_id IS NOT OLD.workspace_id
  OR NEW.parent_id IS NOT OLD.parent_id
  OR NEW.slug IS NOT OLD.slug
  OR NEW.directory IS NOT OLD.directory
  OR NEW.path IS NOT OLD.path
  OR NEW.title IS NOT OLD.title
  OR NEW.version IS NOT OLD.version
  OR NEW.share_url IS NOT OLD.share_url
  OR NEW.summary_additions IS NOT OLD.summary_additions
  OR NEW.summary_deletions IS NOT OLD.summary_deletions
  OR NEW.summary_files IS NOT OLD.summary_files
  OR NEW.summary_diffs IS NOT OLD.summary_diffs
  OR NEW.metadata IS NOT OLD.metadata
  OR NEW.cost IS NOT OLD.cost
  OR NEW.tokens_input IS NOT OLD.tokens_input
  OR NEW.tokens_output IS NOT OLD.tokens_output
  OR NEW.tokens_reasoning IS NOT OLD.tokens_reasoning
  OR NEW.tokens_cache_read IS NOT OLD.tokens_cache_read
  OR NEW.tokens_cache_write IS NOT OLD.tokens_cache_write
  OR NEW.revert IS NOT OLD.revert
  OR NEW.permission IS NOT OLD.permission
  OR NEW.agent IS NOT OLD.agent
  OR NEW.model IS NOT OLD.model
  OR NEW.time_created IS NOT OLD.time_created
  OR NEW.time_updated IS NOT OLD.time_updated
  OR NEW.time_compacting IS NOT OLD.time_compacting
`

export default {
  id: "20261023000001_family_archive_backfill",
  runsOnFreshSchema: true,
  up(tx) {
    return Effect.gen(function* () {
      // A new child session may not be inserted anywhere under an archived or
      // archiving ancestor. Import may opt the exact new session out until expiry.
      yield* tx.run(`
        CREATE TRIGGER \`session_family_archive_insert\`
        BEFORE INSERT ON \`session\`
        WHEN NEW.parent_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM \`session_import_exempt\` e
            WHERE e.session_id = NEW.id AND e.expires_at > ${now}
          )
        BEGIN
          SELECT CASE WHEN EXISTS (${ancestorWalk("NEW.parent_id")})
            THEN RAISE(ABORT, 'SessionArchivedError: cannot insert a child under an archived session')
          END;
        END;
      `)

      // Archive-field changes require an explicit guard or an active import
      // exemption. Archived rows may otherwise only change archive fields.
      yield* tx.run(`
        CREATE TRIGGER \`session_family_archive_update\`
        BEFORE UPDATE ON \`session\`
        BEGIN
          SELECT CASE WHEN (
              NEW.time_archived IS NOT OLD.time_archived
              OR NEW.family_archive_id IS NOT OLD.family_archive_id
              OR NEW.archive_cause IS NOT OLD.archive_cause
              OR NEW.archive_state IS NOT OLD.archive_state
              OR NEW.archive_state_at IS NOT OLD.archive_state_at
            )
            AND NOT EXISTS (
              SELECT 1 FROM \`session_archive_guard\` g WHERE g.session_id = NEW.id
            )
            AND NOT EXISTS (
              SELECT 1 FROM \`session_import_exempt\` e
              WHERE e.session_id = NEW.id AND e.expires_at > ${now}
            )
            THEN RAISE(ABORT, 'SessionArchivedError: archive field change requires a session_archive_guard row')
          END;

          SELECT CASE WHEN (OLD.time_archived IS NOT NULL OR OLD.archive_state = 'archiving')
            AND NOT EXISTS (
              SELECT 1 FROM \`session_import_exempt\` e
              WHERE e.session_id = NEW.id AND e.expires_at > ${now}
            )
            AND (${nonArchiveColumnsChanged})
            THEN RAISE(ABORT, 'SessionArchivedError: archived session is immutable')
          END;

          -- A single guarded statement cannot archive a live row while also
          -- changing non-archive state.
          SELECT CASE WHEN (NEW.time_archived IS NOT NULL OR NEW.archive_state = 'archiving')
            AND OLD.time_archived IS NULL
            AND COALESCE(OLD.archive_state, '') <> 'archiving' 
            AND NOT EXISTS (
              SELECT 1 FROM \`session_import_exempt\` e
              WHERE e.session_id = NEW.id AND e.expires_at > ${now}
            )
            AND (${nonArchiveColumnsChanged})
            THEN RAISE(ABORT, 'SessionArchivedError: archived session is immutable')
          END;

          SELECT CASE WHEN NEW.parent_id IS NOT OLD.parent_id
            AND NOT EXISTS (
              SELECT 1 FROM \`session_import_exempt\` e
              WHERE e.session_id = NEW.id AND e.expires_at > ${now}
            )
            AND (
              OLD.time_archived IS NOT NULL
              OR OLD.archive_state = 'archiving'
              OR NEW.time_archived IS NOT NULL
              OR NEW.archive_state = 'archiving'
              OR EXISTS (${ancestorWalk("OLD.parent_id")})
              OR EXISTS (${ancestorWalk("NEW.parent_id")})
            )
            THEN RAISE(ABORT, 'SessionArchivedError: cannot move a session into or out of an archived family')
          END;
        END;
      `)

      // Message rows resolve their session directly.
      yield* tx.run(`
        CREATE TRIGGER \`message_family_archive_insert\`
        BEFORE INSERT ON \`message\`
        WHEN EXISTS (
          SELECT 1 FROM \`session\` s
          WHERE s.id = NEW.session_id
            AND (s.time_archived IS NOT NULL OR s.archive_state = 'archiving')
            AND NOT EXISTS (
              SELECT 1 FROM \`session_import_exempt\` e
              WHERE e.session_id = s.id AND e.expires_at > ${now}
            )
        )
        BEGIN
          SELECT RAISE(ABORT, 'SessionArchivedError: cannot write messages to an archived session');
        END;
      `)
      yield* tx.run(`
        CREATE TRIGGER \`message_family_archive_update\`
        BEFORE UPDATE ON \`message\`
        WHEN EXISTS (
            SELECT 1 FROM \`session\` s
            WHERE s.id = NEW.session_id
              AND (s.time_archived IS NOT NULL OR s.archive_state = 'archiving')
              AND NOT EXISTS (
                SELECT 1 FROM \`session_import_exempt\` e
                WHERE e.session_id = s.id AND e.expires_at > ${now}
              )
          )
          OR EXISTS (
            SELECT 1 FROM \`session\` s
            WHERE s.id = OLD.session_id
              AND (s.time_archived IS NOT NULL OR s.archive_state = 'archiving')
              AND NOT EXISTS (
                SELECT 1 FROM \`session_import_exempt\` e
                WHERE e.session_id = s.id AND e.expires_at > ${now}
              )
          )
        BEGIN
          SELECT RAISE(ABORT, 'SessionArchivedError: cannot write messages to an archived session');
        END;
      `)

      // Parts resolve their session through the message row, never through
      // NEW.session_id. A missing message is not a writable target.
      const writablePartMessage = (message: string) => `
        SELECT 1 FROM \`message\` m
        JOIN \`session\` s ON s.id = m.session_id
        WHERE m.id = ${message}
          AND (
            (s.time_archived IS NULL AND COALESCE(s.archive_state, '') <> 'archiving')
            OR EXISTS (
              SELECT 1 FROM \`session_import_exempt\` e
              WHERE e.session_id = s.id AND e.expires_at > ${now}
            )
          )
      `
      yield* tx.run(`
        CREATE TRIGGER \`part_family_archive_insert\`
        BEFORE INSERT ON \`part\`
        WHEN NOT EXISTS (${writablePartMessage("NEW.message_id")})
        BEGIN
          SELECT RAISE(ABORT, 'SessionArchivedError: cannot write parts to an archived session');
        END;
      `)
      yield* tx.run(`
        CREATE TRIGGER \`part_family_archive_update\`
        BEFORE UPDATE ON \`part\`
        WHEN NOT EXISTS (${writablePartMessage("OLD.message_id")})
          OR NOT EXISTS (${writablePartMessage("NEW.message_id")})
        BEGIN
          SELECT RAISE(ABORT, 'SessionArchivedError: cannot write parts to an archived session');
        END;
      `)

      // Session-scoped state rows are immutable once the session is archived.
      for (const table of sessionScopedTables) {
        yield* tx.run(`
          CREATE TRIGGER \`${table}_family_archive_insert\`
          BEFORE INSERT ON \`${table}\`
          WHEN EXISTS (
            SELECT 1 FROM \`session\` s
            WHERE s.id = NEW.session_id
              AND (s.time_archived IS NOT NULL OR s.archive_state = 'archiving')
              AND NOT EXISTS (
                SELECT 1 FROM \`session_import_exempt\` e
                WHERE e.session_id = s.id AND e.expires_at > ${now}
              )
          )
          BEGIN
            SELECT RAISE(ABORT, 'SessionArchivedError: cannot write to an archived session');
          END;
        `)
        yield* tx.run(`
          CREATE TRIGGER \`${table}_family_archive_update\`
          BEFORE UPDATE ON \`${table}\`
          WHEN EXISTS (
              SELECT 1 FROM \`session\` s
              WHERE s.id = NEW.session_id
                AND (s.time_archived IS NOT NULL OR s.archive_state = 'archiving')
                AND NOT EXISTS (
                  SELECT 1 FROM \`session_import_exempt\` e
                  WHERE e.session_id = s.id AND e.expires_at > ${now}
                )
            )
            OR EXISTS (
              SELECT 1 FROM \`session\` s
              WHERE s.id = OLD.session_id
                AND (s.time_archived IS NOT NULL OR s.archive_state = 'archiving')
                AND NOT EXISTS (
                  SELECT 1 FROM \`session_import_exempt\` e
                  WHERE e.session_id = s.id AND e.expires_at > ${now}
                )
            )
          BEGIN
            SELECT RAISE(ABORT, 'SessionArchivedError: cannot write to an archived session');
          END;
        `)
      }
    })
  },
} satisfies DatabaseMigration.Migration
