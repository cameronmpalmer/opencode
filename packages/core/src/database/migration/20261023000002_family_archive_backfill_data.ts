import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

// Deterministic data backfill (design 7.1). It lives in its own migration so
// databases that already journaled the trigger migration still run it. Every
// archived row without a family becomes its own root, then every unarchived
// descendant inherits the nearest archived ancestor's family and archive time.
// Raw SQL keeps time_updated untouched. Guards authorize only the archive-field
// writes.
const now = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`

// The nearest archived ancestor for every session. The path guard stops a
// parent cycle; ordering by depth keeps the result independent of row order.
const archivedAncestors = `
  WITH RECURSIVE ancestors(
    session_id, ancestor_id, time_archived, family_archive_id, parent_id, depth, path
  ) AS (
    SELECT
      s.id, p.id, p.time_archived, p.family_archive_id, p.parent_id,
      1, ',' || p.id || ','
    FROM session s
    JOIN session p ON p.id = s.parent_id
    UNION ALL
    SELECT
      a.session_id, p.id, p.time_archived, p.family_archive_id, p.parent_id,
      a.depth + 1, a.path || p.id || ','
    FROM ancestors a
    JOIN session p ON p.id = a.parent_id
    WHERE a.parent_id IS NOT NULL
      AND instr(a.path, ',' || p.id || ',') = 0
  ),
  nearest AS (
    SELECT
      session_id, ancestor_id, time_archived, family_archive_id, depth,
      ROW_NUMBER() OVER (
        PARTITION BY session_id
        ORDER BY depth, ancestor_id
      ) AS rank
    FROM ancestors
    WHERE time_archived IS NOT NULL
  )
`

const createBackfillIDs = `
  CREATE TEMP TABLE family_archive_backfill_ids (session_id text PRIMARY KEY)
`

const collectBackfillIDs = `
  ${archivedAncestors}
  INSERT OR IGNORE INTO family_archive_backfill_ids (session_id)
  SELECT s.id
  FROM session s
  WHERE (s.time_archived IS NOT NULL AND s.family_archive_id IS NULL)
     OR (
       s.time_archived IS NULL
       AND EXISTS (
         SELECT 1 FROM nearest n
         WHERE n.session_id = s.id AND n.rank = 1
       )
     )
`

const insertBackfillGuards = `
  INSERT OR IGNORE INTO session_archive_guard (session_id)
  SELECT session_id FROM family_archive_backfill_ids
`

const clearBackfillGuards = `
  DELETE FROM session_archive_guard
  WHERE session_id IN (SELECT session_id FROM family_archive_backfill_ids)
`

const dropBackfillIDs = `DROP TABLE temp.family_archive_backfill_ids`

const backfillRoots = `
  UPDATE session
  SET family_archive_id = id, archive_cause = 'migration'
  WHERE time_archived IS NOT NULL AND family_archive_id IS NULL
`

const backfillDescendants = `
  ${archivedAncestors}
  UPDATE session AS s
  SET
    time_archived = (
      SELECT n.time_archived FROM nearest n
      WHERE n.session_id = s.id AND n.rank = 1
    ),
    family_archive_id = (
      SELECT COALESCE(n.family_archive_id, n.ancestor_id) FROM nearest n
      WHERE n.session_id = s.id AND n.rank = 1
    ),
    archive_cause = 'migration'
  WHERE s.time_archived IS NULL
    AND EXISTS (
      SELECT 1 FROM nearest n
      WHERE n.session_id = s.id AND n.rank = 1
    )
`

export default {
  id: "20261023000002_family_archive_backfill_data",
  runsOnFreshSchema: true,
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`DROP TABLE IF EXISTS temp.family_archive_backfill_ids`)
      yield* tx.run(createBackfillIDs)
      yield* tx.run(collectBackfillIDs)
      yield* tx.run(insertBackfillGuards)
      yield* tx.run(`
        INSERT OR IGNORE INTO family_archive_migration (root_session_id, root_archived_at, applied_at)
        SELECT id, time_archived, ${now}
        FROM session
        WHERE time_archived IS NOT NULL AND family_archive_id IS NULL
      `)
      yield* tx.run(backfillRoots)
      yield* tx.run(backfillDescendants)
      yield* tx.run(clearBackfillGuards)
      yield* tx.run(dropBackfillIDs)
    })
  },
} satisfies DatabaseMigration.Migration
