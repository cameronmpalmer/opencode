export * as FamilyArchive from "./family-archive.js"

import { and, gte, inArray, sql } from "drizzle-orm"
import { Effect } from "effect"
import { Database } from "../database/database.js"
import { SessionSchema } from "./schema.js"
import { SessionRunLeaseTable, SessionTable } from "./sql.js"

export const LIVE_LEASE_MS = 60_000

type Db = Database.Interface["db"]
export type SessionRow = typeof SessionTable.$inferSelect

export { ArchivePreview, ArchiveResult, UnarchiveResult } from "@opencode/schema/session"

/**
 * The SQL definition of the effective family key (design 6.1), shared by every
 * query that groups or gates sessions by family. Do not re-derive this rule in
 * a caller and do not replace it with `coalesce(family_archive_id, id)`.
 *
 * Yields exactly one row per session with `session_id` and `family_key`:
 *
 * - a non-null `family_archive_id` whose root row exists wins;
 * - an archived row whose family root is missing is its own family;
 * - a `user`-cause row with no existing family root is its own family;
 * - otherwise the row inherits its nearest archived ancestor's effective key
 *   (recursively, through legacy null/null ancestors);
 * - otherwise it falls back to its own ID.
 *
 * The ancestor walk keeps a path guard so a parent cycle terminates instead of
 * recursing forever.
 */
export const effectiveFamilyKeySql = sql`
  WITH RECURSIVE
    session_ancestor(id, ancestor_id, depth, path) AS (
      SELECT id, parent_id, 1, ',' || parent_id || ','
      FROM session_v2
      WHERE parent_id IS NOT NULL
      UNION ALL
      SELECT session_ancestor.id,
             session_v2.parent_id,
             session_ancestor.depth + 1,
             session_ancestor.path || session_v2.parent_id || ','
      FROM session_ancestor
      JOIN session_v2 ON session_v2.id = session_ancestor.ancestor_id
      WHERE session_v2.parent_id IS NOT NULL
        AND instr(session_ancestor.path, ',' || session_v2.parent_id || ',') = 0
    ),
    family_candidate(id, family_key, priority) AS (
      SELECT session_v2.id,
             CASE
               WHEN family_root.id IS NOT NULL THEN session_v2.family_archive_id
               WHEN session_v2.family_archive_id IS NOT NULL AND session_v2.time_archived IS NOT NULL THEN session_v2.id
               WHEN session_v2.archive_cause = 'user' THEN session_v2.id
             END,
             0
      FROM session_v2
      LEFT JOIN session_v2 AS family_root ON family_root.id = session_v2.family_archive_id
      UNION ALL
      SELECT session_ancestor.id,
             CASE
               WHEN family_root.id IS NOT NULL THEN session_v2.family_archive_id
               WHEN session_v2.family_archive_id IS NOT NULL AND session_v2.time_archived IS NOT NULL THEN session_v2.id
               WHEN session_v2.archive_cause = 'user' THEN session_v2.id
             END,
             session_ancestor.depth
      FROM session_ancestor
      JOIN session_v2 ON session_v2.id = session_ancestor.ancestor_id
      LEFT JOIN session_v2 AS family_root ON family_root.id = session_v2.family_archive_id
      WHERE session_v2.time_archived IS NOT NULL
    ),
    resolved_candidate AS (
      SELECT id, family_key
      FROM (
        SELECT id, family_key,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY priority) AS rank
        FROM family_candidate
        WHERE family_key IS NOT NULL
      )
      WHERE rank = 1
    ),
    fallback_ancestor AS (
      SELECT id, ancestor_id
      FROM (
        SELECT session_ancestor.id, session_ancestor.ancestor_id,
               ROW_NUMBER() OVER (PARTITION BY session_ancestor.id
                                  -- The outermost archived ancestor is the terminal key for a legacy chain with no base.
                                  ORDER BY session_ancestor.depth DESC) AS rank
        FROM session_ancestor
        JOIN session_v2 ON session_v2.id = session_ancestor.ancestor_id
        WHERE session_v2.time_archived IS NOT NULL
      )
      WHERE rank = 1
    )
  SELECT session_v2.id AS session_id,
         COALESCE(resolved_candidate.family_key, fallback_ancestor.ancestor_id, session_v2.id) AS family_key
  FROM session_v2
  LEFT JOIN resolved_candidate ON resolved_candidate.id = session_v2.id
  LEFT JOIN fallback_ancestor ON fallback_ancestor.id = session_v2.id
`

export function subtreeIDs(db: Db, rootID: SessionSchema.ID) {
  return db
    .all<{ id: string }>(
      sql`
        WITH RECURSIVE subtree(id, path) AS (
          SELECT id, ',' || id || ',' FROM session_v2 WHERE id = ${rootID}
          UNION ALL
          SELECT session_v2.id, subtree.path || session_v2.id || ','
          FROM session_v2 JOIN subtree ON session_v2.parent_id = subtree.id
          WHERE instr(subtree.path, ',' || session_v2.id || ',') = 0
        )
        SELECT id FROM subtree
      `,
    )
    .pipe(Effect.orDie)
}

export function loadSubtree(db: Db, rootID: SessionSchema.ID) {
  return Effect.gen(function* () {
    const ids = yield* subtreeIDs(db, rootID)
    if (ids.length === 0) return [] as SessionRow[]
    return yield* db
      .select()
      .from(SessionTable)
      .where(
        inArray(
          SessionTable.id,
          ids.map((row) => row.id as SessionSchema.ID),
        ),
      )
      .all()
      .pipe(Effect.orDie)
  })
}

export function orderSubtree(rows: SessionRow[]) {
  const byID = new Map(rows.map((row) => [row.id, row]))
  const depth = (row: SessionRow) => {
    let value = 0
    let parent = row.parent_id
    const seen = new Set<string>([row.id])
    while (parent && byID.has(parent) && !seen.has(parent)) {
      seen.add(parent)
      value += 1
      parent = byID.get(parent)!.parent_id
    }
    return value
  }
  return [...rows].sort((a, b) => depth(a) - depth(b) || a.id.localeCompare(b.id))
}

export function liveLeases(db: Db, ids: SessionSchema.ID[]) {
  if (ids.length === 0) return Effect.succeed([] as (typeof SessionRunLeaseTable.$inferSelect)[])
  return db
    .select()
    .from(SessionRunLeaseTable)
    .where(
      and(
        inArray(SessionRunLeaseTable.session_id, ids),
        gte(SessionRunLeaseTable.heartbeat_at, Date.now() - LIVE_LEASE_MS),
      ),
    )
    .all()
    .pipe(Effect.orDie)
}

/**
 * Return the nearest archived or archiving ancestor of `sessionID`, including
 * the session itself. Used to block creating a child under an archived family.
 */
export const archivedAncestor = (db: Db, sessionID: SessionSchema.ID) =>
  db
    .all<{ id: SessionSchema.ID }>(sql`
      WITH RECURSIVE ancestors(id, parent_id, time_archived, archive_state) AS (
        SELECT s.id, s.parent_id, s.time_archived, s.archive_state
        FROM session_v2 s
        WHERE s.id = ${sessionID}
        UNION
        SELECT p.id, p.parent_id, p.time_archived, p.archive_state
        FROM session_v2 p
        JOIN ancestors a ON p.id = a.parent_id
      )
      SELECT id FROM ancestors
      WHERE time_archived IS NOT NULL OR archive_state = 'archiving'
      LIMIT 1
    `)
    .pipe(Effect.orDie)

export function isDirectFamilyMember(row: SessionRow) {
  return (
    row.time_archived !== null &&
    row.archive_cause !== null &&
    row.archive_cause !== "user" &&
    row.family_archive_id !== null &&
    row.family_archive_id !== row.id
  )
}

/**
 * V1 performed the guard insert, event publication, and guard delete in one
 * transaction. V2 publishes each event through the bus in its own transaction,
 * so a crash can leave a guard row and an "archiving" marker behind. No archive
 * operation can be in progress at startup, so sweep both before the guard
 * trigger lands.
 */
export function reconcileStaleArchiveState(db: Db): Effect.Effect<void, unknown> {
  return db.transaction((tx) =>
    Effect.gen(function* () {
      yield* tx.run(sql`DELETE FROM session_archive_guard`)
      yield* tx.run(
        sql`UPDATE session_v2 SET archive_state = NULL, archive_state_at = NULL WHERE archive_state = 'archiving'`,
      )
    }),
  )
}
