#!/usr/bin/env bun

import { Database as SQLiteDatabase } from "bun:sqlite"
import { Database } from "../src/database/database"

/**
 * Undo the deterministic family archive backfill.
 *
 * The script is deliberately conservative: it only touches rows that still
 * match the migration snapshot (same family key, same archive time, and
 * `archive_cause = 'migration'`). Anything the user re-archived after the
 * snapshot is left alone, and its snapshot row is kept.
 */

export type RollbackReport = {
  roots: number
  descendants: number
  cleared: number
  skipped_snapshots: string[]
  skipped_rows: string[]
}

export type ReconcileReport = {
  updated: string[]
}

const descendantsWithArchivedAncestors = `
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

const beginImmediate = (db: SQLiteDatabase) => db.run("BEGIN IMMEDIATE")
const commit = (db: SQLiteDatabase) => db.run("COMMIT")
const rollbackTransaction = (db: SQLiteDatabase) => db.run("ROLLBACK")

function insideImmediate<T>(db: SQLiteDatabase, fn: () => T): T {
  beginImmediate(db)
  try {
    const result = fn()
    commit(db)
    return result
  } catch (error) {
    rollbackTransaction(db)
    throw error
  }
}

function insertGuards(db: SQLiteDatabase, ids: string[]) {
  const insert = db.prepare("INSERT OR IGNORE INTO session_archive_guard (session_id) VALUES (?)")
  for (const id of ids) insert.run(id)
}

export function rollback(db: SQLiteDatabase): RollbackReport {
  return insideImmediate(db, () => {
    const archiving = db
      .query<{ id: string }, []>("SELECT id FROM session WHERE archive_state = 'archiving' ORDER BY id")
      .all()
      .map((row) => row.id)
    if (archiving.length > 0) {
      throw new Error(`refusing rollback while sessions are archiving: ${archiving.join(", ")}`)
    }

    const snapshots = db
      .query<
        { root_session_id: string; root_archived_at: number },
        []
      >("SELECT root_session_id, root_archived_at FROM family_archive_migration ORDER BY root_session_id")
      .all()

    const matches = db
      .query<{ id: string; is_root: number }, []>(
        `
        SELECT s.id, s.id = m.root_session_id AS is_root
        FROM session s
        JOIN family_archive_migration m
          ON s.family_archive_id = m.root_session_id
         AND s.time_archived = m.root_archived_at
         AND s.archive_cause = 'migration'
        ORDER BY s.id
      `,
      )
      .all()

    const matchedIDs = new Set(matches.map((row) => row.id))
    const skippedRows = db
      .query<{ id: string }, []>(
        `
        WITH RECURSIVE subtree(root_session_id, id, path) AS (
          SELECT root_session_id, root_session_id, ',' || root_session_id || ','
          FROM family_archive_migration
          UNION ALL
          SELECT subtree.root_session_id, session.id, subtree.path || session.id || ','
          FROM subtree
          JOIN session ON session.parent_id = subtree.id
          WHERE instr(subtree.path, ',' || session.id || ',') = 0
        )
        SELECT DISTINCT s.id
        FROM session s
        JOIN subtree ON subtree.id = s.id
        ORDER BY s.id
      `,
      )
      .all()
      .map((row) => row.id)
      .filter((id) => !matchedIDs.has(id))

    const clearedRoots = new Set(matches.filter((row) => row.is_root === 1).map((row) => row.id))
    const skippedSnapshots = snapshots
      .map((snapshot) => snapshot.root_session_id)
      .filter((root) => !clearedRoots.has(root))

    insertGuards(
      db,
      matches.map((row) => row.id),
    )

    const clearDescendant = db.prepare(`
      UPDATE session
      SET time_archived = NULL,
          family_archive_id = NULL,
          archive_cause = NULL,
          archive_state = NULL,
          archive_state_at = NULL
      WHERE id = ?
    `)
    const clearRoot = db.prepare(`
      UPDATE session
      SET family_archive_id = NULL,
          archive_cause = NULL,
          archive_state = NULL,
          archive_state_at = NULL
      WHERE id = ?
    `)

    let descendants = 0
    for (const row of matches) {
      if (row.is_root === 1) {
        clearRoot.run(row.id)
      } else {
        descendants += 1
        clearDescendant.run(row.id)
      }
    }

    const deleteSnapshot = db.prepare("DELETE FROM family_archive_migration WHERE root_session_id = ?")
    for (const root of clearedRoots) deleteSnapshot.run(root)

    db.run("DELETE FROM session_archive_guard")

    return {
      roots: clearedRoots.size,
      descendants,
      cleared: matches.length,
      skipped_snapshots: skippedSnapshots,
      skipped_rows: skippedRows,
    }
  })
}

/**
 * Stamp archived rows whose pre-trigger writer left both family and cause
 * null. A row with an archived ancestor joins that ancestor's effective
 * family; a standalone row becomes its own user-owned family. This is a
 * separate guarded update because `archiveSubtree` short-circuits a fully
 * archived, no-lease family before it would stamp anything.
 */
export function reconcile(db: SQLiteDatabase): ReconcileReport {
  return insideImmediate(db, () => {
    const targets = db
      .query<{ id: string }, []>(
        `
        SELECT id FROM session
        WHERE time_archived IS NOT NULL
          AND family_archive_id IS NULL
          AND archive_cause IS NULL
        ORDER BY id
      `,
      )
      .all()
      .map((row) => row.id)

    if (targets.length === 0) return { updated: [] }

    insertGuards(db, targets)

    db.run(`
      ${descendantsWithArchivedAncestors},
      resolved AS (
        SELECT
          t.id,
          COALESCE(
            (
              SELECT a.family_archive_id
              FROM ancestors a
              WHERE a.session_id = t.id
                AND a.time_archived IS NOT NULL
                AND a.family_archive_id IS NOT NULL
              ORDER BY a.depth
              LIMIT 1
            ),
            (
              SELECT a.ancestor_id
              FROM ancestors a
              WHERE a.session_id = t.id AND a.time_archived IS NOT NULL
              ORDER BY a.depth DESC
              LIMIT 1
            )
          ) AS family_key,
          EXISTS (
            SELECT 1 FROM ancestors a
            WHERE a.session_id = t.id AND a.time_archived IS NOT NULL
          ) AS has_ancestor
        FROM session t
        WHERE t.time_archived IS NOT NULL
          AND t.family_archive_id IS NULL
          AND t.archive_cause IS NULL
      )
      UPDATE session AS s
      SET
        family_archive_id = COALESCE(
          (SELECT r.family_key FROM resolved r WHERE r.id = s.id),
          s.id
        ),
        archive_cause = CASE
          WHEN (SELECT r.has_ancestor FROM resolved r WHERE r.id = s.id) = 1 THEN 'migration'
          ELSE 'user'
        END
      WHERE s.time_archived IS NOT NULL
        AND s.family_archive_id IS NULL
        AND s.archive_cause IS NULL
    `)

    db.run("DELETE FROM session_archive_guard")
    return { updated: targets }
  })
}

function parseDBPath(argv: string[]) {
  const index = argv.indexOf("--db")
  if (index >= 0) return argv[index + 1]
  return Database.path()
}

if (import.meta.main) {
  const argv = process.argv.slice(2)
  const path = parseDBPath(argv)
  const db = new SQLiteDatabase(path, { readwrite: true })
  try {
    if (argv.includes("--reconcile")) {
      const report = reconcile(db)
      console.log(`reconciled ${report.updated.length} archived session(s)`)
      for (const id of report.updated) console.log(`  ${id}`)
    } else {
      const report = rollback(db)
      console.log(
        `rolled back ${report.roots} root(s) and ${report.descendants} descendant(s) (${report.cleared} row(s))`,
      )
      for (const id of report.skipped_snapshots) console.log(`  kept snapshot ${id}`)
      for (const id of report.skipped_rows) console.log(`  skipped ${id}`)
    }
  } finally {
    db.close()
  }
}
