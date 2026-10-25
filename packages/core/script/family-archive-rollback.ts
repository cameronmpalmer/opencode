#!/usr/bin/env bun

import { Database as SQLiteDatabase } from "bun:sqlite"
import { Database } from "../src/database/database"

/**
 * Undo the deterministic family archive backfill and reconcile leftovers.
 *
 * The script is deliberately conservative: it only touches rows that still
 * match the migration snapshot (same family key, same archive time, and
 * `archive_cause = 'migration'`). Anything the user re-archived after the
 * snapshot is left alone, and its snapshot row is kept.
 *
 * Rollout operator steps (manual, no automation here):
 *   1. Pause the nightly purge.
 *   2. Run family-archive-dry-run.ts against the production database and
 *      record the output.
 *   3. Apply the schema and backfill migrations at server start.
 *   4. Enable `familyArchive` in staging, then production.
 *   5. Call `archiveSubtree` for every archived root so live legacy leases are
 *      interrupted. A failed lease fails closed with the blocking session IDs.
 *   6. Run `family-archive-rollback.ts --reconcile`, then verify one family end
 *      to end.
 *   7. Deploy the pipeline only after A and B are live.
 *   8. Re-enable the nightly purge after the migration and the first pipeline
 *      run.
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

function deleteGuards(db: SQLiteDatabase, ids: string[]) {
  const remove = db.prepare("DELETE FROM session_archive_guard WHERE session_id = ?")
  for (const id of ids) remove.run(id)
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

    // Defensive: never delete a snapshot while any row still matches its
    // migration predicate. The match query above is exhaustive, so this should
    // be empty; keeping the snapshot preserves recovery if a row was missed.
    const residual = db.prepare(`
      SELECT 1
      FROM session s
      JOIN family_archive_migration m
        ON s.family_archive_id = m.root_session_id
       AND s.time_archived = m.root_archived_at
       AND s.archive_cause = 'migration'
      WHERE m.root_session_id = ?
      LIMIT 1
    `)
    const deleteSnapshot = db.prepare("DELETE FROM family_archive_migration WHERE root_session_id = ?")
    for (const root of clearedRoots) {
      if (residual.get(root)) {
        if (!skippedSnapshots.includes(root)) skippedSnapshots.push(root)
        continue
      }
      deleteSnapshot.run(root)
    }

    deleteGuards(
      db,
      matches.map((row) => row.id),
    )

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
  type ReconcileRow = {
    id: string
    parent_id: string | null
    time_archived: number | null
    family_archive_id: string | null
    archive_cause: string | null
  }

  return insideImmediate(db, () => {
    const sessions = db
      .query<ReconcileRow, []>(
        `
        SELECT id, parent_id, time_archived, family_archive_id, archive_cause
        FROM session
        ORDER BY id
      `,
      )
      .all()
    const byID = new Map(sessions.map((row) => [row.id, row]))
    const targets = sessions
      .filter((row) => row.time_archived !== null && row.family_archive_id === null && row.archive_cause === null)
      .map((row) => row.id)

    if (targets.length === 0) return { updated: [] }

    const nearestArchivedAncestor = (id: string) => {
      const seen = new Set<string>([id])
      let parent = byID.get(id)?.parent_id ?? null
      while (parent && !seen.has(parent)) {
        const row = byID.get(parent)
        if (!row) return undefined
        if (row.time_archived !== null) return row
        seen.add(parent)
        parent = row.parent_id
      }
      return undefined
    }

    const keys = new Map<string, string>()
    const effectiveKey = (id: string, visiting = new Set<string>()): string => {
      const cached = keys.get(id)
      if (cached !== undefined) return cached
      const row = byID.get(id)
      if (!row) return id
      if (visiting.has(id)) return id
      visiting.add(id)

      let key = id
      if (row.family_archive_id !== null) {
        // An existing family root wins. A missing root on an archived row
        // makes that row its own family.
        key = byID.has(row.family_archive_id) ? row.family_archive_id : id
      } else if (row.archive_cause === "user") {
        key = id
      } else {
        // Legacy null/null rows fall back to the nearest archived ancestor
        // above them, recursively, and to themselves when there is none.
        const ancestor = nearestArchivedAncestor(id)
        key = ancestor ? effectiveKey(ancestor.id, visiting) : id
      }

      visiting.delete(id)
      keys.set(id, key)
      return key
    }

    const resolved = targets.map((id) => {
      const ancestor = nearestArchivedAncestor(id)
      if (!ancestor) return { id, family: id, cause: "user" as const }
      return { id, family: effectiveKey(ancestor.id), cause: "migration" as const }
    })

    insertGuards(
      db,
      resolved.map((row) => row.id),
    )
    const update = db.prepare("UPDATE session SET family_archive_id = ?, archive_cause = ? WHERE id = ?")
    for (const row of resolved) update.run(row.family, row.cause, row.id)
    deleteGuards(
      db,
      resolved.map((row) => row.id),
    )

    return { updated: targets }
  })
}

function parseDBPath(argv: string[]) {
  const index = argv.indexOf("--db")
  if (index >= 0) {
    const value = argv[index + 1]
    if (!value || value.startsWith("--")) throw new Error("--db requires a database path")
    return value
  }
  return Database.path()
}

if (import.meta.main) {
  const argv = process.argv.slice(2)
  const path = parseDBPath(argv)
  const db = new SQLiteDatabase(path, { readwrite: true, create: false })
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
