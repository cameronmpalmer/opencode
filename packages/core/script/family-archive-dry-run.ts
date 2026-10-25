#!/usr/bin/env bun

import { Database as SQLiteDatabase } from "bun:sqlite"
import { Database } from "../src/database/database"

/**
 * Read-only preview of the family archive backfill. Opens the database with
 * SQLite's `readonly` flag and never writes.
 */

export type DryRunRoot = {
  id: string
  time_archived: number
  unarchived_descendants: number
  post_archive_activity: number
}

export type DryRunDescendant = {
  id: string
  root_id: string
  root_archived_at: number
  depth: number
  time_updated: number
  post_archive_activity: boolean
}

export type DryRunReport = {
  path: string
  roots: DryRunRoot[]
  unarchived_descendants: DryRunDescendant[]
  post_archive_activity: DryRunDescendant[]
  counts: {
    roots: number
    unarchived_descendants: number
    post_archive_activity: number
  }
}

const descendantsWithArchivedAncestors = (legacy: boolean) => {
  const family = legacy ? "" : ", family_archive_id"
  const familySelect = legacy ? "" : ", p.family_archive_id"
  return `
  WITH RECURSIVE ancestors(
    session_id, ancestor_id, time_archived${family}, parent_id, depth, path
  ) AS (
    SELECT
      s.id, p.id, p.time_archived${familySelect}, p.parent_id,
      1, ',' || p.id || ','
    FROM session s
    JOIN session p ON p.id = s.parent_id
    UNION ALL
    SELECT
      a.session_id, p.id, p.time_archived${familySelect}, p.parent_id,
      a.depth + 1, a.path || p.id || ','
    FROM ancestors a
    JOIN session p ON p.id = a.parent_id
    WHERE a.parent_id IS NOT NULL
      AND instr(a.path, ',' || p.id || ',') = 0
  ),
  nearest AS (
    SELECT
      session_id, ancestor_id, time_archived${family}, depth,
      ROW_NUMBER() OVER (
        PARTITION BY session_id
        ORDER BY depth, ancestor_id
      ) AS rank
    FROM ancestors
    WHERE time_archived IS NOT NULL
  )
`
}

const sessionHasFamilyArchiveID = (db: SQLiteDatabase) =>
  db
    .query<{ name: string }, []>("PRAGMA table_info(session)")
    .all()
    .some((column) => column.name === "family_archive_id")

export function collectDryRun(db: SQLiteDatabase, path: string): DryRunReport {
  const hasFamily = sessionHasFamilyArchiveID(db)
  const roots = db
    .query<{ id: string; time_archived: number }, []>(
      hasFamily
        ? `
          SELECT id, time_archived
          FROM session
          WHERE time_archived IS NOT NULL
            AND (family_archive_id IS NULL OR family_archive_id = id)
          ORDER BY time_archived, id
        `
        : `
          SELECT id, time_archived
          FROM session
          WHERE time_archived IS NOT NULL
          ORDER BY time_archived, id
        `,
    )
    .all()

  const descendants = db
    .query<
      {
        id: string
        root_id: string
        root_archived_at: number
        depth: number
        time_updated: number
      },
      []
    >(
      `
      ${descendantsWithArchivedAncestors(hasFamily === false)}
      SELECT
        s.id,
        n.ancestor_id AS root_id,
        n.time_archived AS root_archived_at,
        n.depth,
        s.time_updated
      FROM session s
      JOIN nearest n ON n.session_id = s.id AND n.rank = 1
      WHERE s.time_archived IS NULL
      ORDER BY n.ancestor_id, n.depth, s.id
    `,
    )
    .all()
    .map((row) => ({
      ...row,
      post_archive_activity: row.time_updated > row.root_archived_at,
    }))

  const activity = descendants.filter((row) => row.post_archive_activity)
  const byRoot = new Map<string, { descendants: number; activity: number }>()
  for (const row of descendants) {
    const entry = byRoot.get(row.root_id) ?? { descendants: 0, activity: 0 }
    entry.descendants += 1
    if (row.post_archive_activity) entry.activity += 1
    byRoot.set(row.root_id, entry)
  }

  return {
    path,
    roots: roots.map((root) => {
      const counts = byRoot.get(root.id) ?? { descendants: 0, activity: 0 }
      return {
        ...root,
        unarchived_descendants: counts.descendants,
        post_archive_activity: counts.activity,
      }
    }),
    unarchived_descendants: descendants,
    post_archive_activity: activity,
    counts: {
      roots: roots.length,
      unarchived_descendants: descendants.length,
      post_archive_activity: activity.length,
    },
  }
}

export function dryRun(path: string): DryRunReport {
  const db = new SQLiteDatabase(path, { readonly: true, create: false })
  try {
    return collectDryRun(db, path)
  } finally {
    db.close()
  }
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
  const path = parseDBPath(process.argv.slice(2))
  const report = dryRun(path)
  console.log(`database: ${report.path}`)
  console.log(`archived roots: ${report.counts.roots}`)
  for (const root of report.roots) {
    console.log(
      `  ${root.id} archived=${root.time_archived} unarchived_descendants=${root.unarchived_descendants} post_archive_activity=${root.post_archive_activity}`,
    )
  }
  console.log(`unarchived descendants: ${report.counts.unarchived_descendants}`)
  for (const row of report.unarchived_descendants) {
    console.log(
      `  ${row.id} root=${row.root_id} root_archived_at=${row.root_archived_at} depth=${row.depth} time_updated=${row.time_updated}${row.post_archive_activity ? " post_archive_activity" : ""}`,
    )
  }
  console.log(`post-archive activity: ${report.counts.post_archive_activity}`)
  for (const row of report.post_archive_activity) {
    console.log(`  ${row.id} root=${row.root_id} time_updated=${row.time_updated} > ${row.root_archived_at}`)
  }
}
