export * as SessionSchema from "./schema"

import { Session } from "@opencode-ai/schema/session"

export const ID = Session.ID
export type ID = typeof ID.Type

export const Info = Session.Info
export type Info = Session.Info

/**
 * The effective family key for a session row (design 6.1).
 *
 * - A non-null `family_archive_id` whose root still exists wins.
 * - An archived row whose family root is missing is its own family.
 * - A `user`-cause row with no existing family root is its own family.
 * - Otherwise the row inherits its nearest archived ancestor's resolved key,
 *   and falls back to its own ID when it has none.
 *
 * `nearestArchivedAncestorKey` carries that ancestor's already-resolved key,
 * so a legacy chain resolves to the same key as the SQL definition in
 * `sql.ts` (`effectiveFamilyKeySql`). Keep the two in sync.
 */
export function effectiveFamilyKey(
  row: {
    id: string
    family_archive_id?: string | null
    archive_cause?: "user" | "cascade" | "migration" | null
    time_archived?: number | null
  },
  options: {
    familyExists: (id: string) => boolean
    /** The nearest archived ancestor's resolved key. Presence means it is archived. */
    nearestArchivedAncestorKey?: string | null
  },
): string {
  if (row.family_archive_id != null) {
    if (options.familyExists(row.family_archive_id)) return row.family_archive_id
    if (row.time_archived != null) return row.id
  }
  if (row.archive_cause === "user") return row.id
  if (options.nearestArchivedAncestorKey != null) return options.nearestArchivedAncestorKey
  return row.id
}
