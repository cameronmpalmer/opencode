export * as SessionSchema from "./schema"

import { Session } from "@opencode-ai/schema/session"

export const ID = Session.ID
export type ID = typeof ID.Type

export const Info = Session.Info
export type Info = Session.Info

/**
 * The effective family key for a session row (design 6.1).
 *
 * A row belongs to the family named by `family_archive_id` when that root
 * still exists, is its own standalone family when it was archived with cause
 * `user`, otherwise inherits its nearest archived ancestor's key, and finally
 * falls back to its own ID. The SQL definition in `sql.ts`
 * (`effectiveFamilyKeySql`) must stay in sync with this function.
 */
export function effectiveFamilyKey(
  row: { id: string; family_archive_id?: string | null; archive_cause?: string | null },
  options?: {
    nearestArchivedAncestor?: { id: string; time_archived?: number | null; parent_id?: string | null } | null
    familyExists?: (id: string) => boolean
  },
): string {
  if (row.family_archive_id && (options?.familyExists?.(row.family_archive_id) ?? true)) return row.family_archive_id
  if (row.archive_cause === "user") return row.id
  if (options?.nearestArchivedAncestor?.time_archived) return options.nearestArchivedAncestor.id
  return row.id
}
