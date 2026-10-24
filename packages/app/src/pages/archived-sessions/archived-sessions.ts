import type { FamilyArchiveFields } from "@/utils/session"

export const ARCHIVED_SESSION_PAGE_LIMIT = 100

export type ArchivedSessionRow = FamilyArchiveFields & {
  id: string
  parentID?: string | null
  directory?: string
  title?: string
  time: { updated: number; archived?: number | null }
}

export type ArchivedSessionPage = {
  data: readonly ArchivedSessionRow[]
}

export type ArchivedSessionList = (
  input: { archived: true; limit: number; cursor?: number },
  options: { signal?: AbortSignal },
) => Promise<ArchivedSessionPage>

export type ArchivedFamilyEntry = {
  /** Stable render key: the effective family key, or the member ID after a missing-root split. */
  key: string
  /** Family key from `familyArchiveID ?? id`. */
  familyID: string
  memberIDs: string[]
  /** One unarchive call per entry. A missing-root split lists a single member here. */
  unarchiveIDs: string[]
  memberCount: number
  archiving: boolean
  missingRoot: boolean
  directory?: string
  title?: string
}

/**
 * Load every archived page before grouping. The experimental listing pages by
 * the last `time.updated` cursor, so a family can straddle a page boundary.
 * Rows are de-duplicated by ID because cursor pagination can repeat a row when
 * several sessions share a timestamp.
 */
export async function loadArchivedSessions(
  list: ArchivedSessionList,
  limit = ARCHIVED_SESSION_PAGE_LIMIT,
  signal?: AbortSignal,
): Promise<ArchivedSessionRow[]> {
  const rows: ArchivedSessionRow[] = []
  const seen = new Set<string>()
  const cursors = new Set<number>()
  let cursor: number | undefined

  for (;;) {
    const page = await list({ archived: true, limit, ...(cursor !== undefined ? { cursor } : {}) }, { signal })
    let added = 0
    for (const row of page.data) {
      // An older server may ignore `archived=true`; keep the browser archived-only.
      if (typeof row.time.archived !== "number") continue
      if (seen.has(row.id)) continue
      seen.add(row.id)
      rows.push(row)
      added++
    }

    if (page.data.length < limit) return rows
    const next = page.data.at(-1)?.time.updated
    if (next === undefined || next === cursor || cursors.has(next)) return rows
    // A full page that only repeated known rows means the cursor stopped
    // advancing; stop instead of looping forever.
    if (added === 0) return rows
    cursors.add(next)
    cursor = next
  }
}

/**
 * Group archived rows by `familyArchiveID ?? id`. This deliberately ignores
 * ancestry: an independently archived child under an archived parent is its
 * own entry, and grouping by the stored family key keeps one family from
 * claiming another's members.
 *
 * If a non-null family key has no root row among the loaded rows, the root was
 * purged. That group is split into one entry per member keyed by the member's
 * own ID, so every entry can call unarchive for itself and never posts to the
 * missing root.
 */
export function groupArchivedSessions(sessions: readonly ArchivedSessionRow[]): ArchivedFamilyEntry[] {
  const byID = new Map(sessions.map((session) => [session.id, session]))
  const order: string[] = []
  const groups = new Map<string, ArchivedSessionRow[]>()

  for (const session of sessions) {
    const key = session.familyArchiveID ?? session.id
    const group = groups.get(key)
    if (group) {
      group.push(session)
      continue
    }
    groups.set(key, [session])
    order.push(key)
  }

  const entries: ArchivedFamilyEntry[] = []
  for (const key of order) {
    const members = groups.get(key)!
    const root = byID.get(key)
    if (root) {
      entries.push({
        key,
        familyID: key,
        memberIDs: members.map((member) => member.id),
        unarchiveIDs: [key],
        memberCount: root.familyMemberCount ?? members.length,
        archiving: members.some((member) => member.archiveState === "archiving"),
        missingRoot: false,
        directory: root.directory ?? members[0]?.directory,
        title: root.title ?? members[0]?.title,
      })
      continue
    }

    for (const member of members) {
      entries.push({
        key: member.id,
        familyID: key,
        memberIDs: [member.id],
        unarchiveIDs: [member.id],
        memberCount: 1,
        archiving: member.archiveState === "archiving",
        missingRoot: true,
        directory: member.directory,
        title: member.title,
      })
    }
  }

  return entries
}

export type ArchivedUnarchiveApi = {
  unarchive(input: { sessionID: string; directory?: string }): Promise<unknown>
}

export type ArchivedUnarchiveOutcome =
  | { status: "restored"; sessionIDs: string[] }
  | { status: "failed"; error: unknown }

export async function unarchiveArchivedEntry(input: {
  api: ArchivedUnarchiveApi
  entry: ArchivedFamilyEntry
  onError?: (error: unknown) => void
}): Promise<ArchivedUnarchiveOutcome> {
  const restored: string[] = []
  try {
    for (const sessionID of input.entry.unarchiveIDs) {
      await input.api.unarchive({ sessionID, directory: input.entry.directory })
      restored.push(sessionID)
    }
    return { status: "restored", sessionIDs: restored }
  } catch (error) {
    input.onError?.(error)
    return { status: "failed", error }
  }
}

/** Remove the entry only after a successful restore. */
export function applyArchivedUnarchive(
  entries: readonly ArchivedFamilyEntry[],
  key: string,
  outcome: ArchivedUnarchiveOutcome,
): ArchivedFamilyEntry[] {
  if (outcome.status !== "restored") return [...entries]
  return entries.filter((entry) => entry.key !== key)
}
