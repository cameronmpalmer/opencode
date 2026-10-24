import type { SessionApi, SessionInfo, SessionListInput } from "@opencode-ai/client/promise"
import type { Session } from "@opencode-ai/sdk/v2/client"
import { withTimestampedFallback } from "./session-title"

export type FamilyArchiveFields = {
  familyArchiveID?: string | null
  archiveCause?: "user" | "cascade" | "migration" | null
  archiveState?: "archiving" | null
  archiveStateAt?: number | null
  familyMemberCount?: number
}

export type SessionInfoWithFamily = (SessionInfo | Session) & FamilyArchiveFields
export type SessionWithFamily = Session & FamilyArchiveFields

export function normalizeSessionInfo(input: SessionInfoWithFamily): SessionWithFamily {
  if (!("location" in input)) return input
  return {
    id: input.id,
    slug: input.id,
    projectID: input.projectID,
    workspaceID: input.location.workspaceID,
    directory: input.location.directory,
    path: input.subpath,
    parentID: input.parentID,
    cost: input.cost,
    tokens: input.tokens,
    title: withTimestampedFallback(input),
    agent: input.agent,
    model: input.model,
    version: "",
    time: input.time,
    revert: input.revert && {
      messageID: input.revert.messageID,
      partID: input.revert.partID,
      snapshot: input.revert.snapshot,
    },
    familyArchiveID: input.familyArchiveID,
    archiveCause: input.archiveCause,
    archiveState: input.archiveState,
    archiveStateAt: input.archiveStateAt,
    familyMemberCount: input.familyMemberCount,
  }
}

export async function listAllSessions(api: Pick<SessionApi, "list">, input: Omit<SessionListInput, "cursor">) {
  const load = async (cursor?: string): Promise<SessionWithFamily[]> => {
    const result = await api.list({ ...input, limit: input.limit ?? 100, cursor })
    const sessions = result.data.map(normalizeSessionInfo)
    if (result.data.length === 0 || !result.cursor.next) return sessions
    return [...sessions, ...(await load(result.cursor.next))]
  }
  return load()
}
