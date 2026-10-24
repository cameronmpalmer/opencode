import type { Session } from "@opencode-ai/sdk/v2/client"

export type ArchiveSessionNode = Pick<Session, "id" | "parentID" | "directory"> & {
  familyArchiveID?: string | null
  title?: string
}

export type ArchivePreviewCounts = {
  rootID: string
  descendantCount: number
  activeCount: number
  activeSessionIDs: string[]
  fallback: boolean
}

export type SessionArchiveApi = {
  archivePreview(input: { sessionID: string; directory?: string }): Promise<{
    root_id: string
    descendant_count: number
    active_count: number
    active_session_ids: string[]
  }>
  archive(input: { sessionID: string; directory?: string; cancel_active?: boolean }): Promise<{
    root_id: string
    archived_ids: string[]
    already_archived_ids: string[]
    cancelled_ids: string[]
    archive_time: number
  }>
}

export type LegacyArchiveClient = {
  session: {
    update(input: { sessionID: string; directory?: string; time: { archived: number | null } }): Promise<unknown>
  }
}

export type ArchiveOutcome =
  | { status: "archived"; sessionIDs: string[] }
  | { status: "cancelled" }
  | { status: "failed"; error: unknown }

type Translator = (key: string, vars?: Record<string, string | number>) => string

const plural = (count: number, singular: string, plural: string) => (count === 1 ? singular : plural)
const unique = <T>(values: readonly T[]) => [...new Set(values)]

function errorBody(error: unknown): unknown {
  if (error instanceof Error && error.cause && typeof error.cause === "object" && "body" in error.cause) {
    return (error.cause as Record<string, unknown>).body
  }
  return error
}

function errorTag(error: unknown): string | undefined {
  const body = errorBody(error)
  if (!body || typeof body !== "object") return
  const value = body as Record<string, unknown>
  if (typeof value._tag === "string") return value._tag
  if (typeof value.name === "string") return value.name
  if (typeof value.code === "string") return value.code
  const nested = value.error
  if (nested && typeof nested === "object") {
    const tag = (nested as Record<string, unknown>)._tag
    if (typeof tag === "string") return tag
  }
}

function errorStatus(error: unknown): number | string | undefined {
  if (error instanceof Error && error.cause && typeof error.cause === "object" && "status" in error.cause) {
    const status = (error.cause as Record<string, unknown>).status
    if (typeof status === "number" || typeof status === "string") return status
  }
  const body = errorBody(error)
  if (!body || typeof body !== "object") return
  const value = body as Record<string, unknown>
  if (typeof value.status === "number" || typeof value.status === "string") return value.status
  if (typeof value.code === "number") return value.code
  if (typeof value.code === "string" && /^\d+$/.test(value.code)) return Number(value.code)
}

export function isArchiveFeatureUnavailable(error: unknown): boolean {
  // A 404 alone is not enough: the server uses NotFoundError for a missing
  // session and FeatureDisabledError for the familyArchive flag. Treating every
  // 404 as feature-disabled would turn a vanished session into a legacy PATCH.
  const tag = errorTag(error)
  if (tag === "FeatureDisabledError" || tag === "FeatureDisabled") return true
  if (tag === "SessionNotFoundError" || tag === "NotFoundError" || tag === "NotFound") return false
  const code = errorStatus(error)
  return code === "FeatureDisabledError" || code === "FeatureDisabled"
}

export function isArchivePending(session: { archiveState?: "archiving" | null } | undefined): boolean {
  return session?.archiveState === "archiving"
}

export function archiveErrorMessage(error: unknown, fallback = "Archive failed") {
  const body = errorBody(error)
  if (body && typeof body === "object") {
    const value = body as Record<string, unknown>
    const data = value.data
    if (data && typeof data === "object") {
      const message = (data as Record<string, unknown>).message
      if (typeof message === "string" && message) return message
    }
    if (typeof value.message === "string" && value.message) return value.message
  }

  switch (errorTag(error)) {
    case "SessionFamilyMemberError":
      return "This session belongs to a family. Archive the family root instead."
    case "SessionArchiveInProgressError":
      return "This session is already being archived. Try again in a moment."
    case "SessionArchiveBlockedError":
      return "This session cannot be archived right now."
  }

  if (error instanceof Error && error.message) return error.message
  return fallback
}

export function collectSessionSubtree(sessions: readonly Pick<Session, "id" | "parentID">[], rootID: string): string[] {
  const children = new Map<string, string[]>()
  for (const session of sessions) {
    if (!session.parentID) continue
    const list = children.get(session.parentID)
    if (list) list.push(session.id)
    else children.set(session.parentID, [session.id])
  }

  const seen = new Set<string>()
  const stack = [rootID]
  while (stack.length > 0) {
    const id = stack.pop()!
    if (seen.has(id)) continue
    seen.add(id)
    stack.push(...(children.get(id) ?? []))
  }
  return [...seen]
}

function nearestRoot(session: ArchiveSessionNode, byID: ReadonlyMap<string, ArchiveSessionNode>): ArchiveSessionNode {
  const seen = new Set<string>()
  let current = session
  while (current.parentID && byID.has(current.parentID) && !seen.has(current.id)) {
    seen.add(current.id)
    current = byID.get(current.parentID)!
  }
  return current
}

export type SessionFamilyGroup = {
  rootID: string
  directory: string
  title?: string
  sessionIDs: string[]
}

export function groupSessionFamilies(sessions: readonly ArchiveSessionNode[]): SessionFamilyGroup[] {
  const byID = new Map(sessions.map((session) => [session.id, session]))
  const groups = new Map<string, SessionFamilyGroup>()

  for (const session of sessions) {
    const root = nearestRoot(session, byID)
    const rootID = root.familyArchiveID ?? session.familyArchiveID ?? root.id
    const group = groups.get(rootID)
    if (group) {
      if (!group.sessionIDs.includes(session.id)) group.sessionIDs.push(session.id)
      continue
    }
    groups.set(rootID, {
      rootID,
      directory: root.directory,
      title: root.title,
      sessionIDs: [session.id],
    })
  }

  return [...groups.values()]
    .map((group) => ({ ...group, sessionIDs: unique(group.sessionIDs).sort() }))
    .sort((a, b) => (a.rootID < b.rootID ? -1 : a.rootID > b.rootID ? 1 : 0))
}

export function archiveConfirmation(preview: ArchivePreviewCounts, translate?: Translator): string {
  const { descendantCount, activeCount } = preview
  if (descendantCount > 0) {
    if (translate) {
      return translate("session.archive.confirm.family", {
        count: descendantCount,
        tasks: activeCount,
      })
    }
    return `Archive this session and its ${descendantCount} child ${plural(
      descendantCount,
      "session",
      "sessions",
    )}? ${activeCount} running ${plural(activeCount, "task", "tasks")} will be cancelled.`
  }
  if (activeCount > 0) {
    return translate
      ? translate("session.archive.confirm.running")
      : "This session is running. Archive it and cancel the running task?"
  }
  return translate
    ? translate("session.archive.confirm.single")
    : "Archive this session? It will be hidden from your session list."
}

export function workspaceArchiveConfirmation(
  previews: readonly ArchivePreviewCounts[],
  translate?: Translator,
): string {
  const families = previews.length
  const sessions = previews.reduce((total, preview) => total + preview.descendantCount + 1, 0)
  const tasks = previews.reduce((total, preview) => total + preview.activeCount, 0)
  if (families === 0) {
    return translate ? translate("workspace.reset.archived.none") : "No active sessions will be archived."
  }
  if (translate) {
    return translate("workspace.archive.confirm", { families, sessions, tasks })
  }
  return `Archive ${families} ${plural(families, "family", "families")} and ${sessions} ${plural(
    sessions,
    "session",
    "sessions",
  )}? ${tasks} running ${plural(tasks, "task", "tasks")} will be cancelled.`
}

export function nextSurvivingSession(
  sessions: readonly Pick<Session, "id">[],
  removed: ReadonlySet<string>,
  currentID: string | undefined,
): string | undefined {
  if (!currentID || !removed.has(currentID)) return
  const index = sessions.findIndex((session) => session.id === currentID)
  if (index === -1) {
    return sessions.find((session) => !removed.has(session.id))?.id
  }
  for (let offset = 1; offset <= sessions.length; offset++) {
    const next = sessions[index + offset] ?? sessions[index - offset]
    if (next && !removed.has(next.id)) return next.id
  }
}

export async function previewSessionArchive(input: {
  api: Pick<SessionArchiveApi, "archivePreview">
  sessionID: string
  directory?: string
}): Promise<ArchivePreviewCounts> {
  try {
    const preview = await input.api.archivePreview({
      sessionID: input.sessionID,
      directory: input.directory,
    })
    return {
      rootID: preview.root_id ?? input.sessionID,
      descendantCount: preview.descendant_count ?? 0,
      activeCount: preview.active_count ?? 0,
      activeSessionIDs: preview.active_session_ids ?? [],
      fallback: false,
    }
  } catch (error) {
    if (!isArchiveFeatureUnavailable(error)) throw error
    return {
      rootID: input.sessionID,
      descendantCount: 0,
      activeCount: 0,
      activeSessionIDs: [],
      fallback: true,
    }
  }
}

export type SessionArchiveRoot = {
  rootID: string
  directory: string
  sessionIDs: readonly string[]
  fallback: boolean
}

export type ArchiveRootOutcome = {
  rootID: string
  sessionIDs: string[]
  error?: unknown
}

async function archiveLegacyGroup(input: {
  sessionIDs: readonly string[]
  directory?: string
  legacyArchive: (sessionID: string, directory?: string) => Promise<unknown>
}) {
  const results = await Promise.allSettled(
    input.sessionIDs.map((sessionID) => input.legacyArchive(sessionID, input.directory)),
  )
  const failed = results.find((result): result is PromiseRejectedResult => result.status === "rejected")
  if (failed) throw failed.reason
  return unique(input.sessionIDs)
}

export async function archiveSessionRoots(input: {
  api: SessionArchiveApi
  roots: readonly SessionArchiveRoot[]
  legacyArchive: (sessionID: string, directory?: string) => Promise<unknown>
}): Promise<ArchiveRootOutcome[]> {
  return Promise.all(
    input.roots.map(async (root): Promise<ArchiveRootOutcome> => {
      try {
        if (root.fallback) {
          const sessionIDs = await archiveLegacyGroup({
            sessionIDs: root.sessionIDs,
            directory: root.directory,
            legacyArchive: input.legacyArchive,
          })
          return { rootID: root.rootID, sessionIDs }
        }
        let result: Awaited<ReturnType<SessionArchiveApi["archive"]>>
        try {
          result = await input.api.archive({ sessionID: root.rootID, directory: root.directory })
        } catch (error) {
          if (!isArchiveFeatureUnavailable(error)) throw error
          const sessionIDs = await archiveLegacyGroup({
            sessionIDs: root.sessionIDs,
            directory: root.directory,
            legacyArchive: input.legacyArchive,
          })
          return { rootID: root.rootID, sessionIDs }
        }
        return {
          rootID: root.rootID,
          sessionIDs: unique([
            result.root_id ?? root.rootID,
            ...root.sessionIDs,
            ...result.archived_ids,
            ...result.already_archived_ids,
          ]),
        }
      } catch (error) {
        return { rootID: root.rootID, sessionIDs: [], error }
      }
    }),
  )
}

export async function legacyArchiveSession(client: LegacyArchiveClient, sessionID: string, directory?: string) {
  await client.session.update({
    sessionID,
    directory,
    time: { archived: Date.now() },
  })
}

export async function archiveSessionFamily(input: {
  api: SessionArchiveApi
  sessionID: string
  directory?: string
  confirm: (preview: ArchivePreviewCounts) => boolean | Promise<boolean>
  localSessions: () => readonly Pick<Session, "id" | "parentID">[]
  legacyArchive: (sessionID: string, directory?: string) => Promise<unknown>
  remove: (sessionIDs: string[]) => void
  onError?: (error: unknown) => void
}): Promise<ArchiveOutcome> {
  let preview: ArchivePreviewCounts
  try {
    preview = await previewSessionArchive({
      api: input.api,
      sessionID: input.sessionID,
      directory: input.directory,
    })
  } catch (error) {
    input.onError?.(error)
    return { status: "failed", error }
  }

  if (!(await input.confirm(preview))) return { status: "cancelled" }

  let sessionIDs: string[]
  const group = collectSessionSubtree(input.localSessions(), input.sessionID)
  try {
    if (preview.fallback) {
      sessionIDs = await archiveLegacyGroup({
        sessionIDs: group,
        directory: input.directory,
        legacyArchive: input.legacyArchive,
      })
    } else {
      let result: Awaited<ReturnType<SessionArchiveApi["archive"]>>
      try {
        result = await input.api.archive({
          sessionID: input.sessionID,
          directory: input.directory,
        })
      } catch (error) {
        if (!isArchiveFeatureUnavailable(error)) throw error
        sessionIDs = await archiveLegacyGroup({
          sessionIDs: group,
          directory: input.directory,
          legacyArchive: input.legacyArchive,
        })
        input.remove(sessionIDs)
        return { status: "archived", sessionIDs }
      }
      sessionIDs = unique([
        result.root_id ?? input.sessionID,
        ...result.archived_ids,
        ...result.already_archived_ids,
        ...group,
      ])
    }
  } catch (error) {
    input.onError?.(error)
    return { status: "failed", error }
  }

  input.remove(sessionIDs)
  return { status: "archived", sessionIDs }
}
