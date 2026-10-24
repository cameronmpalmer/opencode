import type { Session } from "@opencode-ai/sdk/v2/client"
import { notifySessionTabsRemoved } from "@/components/titlebar-session-events"
import type { ServerConnection } from "@/context/server"
import { archiveSessionFamily, type ArchivePreviewCounts, type SessionArchiveApi } from "@/utils/session-archive"

type HomeSession = {
  id: string
  directory: string
}

export async function archiveHomeSession(input: {
  server: ServerConnection.Key
  session: HomeSession
  api: SessionArchiveApi
  confirm: (preview: ArchivePreviewCounts) => boolean | Promise<boolean>
  localSessions: () => readonly Pick<Session, "id" | "parentID">[]
  legacyArchive: (sessionID: string, directory?: string) => Promise<unknown>
  remove: (sessionIDs: string[]) => void
  onError?: (error: unknown) => void
}) {
  return archiveSessionFamily({
    api: input.api,
    sessionID: input.session.id,
    directory: input.session.directory,
    confirm: input.confirm,
    localSessions: input.localSessions,
    legacyArchive: input.legacyArchive,
    remove: (sessionIDs) => {
      input.remove(sessionIDs)
      notifySessionTabsRemoved({
        server: input.server,
        directory: input.session.directory,
        sessionIDs,
      })
    },
    onError: input.onError,
  })
}
