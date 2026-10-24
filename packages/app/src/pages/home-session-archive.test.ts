import { expect, test } from "bun:test"
import { SESSION_TABS_REMOVED_EVENT, readSessionTabsRemovedDetail } from "@/components/titlebar-session-events"
import { archiveHomeSession } from "./home-session-archive"
import type { ServerConnection } from "@/context/server"

const remote = "remote" as ServerConnection.Key

test("archiving a Home session removes its open titlebar tab", async () => {
  let detail: ReturnType<typeof readSessionTabsRemovedDetail>
  let removed = false
  window.addEventListener(
    SESSION_TABS_REMOVED_EVENT,
    (event) => {
      detail = readSessionTabsRemovedDetail(event)
    },
    { once: true },
  )

  await archiveHomeSession({
    server: remote,
    session: { id: "ses_1", directory: "/workspace" },
    api: {
      archivePreview: async () => ({
        root_id: "ses_1",
        descendant_count: 0,
        active_count: 0,
        active_session_ids: [],
      }),
      archive: async () => ({
        root_id: "ses_1",
        archived_ids: ["ses_1"],
        already_archived_ids: [],
        cancelled_ids: [],
        archive_time: 1,
      }),
    },
    confirm: () => true,
    localSessions: () => [{ id: "ses_1", directory: "/workspace" }],
    legacyArchive: async () => undefined,
    remove: (sessionIDs) => {
      removed = sessionIDs.includes("ses_1")
    },
  })

  expect(removed).toBe(true)
  expect(detail).toEqual({ server: remote, directory: "/workspace", sessionIDs: ["ses_1"] })
})

test("reports archive failures without removing the session", async () => {
  const failure = new Error("offline")
  let error: unknown
  let removed = false

  await archiveHomeSession({
    server: remote,
    session: { id: "ses_1", directory: "/workspace" },
    api: {
      archivePreview: async () => ({
        root_id: "ses_1",
        descendant_count: 0,
        active_count: 0,
        active_session_ids: [],
      }),
      archive: async () => Promise.reject(failure),
    },
    confirm: () => true,
    localSessions: () => [{ id: "ses_1", directory: "/workspace" }],
    legacyArchive: async () => undefined,
    remove: () => {
      removed = true
    },
    onError: (value) => {
      error = value
    },
  })

  expect(error).toBe(failure)
  expect(removed).toBe(false)
})
