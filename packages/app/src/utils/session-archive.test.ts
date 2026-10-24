import { describe, expect, test } from "bun:test"
import { readFileSync } from "node:fs"
import {
  archiveConfirmation,
  archiveErrorMessage,
  archiveSessionFamily,
  archiveSessionRoots,
  collectSessionSubtree,
  groupSessionFamilies,
  isArchiveFeatureUnavailable,
  isArchivePending,
  nextSurvivingSession,
  previewSessionArchive,
  workspaceArchiveConfirmation,
  type ArchivePreviewCounts,
  type SessionArchiveApi,
} from "./session-archive"

const preview = (input: Partial<ArchivePreviewCounts> = {}): ArchivePreviewCounts => ({
  rootID: "root",
  descendantCount: 0,
  activeCount: 0,
  activeSessionIDs: [],
  fallback: false,
  ...input,
})

const sessions = [
  { id: "root", directory: "/repo" },
  { id: "child", parentID: "root", directory: "/repo" },
  { id: "grandchild", parentID: "child", directory: "/repo" },
  { id: "other", directory: "/repo" },
]

function api(overrides: Partial<SessionArchiveApi> = {}): SessionArchiveApi {
  return {
    archivePreview: async () => ({
      root_id: "root",
      descendant_count: 2,
      active_count: 1,
      active_session_ids: ["root"],
    }),
    archive: async () => ({
      root_id: "root",
      archived_ids: ["root", "child", "grandchild"],
      already_archived_ids: [],
      cancelled_ids: ["root"],
      archive_time: 1,
    }),
    ...overrides,
  }
}

describe("collectSessionSubtree", () => {
  test("walks every local descendant", () => {
    expect(collectSessionSubtree(sessions, "root")).toEqual(["root", "child", "grandchild"])
  })

  test("returns the root when it has no local descendants", () => {
    expect(collectSessionSubtree(sessions, "other")).toEqual(["other"])
  })
})

describe("groupSessionFamilies", () => {
  test("groups by family archive id and deduplicates subtree roots", () => {
    const groups = groupSessionFamilies([
      { id: "root", directory: "/repo", familyArchiveID: "family-1" },
      { id: "child", parentID: "root", directory: "/repo" },
      { id: "sibling-root", directory: "/repo" },
      { id: "sibling-child", parentID: "sibling-root", directory: "/repo", familyArchiveID: "family-1" },
    ])

    expect(groups).toEqual([
      { rootID: "family-1", directory: "/repo", title: undefined, sessionIDs: ["child", "root", "sibling-child"] },
      { rootID: "sibling-root", directory: "/repo", title: undefined, sessionIDs: ["sibling-root"] },
    ])
  })

  test("uses the nearest local root when a session has no family id", () => {
    expect(groupSessionFamilies(sessions)).toEqual([
      { rootID: "other", directory: "/repo", title: undefined, sessionIDs: ["other"] },
      { rootID: "root", directory: "/repo", title: undefined, sessionIDs: ["child", "grandchild", "root"] },
    ])
  })
})

describe("archiveConfirmation", () => {
  test("names descendants and running tasks", () => {
    expect(archiveConfirmation(preview({ descendantCount: 2, activeCount: 1 }))).toBe(
      "Archive this session and its 2 child sessions? 1 running task will be cancelled.",
    )
  })

  test("pluralizes a single child and task", () => {
    expect(archiveConfirmation(preview({ descendantCount: 1, activeCount: 1 }))).toBe(
      "Archive this session and its 1 child session? 1 running task will be cancelled.",
    )
  })

  test("warns when the session itself is running", () => {
    expect(archiveConfirmation(preview({ activeCount: 1 }))).toBe(
      "This session is running. Archive it and cancel the running task?",
    )
  })

  test("uses the existing single-session copy", () => {
    expect(archiveConfirmation(preview())).toBe("Archive this session? It will be hidden from your session list.")
  })
})

describe("workspaceArchiveConfirmation", () => {
  test("names families, sessions, and running tasks", () => {
    expect(
      workspaceArchiveConfirmation([
        preview({ descendantCount: 1, activeCount: 1 }),
        preview({ rootID: "root-2", descendantCount: 2, activeCount: 0 }),
      ]),
    ).toBe("Archive 2 families and 5 sessions? 1 running task will be cancelled.")
  })

  test("pluralizes a single family, session, and task", () => {
    expect(workspaceArchiveConfirmation([preview({ activeCount: 1 })])).toBe(
      "Archive 1 family and 1 session? 1 running task will be cancelled.",
    )
  })

  test("reports when nothing will be archived", () => {
    expect(workspaceArchiveConfirmation([])).toBe("No active sessions will be archived.")
  })
})

describe("isArchiveFeatureUnavailable", () => {
  test("accepts SDK-wrapped 404 feature disabled errors", () => {
    const error = new Error("family archive disabled", {
      cause: { status: 404, body: { _tag: "FeatureDisabledError", feature: "familyArchive" } },
    })
    expect(isArchiveFeatureUnavailable(error)).toBe(true)
  })

  test("accepts bare tagged feature disabled errors", () => {
    expect(isArchiveFeatureUnavailable({ _tag: "FeatureDisabledError" })).toBe(true)
  })

  test("does not treat a missing session as a fallback", () => {
    expect(isArchiveFeatureUnavailable({ _tag: "NotFoundError", status: 404 })).toBe(false)
    expect(isArchiveFeatureUnavailable({ _tag: "SessionNotFoundError", status: 404 })).toBe(false)
  })

  test("does not treat an untagged 404 as feature disabled", () => {
    expect(isArchiveFeatureUnavailable(new Error("missing", { cause: { status: 404 } }))).toBe(false)
  })
})

describe("previewSessionArchive", () => {
  test("maps the server preview", async () => {
    const result = await previewSessionArchive({ api: api(), sessionID: "root", directory: "/repo" })
    expect(result).toEqual({
      rootID: "root",
      descendantCount: 2,
      activeCount: 1,
      activeSessionIDs: ["root"],
      fallback: false,
    })
  })

  test("returns a zero preview when the feature is unavailable", async () => {
    const result = await previewSessionArchive({
      api: api({
        archivePreview: async () => {
          throw new Error("disabled", { cause: { status: 404, body: { _tag: "FeatureDisabledError" } } })
        },
      }),
      sessionID: "root",
    })

    expect(result).toEqual({
      rootID: "root",
      descendantCount: 0,
      activeCount: 0,
      activeSessionIDs: [],
      fallback: true,
    })
  })
})

describe("archiveSessionRoots", () => {
  test("keeps archiving the other roots when one fails", async () => {
    const failure = new Error("blocked", { cause: { status: 409, body: { _tag: "SessionArchiveBlockedError" } } })
    const archived: string[] = []

    const result = await archiveSessionRoots({
      api: api({
        archive: async ({ sessionID }) => {
          if (sessionID === "root-2") throw failure
          archived.push(sessionID)
          return {
            root_id: sessionID,
            archived_ids: [sessionID, `${sessionID}-child`],
            already_archived_ids: [],
            cancelled_ids: [],
            archive_time: 1,
          }
        },
      }),
      roots: [
        { rootID: "root-1", directory: "/repo", sessionIDs: ["root-1"], fallback: false },
        { rootID: "root-2", directory: "/repo", sessionIDs: ["root-2"], fallback: false },
        { rootID: "root-3", directory: "/repo", sessionIDs: ["root-3", "root-3-child"], fallback: true },
      ],
      legacyArchive: async (sessionID) => {
        archived.push(`legacy:${sessionID}`)
      },
    })

    expect(archived).toEqual(["root-1", "legacy:root-3", "legacy:root-3-child"])
    expect(result).toEqual([
      { rootID: "root-1", sessionIDs: ["root-1", "root-1-child"] },
      { rootID: "root-2", sessionIDs: [], error: failure },
      { rootID: "root-3", sessionIDs: ["root-3", "root-3-child"] },
    ])
  })

  test("falls back to per-session legacy PATCHes when POST returns FeatureDisabled", async () => {
    const legacy: string[] = []
    const result = await archiveSessionRoots({
      api: api({
        archive: async () => {
          throw new Error("disabled", { cause: { status: 404, body: { _tag: "FeatureDisabledError" } } })
        },
      }),
      roots: [{ rootID: "root", directory: "/repo", sessionIDs: ["root", "child"], fallback: false }],
      legacyArchive: async (sessionID) => {
        legacy.push(sessionID)
      },
    })

    expect(legacy).toEqual(["root", "child"])
    expect(result).toEqual([{ rootID: "root", sessionIDs: ["root", "child"] }])
  })

  test("keeps a root unremoved when one legacy PATCH fails", async () => {
    const result = await archiveSessionRoots({
      api: api(),
      roots: [{ rootID: "root", directory: "/repo", sessionIDs: ["root", "child"], fallback: true }],
      legacyArchive: async (sessionID) => {
        if (sessionID === "child") throw new Error("offline")
      },
    })

    expect(result[0]?.sessionIDs).toEqual([])
    expect(result[0]?.error).toBeInstanceOf(Error)
  })
})

describe("archiveSessionFamily", () => {
  test("previews, confirms, archives, and removes the whole local subtree", async () => {
    let shown: ArchivePreviewCounts | undefined
    let archived = 0
    let removed: string[] = []
    const calls: string[] = []

    const result = await archiveSessionFamily({
      api: api({
        archive: async () => {
          archived++
          return {
            root_id: "root",
            archived_ids: ["root", "child"],
            already_archived_ids: ["grandchild"],
            cancelled_ids: ["root"],
            archive_time: 1,
          }
        },
      }),
      sessionID: "root",
      directory: "/repo",
      confirm: (value) => {
        shown = value
        return true
      },
      localSessions: () => sessions,
      legacyArchive: async () => {
        calls.push("legacy")
      },
      remove: (ids) => {
        removed = ids
      },
    })

    expect(shown).toMatchObject({ descendantCount: 2, activeCount: 1 })
    expect(archived).toBe(1)
    expect(calls).toEqual([])
    expect(removed).toEqual(["root", "child", "grandchild"])
    expect(result).toEqual({ status: "archived", sessionIDs: ["root", "child", "grandchild"] })
  })

  test("falls back to a legacy PATCH for every session in the group", async () => {
    let removed: string[] = []
    const legacy: string[] = []
    let archived = 0

    const result = await archiveSessionFamily({
      api: api({
        archivePreview: async () => {
          throw new Error("disabled", { cause: { status: 404, body: { _tag: "FeatureDisabledError" } } })
        },
        archive: async () => {
          archived++
          throw new Error("should not archive")
        },
      }),
      sessionID: "root",
      directory: "/repo",
      confirm: () => true,
      localSessions: () => sessions,
      legacyArchive: async (sessionID) => {
        legacy.push(sessionID)
      },
      remove: (ids) => {
        removed = ids
      },
    })

    expect(legacy).toEqual(["root", "child", "grandchild"])
    expect(archived).toBe(0)
    expect(removed).toEqual(["root", "child", "grandchild"])
    expect(result).toEqual({ status: "archived", sessionIDs: ["root", "child", "grandchild"] })
  })

  test("falls back to per-session PATCHes when POST returns FeatureDisabled", async () => {
    const legacy: string[] = []
    let removed: string[] = []
    const result = await archiveSessionFamily({
      api: api({
        archive: async () => {
          throw new Error("disabled", { cause: { status: 404, body: { _tag: "FeatureDisabledError" } } })
        },
      }),
      sessionID: "root",
      directory: "/repo",
      confirm: () => true,
      localSessions: () => sessions,
      legacyArchive: async (sessionID) => {
        legacy.push(sessionID)
      },
      remove: (ids) => {
        removed = ids
      },
    })

    expect(legacy).toEqual(["root", "child", "grandchild"])
    expect(removed).toEqual(["root", "child", "grandchild"])
    expect(result).toEqual({ status: "archived", sessionIDs: ["root", "child", "grandchild"] })
  })

  test("does not archive when the confirmation is dismissed", async () => {
    let called = false
    const result = await archiveSessionFamily({
      api: api({
        archive: async () => {
          called = true
          throw new Error("unreachable")
        },
      }),
      sessionID: "root",
      confirm: () => false,
      localSessions: () => sessions,
      legacyArchive: async () => undefined,
      remove: () => undefined,
    })

    expect(called).toBe(false)
    expect(result).toEqual({ status: "cancelled" })
  })

  test("reports a 409 archive failure and keeps the tree", async () => {
    let error: unknown
    let removed = false
    const failure = new Error("family", {
      cause: { status: 409, body: { _tag: "SessionFamilyMemberError", message: "Archive the root" } },
    })

    const result = await archiveSessionFamily({
      api: api({
        archive: async () => {
          throw failure
        },
      }),
      sessionID: "child",
      confirm: () => true,
      localSessions: () => sessions,
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
    expect(result).toEqual({ status: "failed", error: failure })
    expect(archiveErrorMessage(failure)).toBe("Archive the root")
  })
})

describe("isArchivePending", () => {
  test("only reports the archiving marker", () => {
    expect(isArchivePending({ archiveState: "archiving" })).toBe(true)
    expect(isArchivePending({ archiveState: null })).toBe(false)
    expect(isArchivePending(undefined)).toBe(false)
  })
})

describe("nextSurvivingSession", () => {
  test("skips descendants removed with the root", () => {
    const removed = new Set(["root", "child", "grandchild"])
    expect(nextSurvivingSession(sessions, removed, "root")).toBe("other")
    expect(nextSurvivingSession(sessions, removed, "grandchild")).toBe("other")
  })

  test("prefers the nearest forward survivor then the nearest backward one", () => {
    const ordered = [{ id: "a" }, { id: "b" }, { id: "c" }, { id: "d" }]
    expect(nextSurvivingSession(ordered, new Set(["c"]), "c")).toBe("d")
    expect(nextSurvivingSession(ordered, new Set(["d"]), "d")).toBe("c")
    expect(nextSurvivingSession(ordered, new Set(["a"]), "a")).toBe("b")
  })

  test("does not navigate when the current route survives", () => {
    expect(nextSurvivingSession(sessions, new Set(["root"]), "other")).toBeUndefined()
  })
})

test("UI archive entry points do not issue bare archive updates", () => {
  const paths = [
    "../pages/layout.tsx",
    "../pages/session/timeline/message-timeline.tsx",
    "../pages/home/home-sessions-controller.tsx",
    "../pages/home/home-sessions-view.tsx",
    "../pages/layout/sidebar-items.tsx",
  ]

  for (const path of paths) {
    const source = readFileSync(new URL(path, import.meta.url), "utf8")
    expect(source).not.toMatch(/archived:\s*Date\.now\(\)/)
    expect(source).not.toMatch(/\.session\.update\([\s\S]{0,240}?archived/)
  }
})
