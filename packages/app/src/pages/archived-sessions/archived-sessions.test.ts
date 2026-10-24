import { describe, expect, test } from "bun:test"
import {
  applyArchivedUnarchive,
  groupArchivedSessions,
  loadArchivedSessions,
  unarchiveArchivedEntry,
  type ArchivedSessionRow,
} from "./archived-sessions"

const row = (input: {
  id: string
  familyArchiveID?: string | null
  archiveState?: "archiving" | null
  familyMemberCount?: number
  directory?: string
  title?: string
  updated?: number
  parentID?: string | null
}): ArchivedSessionRow => ({
  id: input.id,
  parentID: input.parentID,
  familyArchiveID: input.familyArchiveID,
  archiveState: input.archiveState,
  familyMemberCount: input.familyMemberCount,
  directory: input.directory ?? "/workspace",
  title: input.title ?? input.id,
  time: { updated: input.updated ?? 1, archived: 1 },
})

const page = (data: ArchivedSessionRow[]) => ({ data })

describe("archived session loading", () => {
  test("merges pages by cursor without duplicate rows", async () => {
    const calls: Array<{ limit: number; cursor?: number }> = []
    const loaded = await loadArchivedSessions(async (input) => {
      calls.push({ limit: input.limit, ...(input.cursor !== undefined ? { cursor: input.cursor } : {}) })
      if (input.cursor === undefined) {
        return page([row({ id: "child", familyArchiveID: "root", updated: 30 }), row({ id: "other", updated: 20 })])
      }
      return page([row({ id: "root", familyArchiveID: "root", updated: 10 })])
    }, 2)

    expect(calls).toEqual([{ limit: 2 }, { limit: 2, cursor: 20 }])
    expect(loaded.map((item) => item.id)).toEqual(["child", "other", "root"])
  })

  test("stops when a cursor does not advance and drops repeated rows", async () => {
    const loaded = await loadArchivedSessions(
      async (input) =>
        input.cursor === undefined
          ? page([row({ id: "one", updated: 5 }), row({ id: "two", updated: 5 })])
          : page([row({ id: "one", updated: 5 }), row({ id: "two", updated: 5 })]),
      2,
    )

    expect(loaded.map((item) => item.id)).toEqual(["one", "two"])
  })
})

describe("archived session grouping", () => {
  test("groups a normal family into one entry keyed by the family archive ID", () => {
    const entries = groupArchivedSessions([
      row({ id: "root", familyArchiveID: "root", familyMemberCount: 3 }),
      row({ id: "child-a", familyArchiveID: "root" }),
      row({ id: "child-b", familyArchiveID: "root" }),
    ])

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      key: "root",
      familyID: "root",
      memberIDs: ["root", "child-a", "child-b"],
      unarchiveIDs: ["root"],
      memberCount: 3,
      missingRoot: false,
    })
  })

  test("keeps an independently archived child under an archived parent separate", () => {
    const entries = groupArchivedSessions([
      row({ id: "parent", familyArchiveID: "parent", familyMemberCount: 1 }),
      row({ id: "child", familyArchiveID: "child", familyMemberCount: 1 }),
    ])

    expect(entries.map((entry) => entry.key)).toEqual(["parent", "child"])
    expect(entries.map((entry) => entry.memberIDs)).toEqual([["parent"], ["child"]])
  })

  test("merges a family split across pages into a single entry at the earliest position", async () => {
    const loaded = await loadArchivedSessions(
      async (input) =>
        input.cursor === undefined
          ? page([row({ id: "child", familyArchiveID: "root", updated: 30 })])
          : page([row({ id: "root", familyArchiveID: "root", familyMemberCount: 2, updated: 10 })]),
      1,
    )
    const entries = groupArchivedSessions(loaded)

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      key: "root",
      memberIDs: ["child", "root"],
      memberCount: 2,
      title: "root",
    })
  })

  test("does not duplicate an entry when the same family spans pages", () => {
    const entries = groupArchivedSessions([
      row({ id: "child", familyArchiveID: "root" }),
      row({ id: "root", familyArchiveID: "root", familyMemberCount: 2 }),
      row({ id: "child", familyArchiveID: "root" }),
    ])

    expect(entries.map((entry) => entry.key)).toEqual(["root"])
  })

  test("splits a missing-root group into per-member entries", () => {
    const entries = groupArchivedSessions([
      row({ id: "one", familyArchiveID: "missing", familyMemberCount: 2 }),
      row({ id: "two", familyArchiveID: "missing" }),
    ])

    expect(entries.map((entry) => entry.key)).toEqual(["one", "two"])
    expect(entries.map((entry) => entry.familyID)).toEqual(["missing", "missing"])
    expect(entries.map((entry) => entry.unarchiveIDs)).toEqual([["one"], ["two"]])
    expect(entries.every((entry) => entry.missingRoot && entry.memberCount === 1)).toBe(true)
  })

  test("marks an archiving family and exposes the disabled action state", () => {
    const [entry] = groupArchivedSessions([
      row({ id: "root", familyArchiveID: "root", archiveState: "archiving", familyMemberCount: 2 }),
      row({ id: "child", familyArchiveID: "root", archiveState: "archiving" }),
    ])

    expect(entry?.archiving).toBe(true)
  })
})

describe("archived session unarchive", () => {
  test("calls unarchive once for a family entry and removes it", async () => {
    const calls: Array<{ sessionID: string; directory?: string }> = []
    const entries = groupArchivedSessions([
      row({ id: "root", familyArchiveID: "root", directory: "/repo", familyMemberCount: 2 }),
      row({ id: "child", familyArchiveID: "root" }),
    ])

    const outcome = await unarchiveArchivedEntry({
      api: {
        unarchive: async (input) => {
          calls.push(input)
        },
      },
      entry: entries[0]!,
    })

    expect(calls).toEqual([{ sessionID: "root", directory: "/repo" }])
    expect(applyArchivedUnarchive(entries, "root", outcome)).toEqual([])
  })

  test("calls unarchive per member after a missing-root split", async () => {
    const calls: string[] = []
    const entries = groupArchivedSessions([
      row({ id: "one", familyArchiveID: "missing" }),
      row({ id: "two", familyArchiveID: "missing" }),
    ])
    const api = {
      unarchive: async (input: { sessionID: string }) => {
        calls.push(input.sessionID)
      },
    }

    for (const entry of entries) await unarchiveArchivedEntry({ api, entry })

    expect(calls).toEqual(["one", "two"])
  })

  test("leaves the entry in place and reports an error when unarchive fails", async () => {
    const failure = new Error("offline")
    let reported: unknown
    const entries = groupArchivedSessions([row({ id: "root", familyArchiveID: "root", familyMemberCount: 1 })])

    const outcome = await unarchiveArchivedEntry({
      api: {
        unarchive: async () => {
          throw failure
        },
      },
      entry: entries[0]!,
      onError: (error) => {
        reported = error
      },
    })

    expect(reported).toBe(failure)
    expect(outcome).toEqual({ status: "failed", error: failure })
    expect(applyArchivedUnarchive(entries, "root", outcome)).toEqual(entries)
  })
})
