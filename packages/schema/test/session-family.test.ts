import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { Session } from "../src/session.js"

const info = {
  id: "ses_family",
  projectID: "global",
  cost: 0,
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  time: { created: 0, updated: 0 },
  location: { directory: "/project" },
}

describe("Session.Info family archive fields", () => {
  test("accepts explicit null for every family field", () => {
    expect(
      Schema.decodeUnknownSync(Session.Info)({
        ...info,
        familyArchiveID: null,
        archiveCause: null,
        archiveState: null,
        archiveStateAt: null,
      }),
    ).toMatchObject({
      familyArchiveID: null,
      archiveCause: null,
      archiveState: null,
      archiveStateAt: null,
    })
  })

  test("accepts the archiving state and rejects other states", () => {
    expect(Schema.decodeUnknownSync(Session.Info)({ ...info, archiveState: "archiving" }).archiveState).toBe(
      "archiving",
    )
    expect(() => Schema.decodeUnknownSync(Session.Info)({ ...info, archiveState: "other" })).toThrow()
  })
})
