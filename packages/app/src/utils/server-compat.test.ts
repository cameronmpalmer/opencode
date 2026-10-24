import { describe, expect, test } from "bun:test"
import { createApiForServer, createSdkForServer } from "./server"
import { createCompatibleApi } from "./server-compat"

function setup(
  protocol: "v1" | "v2" | Promise<"v1" | "v2">,
  responses?: { vcs?: { branch: string; default_branch: string }; sessions?: unknown[] },
) {
  const requests: Request[] = []
  const fetcher = Object.assign(
    async (input: string | URL | Request, init?: RequestInit) => {
      const request = new Request(input, init)
      requests.push(request)
      if (request.method === "PATCH") {
        return Response.json({
          id: "ses_1",
          slug: "ses_1",
          projectID: "project",
          directory: "/repo",
          title: "Session",
          version: "1",
          time: { created: 1, updated: 1 },
        })
      }
      if (request.method === "GET" && new URL(request.url).pathname.endsWith("/archive-preview")) {
        return Response.json({
          root_id: "ses_1",
          descendant_count: 2,
          active_count: 1,
          active_session_ids: ["ses_1"],
        })
      }
      if (request.method === "POST" && new URL(request.url).pathname.endsWith("/archive")) {
        return Response.json({
          root_id: "ses_1",
          archived_ids: ["ses_1", "ses_2"],
          already_archived_ids: [],
          cancelled_ids: ["ses_1"],
          archive_time: 10,
        })
      }
      if (request.method === "POST" && new URL(request.url).pathname.endsWith("/unarchive")) {
        return Response.json({
          root_id: "ses_1",
          restored_ids: ["ses_1", "ses_2"],
          still_archived_ids: [],
        })
      }
      if (request.method === "POST" && request.url.endsWith("/prompt_async"))
        return new Response(undefined, { status: 204 })
      if (request.method === "POST" && request.url.endsWith("/prompt")) {
        return Response.json({
          admittedSeq: 1,
          id: "msg_1",
          sessionID: "ses_1",
          timeCreated: 1,
          type: "user",
          data: { text: "hello" },
          delivery: "steer",
        })
      }
      if (request.method === "GET" && new URL(request.url).pathname === "/vcs")
        return Response.json(responses?.vcs ?? {})
      if (request.method === "GET" && new URL(request.url).pathname === "/session")
        return Response.json(responses?.sessions ?? [])
      if (request.method === "GET") return Response.json([])
      return new Response(undefined, { status: 204 })
    },
    { preconnect: globalThis.fetch.preconnect },
  )
  const server = { url: "http://localhost:4096" }
  const api = createCompatibleApi({
    protocol: typeof protocol === "string" ? Promise.resolve(protocol) : protocol,
    current: createApiForServer({ server, fetch: fetcher }),
    legacy: (directory) => createSdkForServer({ server, fetch: fetcher, directory, throwOnError: true }),
    directory: "/repo",
  })
  return { api, requests }
}

describe("createCompatibleApi", () => {
  test("routes V1 archive through the legacy session update", async () => {
    const { api, requests } = setup("v1")
    const result = await api.session.archive({ sessionID: "ses_1", directory: "/repo" })

    const url = new URL(requests[0]!.url)
    expect(url.pathname).toBe("/session/ses_1")
    expect(requests[0]!.headers.get("x-opencode-directory")).toBe("%2Frepo")
    expect(requests[0]!.method).toBe("PATCH")
    expect(await requests[0]!.json()).toMatchObject({ time: { archived: expect.any(Number) } })
    expect(result.archived_ids).toEqual(["ses_1"])
  })

  test("marks V1 family preview unavailable without calling the server", async () => {
    const { api, requests } = setup("v1")

    const error = await api.session
      .archivePreview({ sessionID: "ses_1", directory: "/repo" })
      .then(() => undefined)
      .catch((cause) => cause)

    expect(requests).toHaveLength(0)
    expect(error).toBeInstanceOf(Error)
    expect((error as Error).cause).toMatchObject({
      status: 404,
      body: { _tag: "FeatureDisabledError", feature: "familyArchive" },
    })
  })

  test("routes V1 unarchive through the legacy session update", async () => {
    const { api, requests } = setup("v1")

    await api.session.unarchive({ sessionID: "ses_1", directory: "/repo" })

    expect(new URL(requests[0]!.url).pathname).toBe("/session/ses_1")
    expect(requests[0]!.method).toBe("PATCH")
    expect(await requests[0]!.json()).toEqual({ time: { archived: null } })
  })

  test("converts current prompts to the V1 prompt contract", async () => {
    const { api, requests } = setup("v1")
    await api.session.prompt({
      sessionID: "ses_1",
      id: "msg_1",
      text: "hello @src/index.ts",
      agent: "build",
      model: { providerID: "provider", modelID: "model" },
      files: [
        { uri: "file:///repo/src/index.ts", name: "index.ts", mention: { text: "@src/index.ts", start: 6, end: 19 } },
        { uri: "data:text/plain;base64,aGVsbG8=", name: "notes.txt" },
      ],
    })

    expect(new URL(requests[0]!.url).pathname).toBe("/session/ses_1/prompt_async")
    const body = await requests[0]!.json()
    expect(body).toMatchObject({
      messageID: "msg_1",
      agent: "build",
      model: { providerID: "provider", modelID: "model" },
      parts: [
        { type: "text", text: "hello @src/index.ts" },
        {
          type: "file",
          mime: "text/plain",
          url: "file:///repo/src/index.ts",
          filename: "index.ts",
          source: {
            type: "file",
            text: { value: "@src/index.ts", start: 6, end: 19 },
            path: "file:///repo/src/index.ts",
          },
        },
        {
          type: "file",
          mime: "text/plain",
          url: "data:text/plain;base64,aGVsbG8=",
          filename: "notes.txt",
        },
      ],
    })
    expect(body.parts[2]).not.toHaveProperty("source")
  })

  test("preserves original parts for V1 optimistic reconciliation", async () => {
    const { api, requests } = setup("v1")
    await api.session.prompt({
      sessionID: "ses_1",
      id: "msg_1",
      text: "look",
      files: [{ uri: "data:image/png;base64,AAAA", name: "image.png" }],
      legacyParts: [
        { id: "prt_text", type: "text", text: "look" },
        { id: "prt_image", type: "file", mime: "image/png", url: "data:image/png;base64,AAAA", filename: "image.png" },
      ],
    })

    expect((await requests[0]!.json()).parts).toEqual([
      { id: "prt_text", type: "text", text: "look" },
      { id: "prt_image", type: "file", mime: "image/png", url: "data:image/png;base64,AAAA", filename: "image.png" },
    ])
  })

  test("resolves protocol detection once across implementation methods", async () => {
    let detections = 0
    const resolved = Promise.resolve<"v1" | "v2">("v2")
    const protocol = new Proxy(resolved, {
      get(target, property) {
        if (property !== "then") return Reflect.get(target, property, target)
        detections++
        return target.then.bind(target)
      },
    })
    const { api } = setup(protocol)

    await api.session.list()
    await api.session.list()

    expect(detections).toBe(1)
  })

  test("previews a V2 family through the generated archive route", async () => {
    const { api, requests } = setup("v2")

    const preview = await api.session.archivePreview({ sessionID: "ses_1" })

    expect(new URL(requests[0]!.url).pathname).toBe("/session/ses_1/archive-preview")
    expect(requests[0]!.method).toBe("GET")
    expect(preview).toMatchObject({ descendant_count: 2, active_count: 1 })
  })

  test("keeps V2 family archive on the generated archive route", async () => {
    const { api, requests } = setup("v2")

    const result = await api.session.archive({ sessionID: "ses_1" })

    expect(new URL(requests[0]!.url).pathname).toBe("/session/ses_1/archive")
    expect(requests[0]!.method).toBe("POST")
    expect(result.archived_ids).toEqual(["ses_1", "ses_2"])
  })

  test("keeps V2 family unarchive on the generated unarchive route", async () => {
    const { api, requests } = setup("v2")

    const result = await api.session.unarchive({ sessionID: "ses_1" })

    expect(new URL(requests[0]!.url).pathname).toBe("/session/ses_1/unarchive")
    expect(requests[0]!.method).toBe("POST")
    expect(result.restored_ids).toEqual(["ses_1", "ses_2"])
  })

  test("preserves family archive fields through the V1 session projection", async () => {
    const { api } = setup("v1", {
      sessions: [
        {
          id: "ses_1",
          slug: "ses_1",
          projectID: "project",
          directory: "/repo",
          title: "Session",
          version: "1",
          time: { created: 1, updated: 1 },
          familyArchiveID: "family-1",
          archiveCause: "cascade",
          archiveState: "archiving",
          archiveStateAt: 4,
          familyMemberCount: 3,
        },
      ],
    })

    const result = await api.session.list({ directory: "/repo" })

    expect(result.data[0]).toMatchObject({
      familyArchiveID: "family-1",
      archiveCause: "cascade",
      archiveState: "archiving",
      archiveStateAt: 4,
      familyMemberCount: 3,
    })
  })

  test("uses the global V1 session search endpoint", async () => {
    const { api, requests } = setup("v1")
    await api.session.list({ parentID: null, search: "session", limit: 50 })

    expect(new URL(requests[0]!.url).pathname).toBe("/experimental/session")
  })

  /*
  test("projects the V1 default branch", async () => {
    const { api } = setup("v1", { vcs: { branch: "feature", default_branch: "dev" } })

    expect(await api.vcs.get({ location: { directory: "/repo" } })).toMatchObject({
      data: { branch: "feature", defaultBranch: "dev" },
    })
  })
  */

  test("translates current file searches to the V1 dirs parameter", async () => {
    const { api, requests } = setup("v1")
    await api.file.find({ location: { directory: "/repo" }, query: "src", type: "file", limit: 20 })

    const url = new URL(requests[0]!.url)
    expect(url.pathname).toBe("/find/file")
    expect(url.searchParams.get("dirs")).toBe("false")
    expect(url.searchParams.get("limit")).toBe("20")
  })

  test("routes V1 permission replies through the requested directory", async () => {
    const { api, requests } = setup("v1")
    await api.permission.reply({
      sessionID: "ses_1",
      requestID: "permission_1",
      reply: "once",
      location: { directory: "/other" },
    })

    expect(new URL(requests[0]!.url).pathname).toBe("/session/ses_1/permissions/permission_1")
    expect(new URL(requests[0]!.url).searchParams.get("directory")).toBe("/other")
  })

  test("disposes the V1 instance after connecting a provider", async () => {
    const { api, requests } = setup("v1")

    await api.integration.connect.key({
      integrationID: "openrouter",
      key: "secret",
      location: { directory: "/repo" },
    })

    expect(requests.map((request) => new URL(request.url).pathname)).toEqual([
      "/auth/openrouter",
      "/instance/dispose",
      "/instance/dispose",
    ])
    expect(requests[1]!.headers.get("x-opencode-directory")).toBe("%2Frepo")
    expect(requests[2]!.headers.get("x-opencode-directory")).toBeNull()
  })

  test("disposes the V1 instance after completing provider OAuth", async () => {
    const { api, requests } = setup("v1")

    await api.integration.oauth.complete({
      integrationID: "openrouter",
      attemptID: "openrouter:0",
      code: "code",
      location: { directory: "/repo" },
    })

    expect(requests.map((request) => new URL(request.url).pathname)).toEqual([
      "/provider/openrouter/oauth/callback",
      "/instance/dispose",
      "/instance/dispose",
    ])
    expect(requests[1]!.headers.get("x-opencode-directory")).toBe("%2Frepo")
    expect(requests[2]!.headers.get("x-opencode-directory")).toBeNull()
  })
})
