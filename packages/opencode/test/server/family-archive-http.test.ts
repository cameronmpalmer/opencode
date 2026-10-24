import { describe, expect } from "bun:test"
import { NodeHttpServer, NodeServices } from "@effect/platform-node"
import { Config, ConfigProvider, Effect, Layer } from "effect"
import { HttpClient, HttpClientRequest, HttpRouter, HttpServer } from "effect/unstable/http"
import { layerWebSocketConstructorGlobal } from "effect/unstable/socket/Socket"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import { SessionArchiveGuardTable } from "@opencode-ai/core/session/sql"
import { InstanceBootstrap as InstanceBootstrapService } from "../../src/project/bootstrap-service"
import { InstanceStore } from "../../src/project/instance-store"
import { Project } from "../../src/project/project"
import { Session } from "@/session/session"
import { SessionPaths } from "../../src/server/routes/instance/httpapi/groups/session"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const noopBootstrapLayer = Layer.succeed(
  InstanceBootstrapService.Service,
  InstanceBootstrapService.Service.of({ run: Effect.void }),
)

const servedRoutes: Layer.Layer<never, Config.ConfigError, HttpServer.HttpServer> = HttpRouter.serve(
  HttpApiApp.routes,
  { disableListenLog: true, disableLogger: true },
)
const httpApiLayer = servedRoutes.pipe(
  Layer.provide(layerWebSocketConstructorGlobal),
  Layer.provideMerge(NodeHttpServer.layerTest),
  Layer.provideMerge(NodeServices.layer),
)

const appLayer = AppNodeBuilder.build(
  LayerNode.group([InstanceStore.node, Project.node, Session.node, Database.node, CrossSpawnSpawner.node]),
  [[InstanceStore.bootstrapNode, noopBootstrapLayer]],
)

const suite = (familyArchive: boolean) =>
  testEffect(
    Layer.mergeAll(appLayer, httpApiLayer).pipe(
      Layer.provide(
        ConfigProvider.layer(
          ConfigProvider.fromUnknown({
            OPENCODE_EXPERIMENTAL_FAMILY_ARCHIVE: familyArchive ? "true" : "false",
          }),
        ),
      ),
    ),
  )

const enabled = suite(true)
const disabled = suite(false)

const request = (path: string, init?: RequestInit) => {
  const url = new URL(path, "http://localhost")
  return HttpClientRequest.fromWeb(new Request(url, init)).pipe(
    HttpClientRequest.setUrl(url.pathname),
    HttpClient.execute,
  )
}

const pathFor = (path: string, params: Record<string, string>) =>
  Object.entries(params).reduce((result, [key, value]) => result.replace(`:${key}`, value), path)

const post = (path: string, headers: Record<string, string>, body?: unknown) =>
  request(path, {
    method: "POST",
    headers: body === undefined ? headers : { ...headers, "content-type": "application/json" },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  })
const patch = (path: string, headers: Record<string, string>, body: unknown) =>
  request(path, {
    method: "PATCH",
    headers: { ...headers, "content-type": "application/json" },
    body: JSON.stringify(body),
  })

const seedFamily = (session: Session.Interface) =>
  Effect.gen(function* () {
    const parent = yield* session.create({ title: "parent" })
    const child = yield* session.create({ parentID: parent.id, title: "child" })
    return { parent, child }
  })

describe("family archive HTTP API", () => {
  enabled.instance("previews, archives, and unarchives a family", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const headers = { "x-opencode-directory": test.directory }
      const session = yield* Session.Service
      const family = yield* seedFamily(session)

      const previewResponse = yield* request(pathFor(SessionPaths.archivePreview, { sessionID: family.parent.id }), {
        headers,
      })
      expect(previewResponse.status).toBe(200)
      const preview = (yield* previewResponse.json) as Record<string, unknown>
      expect(Object.keys(preview).sort()).toEqual(
        ["active_count", "active_session_ids", "descendant_count", "root_id"].sort(),
      )
      expect(preview.root_id).toBe(family.parent.id)
      expect(preview.descendant_count).toBe(1)
      expect(preview.active_count).toBe(0)

      const archiveResponse = yield* post(pathFor(SessionPaths.archive, { sessionID: family.parent.id }), headers)
      expect(archiveResponse.status).toBe(200)
      const archived = (yield* archiveResponse.json) as Record<string, unknown>
      expect(Object.keys(archived).sort()).toEqual(
        ["already_archived_ids", "archive_time", "archived_ids", "cancelled_ids", "root_id"].sort(),
      )
      expect(archived.root_id).toBe(family.parent.id)
      expect(archived.archived_ids).toEqual(expect.arrayContaining([family.parent.id, family.child.id]))
      expect(archived.already_archived_ids).toEqual([])
      expect(archived.cancelled_ids).toEqual([])

      const unarchiveResponse = yield* post(pathFor(SessionPaths.unarchive, { sessionID: family.parent.id }), headers)
      expect(unarchiveResponse.status).toBe(200)
      const restored = (yield* unarchiveResponse.json) as Record<string, unknown>
      expect(restored.restored_ids).toEqual(expect.arrayContaining([family.parent.id, family.child.id]))
      expect((yield* session.get(family.child.id)).time.archived).toBeUndefined()
    }),
  )

  enabled.instance("PATCH numeric archives the family, PATCH null unarchives, and time {} is a no-op", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const headers = { "x-opencode-directory": test.directory }
      const session = yield* Session.Service
      const family = yield* seedFamily(session)

      const archiveResponse = yield* patch(pathFor(SessionPaths.update, { sessionID: family.parent.id }), headers, {
        time: { archived: family.parent.time.created },
      })
      expect(archiveResponse.status).toBe(200)
      expect((yield* session.get(family.child.id)).time.archived).not.toBeUndefined()
      expect((yield* session.get(family.child.id)).archiveCause).toBe("cascade")

      const noopResponse = yield* patch(pathFor(SessionPaths.update, { sessionID: family.parent.id }), headers, {
        time: {},
      })
      expect(noopResponse.status).toBe(200)
      expect((yield* session.get(family.parent.id)).time.archived).not.toBeUndefined()

      const unarchiveResponse = yield* patch(pathFor(SessionPaths.update, { sessionID: family.parent.id }), headers, {
        time: { archived: null },
      })
      expect(unarchiveResponse.status).toBe(200)
      expect((yield* session.get(family.parent.id)).time.archived).toBeUndefined()
      expect((yield* session.get(family.child.id)).time.archived).toBeUndefined()
    }),
  )

  enabled.instance("preview of a cascade member returns 409 with the family root", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const headers = { "x-opencode-directory": test.directory }
      const session = yield* Session.Service
      const family = yield* seedFamily(session)
      yield* session.archiveSubtree({ sessionID: family.parent.id })

      const response = yield* request(pathFor(SessionPaths.archivePreview, { sessionID: family.child.id }), {
        headers,
      })
      expect(response.status).toBe(409)
      const body = (yield* response.json) as Record<string, unknown>
      expect(body._tag).toBe("SessionFamilyMemberError")
      expect(body.familyRootID).toBe(family.parent.id)
    }),
  )

  enabled.instance("rejects cancel_active false and accepts no body, {}, or true", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const headers = { "x-opencode-directory": test.directory }
      const session = yield* Session.Service

      const rejectedFamily = yield* seedFamily(session)
      const rejected = yield* post(pathFor(SessionPaths.archive, { sessionID: rejectedFamily.parent.id }), headers, {
        cancel_active: false,
      })
      expect(rejected.status).toBe(400)

      for (const body of [undefined, {}, { cancel_active: true }]) {
        const family = yield* seedFamily(session)
        const response = yield* post(pathFor(SessionPaths.archive, { sessionID: family.parent.id }), headers, body)
        expect(response.status).toBe(200)
      }
    }),
  )
  enabled.instance("a cascade member returns 409 with the family root", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const headers = { "x-opencode-directory": test.directory }
      const session = yield* Session.Service
      const family = yield* seedFamily(session)
      yield* session.archiveSubtree({ sessionID: family.parent.id })

      const response = yield* post(pathFor(SessionPaths.unarchive, { sessionID: family.child.id }), headers)
      expect(response.status).toBe(409)
      const body = (yield* response.json) as Record<string, unknown>
      expect(body._tag).toBe("SessionFamilyMemberError")
      expect(body.familyRootID).toBe(family.parent.id)
    }),
  )

  disabled.instance("preview and archive return FeatureDisabled while unarchive still works", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const headers = { "x-opencode-directory": test.directory }
      const session = yield* Session.Service
      const family = yield* seedFamily(session)

      const previewResponse = yield* request(pathFor(SessionPaths.archivePreview, { sessionID: family.parent.id }), {
        headers,
      })
      expect(previewResponse.status).toBe(404)
      const previewBody = (yield* previewResponse.json) as Record<string, unknown>
      expect(previewBody._tag).toBe("FeatureDisabledError")

      const archiveResponse = yield* post(pathFor(SessionPaths.archive, { sessionID: family.parent.id }), headers)
      expect(archiveResponse.status).toBe(404)

      yield* session.archiveSubtree({ sessionID: family.parent.id })
      const unarchiveResponse = yield* post(pathFor(SessionPaths.unarchive, { sessionID: family.parent.id }), headers)
      expect(unarchiveResponse.status).toBe(200)
      expect((yield* session.get(family.child.id)).time.archived).toBeUndefined()
    }),
  )

  disabled.instance("PATCH numeric archives only the session and detaches its family", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const headers = { "x-opencode-directory": test.directory }
      const session = yield* Session.Service
      const family = yield* seedFamily(session)

      const response = yield* patch(pathFor(SessionPaths.update, { sessionID: family.parent.id }), headers, {
        time: { archived: family.parent.time.created },
      })
      expect(response.status).toBe(200)

      const parent = yield* session.get(family.parent.id)
      expect(parent.time.archived).not.toBeUndefined()
      expect(parent.archiveCause).toBe("user")
      expect(parent.familyArchiveID).toBeUndefined()
      expect((yield* session.get(family.child.id)).time.archived).toBeUndefined()

      const { db } = yield* Database.Service
      const guards = yield* db.select().from(SessionArchiveGuardTable).all().pipe(Effect.orDie)
      expect(guards).toEqual([])
    }),
  )

  disabled.instance("PATCH null unarchives the family and rejects a cascade member", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const headers = { "x-opencode-directory": test.directory }
      const session = yield* Session.Service
      const family = yield* seedFamily(session)
      yield* session.archiveSubtree({ sessionID: family.parent.id })

      const memberResponse = yield* patch(pathFor(SessionPaths.update, { sessionID: family.child.id }), headers, {
        time: { archived: null },
      })
      expect(memberResponse.status).toBe(409)
      const memberBody = (yield* memberResponse.json) as Record<string, unknown>
      expect(memberBody._tag).toBe("SessionFamilyMemberError")

      const rootResponse = yield* patch(pathFor(SessionPaths.update, { sessionID: family.parent.id }), headers, {
        time: { archived: null },
      })
      expect(rootResponse.status).toBe(200)
      expect((yield* session.get(family.parent.id)).time.archived).toBeUndefined()
      expect((yield* session.get(family.child.id)).time.archived).toBeUndefined()
    }),
  )
})
