import { describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import {
  MessageTable,
  PartTable,
  SessionArchiveGuardTable,
  SessionImportExemptTable,
  SessionTable,
} from "@opencode-ai/core/session/sql"
import { Database } from "@opencode-ai/core/database/database"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { eq } from "drizzle-orm"
import { Effect } from "effect"
import { Project } from "@/project/project"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Session as SessionNs } from "@/session/session"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { importExemptionMs, writeImport, type ExportData } from "@/cli/cmd/import"
import { testEffect } from "../lib/effect"

const it = testEffect(
  LayerNode.compile(
    LayerNode.group([SessionNs.node, SessionProjector.node, Project.node, CrossSpawnSpawner.node, Database.node]),
    [[SessionExecution.node, SessionExecution.noopLayer]],
  ),
)

const archivedInfo = (base: SessionNs.Info, input: { id: SessionID; archivedAt: number }): SessionNs.Info => ({
  ...base,
  id: input.id,
  time: { ...base.time, archived: input.archivedAt },
  familyArchiveID: input.id,
  archiveCause: "user",
  archiveState: null,
  archiveStateAt: null,
})

const noImportState = (db: Database.Interface["db"]) =>
  Effect.gen(function* () {
    const exempt = yield* db.select().from(SessionImportExemptTable).all().pipe(Effect.orDie)
    const guards = yield* db.select().from(SessionArchiveGuardTable).all().pipe(Effect.orDie)
    expect(exempt).toEqual([])
    expect(guards).toEqual([])
  })

describe("family archive import", () => {
  it.instance("imports an archived session with its family fields and leaves no exemptions", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const { db } = yield* Database.Service
      const base = yield* session.create({ title: "base" })
      const id = SessionID.descending()
      const info = archivedInfo(base, { id, archivedAt: 12_345 })

      yield* writeImport(db, info, [])

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, id)).get().pipe(Effect.orDie)
      expect(row?.time_archived).toBe(12_345)
      expect(row?.family_archive_id).toBe(id)
      expect(row?.archive_cause).toBe("user")
      expect(row?.archive_state).toBeNull()
      expect(row?.archive_state_at).toBeNull()
      yield* noImportState(db)
    }),
  )

  it.instance("restores archive fields onto an existing row whose live archive fields are empty", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const { db } = yield* Database.Service
      const existing = yield* session.create({ title: "existing" })
      const info = archivedInfo(existing, { id: existing.id, archivedAt: 4_242 })

      yield* writeImport(db, info, [])

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, existing.id)).get().pipe(Effect.orDie)
      expect(row?.time_archived).toBe(4_242)
      expect(row?.family_archive_id).toBe(existing.id)
      expect(row?.archive_cause).toBe("user")
      yield* noImportState(db)
    }),
  )

  it.instance("never overwrites a newer live archive with a stale export", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const { db } = yield* Database.Service
      const live = yield* session.create({ title: "live" })
      yield* db
        .update(SessionTable)
        .set({ time_archived: 9_999, family_archive_id: live.id, archive_cause: "user" })
        .where(eq(SessionTable.id, live.id))
        .run()
        .pipe(Effect.orDie)
      const stale = archivedInfo(live, { id: live.id, archivedAt: 1_111 })

      yield* writeImport(db, stale, [])

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, live.id)).get().pipe(Effect.orDie)
      expect(row?.time_archived).toBe(9_999)
      expect(row?.family_archive_id).toBe(live.id)
      expect(row?.archive_cause).toBe("user")
      yield* noImportState(db)
    }),
  )

  it.effect("defaults the import exemption budget to thirty minutes", () =>
    Effect.sync(() => {
      expect(importExemptionMs({})).toBe(1_800_000)
      expect(importExemptionMs({ OPENCODE_SESSION_IMPORT_EXEMPTION_MS: "5000" })).toBe(5_000)
    }),
  )

  it.instance("imports messages and parts for an archived session", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const { db } = yield* Database.Service
      const source = yield* session.create({ title: "source" })
      const message = yield* session.updateMessage({
        id: MessageID.ascending(),
        role: "user",
        sessionID: source.id,
        agent: "build",
        model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
        time: { created: Date.now() },
      })
      const part = yield* session.updatePart({
        id: PartID.ascending(),
        sessionID: source.id,
        messageID: message.id,
        type: "text",
        text: "imported text",
      })
      const id = SessionID.descending()
      const info = archivedInfo(source, { id, archivedAt: 3_000 })
      const importedMessage = { ...message, id: MessageID.ascending(), sessionID: id }
      const importedPart = {
        ...part,
        id: PartID.ascending(),
        sessionID: id,
        messageID: importedMessage.id,
      }
      const messages = [{ info: importedMessage, parts: [importedPart] }] as unknown as ExportData["messages"]

      yield* writeImport(db, info, messages)

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, id)).get().pipe(Effect.orDie)
      expect(row?.time_archived).toBe(3_000)
      const storedMessage = yield* db
        .select()
        .from(MessageTable)
        .where(eq(MessageTable.session_id, id))
        .get()
        .pipe(Effect.orDie)
      expect(storedMessage?.id).toBe(importedMessage.id)
      const storedParts = yield* db
        .select()
        .from(PartTable)
        .where(eq(PartTable.session_id, id))
        .all()
        .pipe(Effect.orDie)
      expect(storedParts.map((item) => item.id)).toEqual([importedPart.id])
      yield* noImportState(db)
    }),
  )

  it.instance("imports an archived child under an archived parent", () =>
    Effect.gen(function* () {
      const session = yield* SessionNs.Service
      const { db } = yield* Database.Service
      const parent = yield* session.create({ title: "parent" })
      yield* db
        .update(SessionTable)
        .set({ time_archived: 7_777, family_archive_id: parent.id, archive_cause: "user" })
        .where(eq(SessionTable.id, parent.id))
        .run()
        .pipe(Effect.orDie)

      const id = SessionID.descending()
      const info: SessionNs.Info = {
        ...parent,
        id,
        parentID: parent.id,
        time: { ...parent.time, archived: 7_777 },
        familyArchiveID: parent.id,
        archiveCause: "cascade",
        archiveState: null,
        archiveStateAt: null,
      }

      yield* writeImport(db, info, [])

      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, id)).get().pipe(Effect.orDie)
      expect(row?.parent_id).toBe(parent.id)
      expect(row?.time_archived).toBe(7_777)
      expect(row?.family_archive_id).toBe(parent.id)
      expect(row?.archive_cause).toBe("cascade")
      yield* noImportState(db)
    }),
  )
})
