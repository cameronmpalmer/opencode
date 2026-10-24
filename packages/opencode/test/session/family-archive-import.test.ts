import { describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionArchiveGuardTable, SessionImportExemptTable, SessionTable } from "@opencode-ai/core/session/sql"
import { Database } from "@opencode-ai/core/database/database"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { eq } from "drizzle-orm"
import { Effect } from "effect"
import { Project } from "@/project/project"
import { Session as SessionNs } from "@/session/session"
import { SessionID } from "@/session/schema"
import { importExemptionMs, writeImport } from "@/cli/cmd/import"
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
})
