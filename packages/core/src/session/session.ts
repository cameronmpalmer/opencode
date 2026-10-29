export * as Session from "./session.js"

import { DateTime, Effect, Exit, Fiber, Scope } from "effect"
import { and, eq, inArray } from "drizzle-orm"
import type { Agent } from "@opencode/schema/agent"
import type { Model } from "@opencode/schema/model"
import type { Permission } from "@opencode/schema/permission"
import { Event } from "@opencode/schema/event"
import { FSUtil } from "@opencode/util/fs-util"
import { Bus } from "../bus.js"
import { Database } from "../database/database.js"
import { Instance } from "../instance/service.js"
import { ShellResult } from "../shell/result.js"
import type { Skill } from "../skill.js"
import {
  ArchiveBlockedError,
  ArchiveInProgressError,
  BusyError,
  CompactionConflictError,
  FamilyMemberError,
  InboxConflictError,
  InvalidArchiveTimeError,
  NotFoundError,
  PromptConflictError,
  SessionArchivedError,
  SyntheticConflictError,
} from "./error.js"
import { SessionEvent } from "./event.js"
import { SessionExecution } from "./execution.js"
import { isDirectFamilyMember, liveLeases, loadSubtree, orderSubtree } from "./family-archive.js"
import { SessionInbox } from "./inbox.js"
import { SessionMessage } from "./message.js"
import { SessionPrompt } from "./prompt.js"
import { SessionRevert } from "./revert.js"
import { SessionShell } from "./shell.js"
import { SessionSkill } from "./skill.js"
import { SessionSchema } from "./schema.js"
import { SessionStore } from "./store.js"
import { FamilyArchiveMigrationTable, SessionArchiveGuardTable, SessionRunLeaseTable, SessionTable } from "./sql.js"

type PromptRequest = SessionPrompt.Input & {
  id?: SessionMessage.ID
  resume?: boolean
}

/**
 * Build once in the host Scope: `const sessions = yield* Session.make()`.
 * Use `sessions.forSession(id)` for handles that share host services and reload current state.
 */
export const make = Effect.fn("Session.make")(function* () {
  const bus = yield* Bus.Service
  const database = yield* Database.Service
  const store = yield* SessionStore.Service
  const instances = yield* Instance.Service
  const execution = yield* SessionExecution.Service
  const admission = yield* SessionInbox.Service
  const fs = yield* FSUtil.Service
  const scope = yield* Scope.Scope

  const get = Effect.fn("Session.get")(function* (sessionID: SessionSchema.ID) {
    const session = yield* store.get(sessionID)
    if (!session) return yield* new NotFoundError({ sessionID })
    return session
  })
  const assertWritable = (session: SessionSchema.Info) =>
    session.time.archived !== undefined || session.archiveState === "archiving"
      ? Effect.die(new SessionArchivedError({ sessionID: session.id, message: `Session ${session.id} is archived` }))
      : Effect.void
  const message = Effect.fn("Session.message")(function* (sessionID: SessionSchema.ID, messageID: SessionMessage.ID) {
    const stored = yield* store.message(messageID)
    return stored?.sessionID === sessionID ? stored.message : undefined
  })
  const view = Effect.fn("Session.view")(function* (sessionID: SessionSchema.ID, input: { idle: number }) {
    const session = yield* get(sessionID)
    if (
      session.time.idle === undefined ||
      input.idle > DateTime.toEpochMillis(session.time.idle) ||
      (session.time.viewed !== undefined && DateTime.toEpochMillis(session.time.viewed) >= input.idle)
    )
      return
    yield* bus.publish(SessionEvent.Viewed, { sessionID, idle: input.idle })
  })
  const rename = Effect.fn("Session.rename")(function* (sessionID: SessionSchema.ID, input: { title: string }) {
    const session = yield* get(sessionID)
    yield* assertWritable(session)
    yield* bus.publish(SessionEvent.Renamed, { sessionID, title: input.title })
  })
  const guardRows = (ids: SessionSchema.ID[]) =>
    database.db
      .transaction((tx) =>
        tx
          .insert(SessionArchiveGuardTable)
          .values(ids.map((id) => ({ session_id: id })))
          .onConflictDoNothing()
          .run(),
      )
      .pipe(Effect.orDie)
  const releaseRows = (ids: SessionSchema.ID[]) =>
    database.db
      .transaction((tx) =>
        tx.delete(SessionArchiveGuardTable).where(inArray(SessionArchiveGuardTable.session_id, ids)).run(),
      )
      .pipe(Effect.orDie)
  const archive = Effect.fn("Session.archive")(function* (sessionID: SessionSchema.ID) {
    const session = yield* get(sessionID)
    if (session.time.archived) return
    yield* guardRows([sessionID])
    yield* bus
      .publish(SessionEvent.Archived, { sessionID })
      .pipe(Effect.ensuring(releaseRows([sessionID])))
  })
  const unarchive = Effect.fn("Session.unarchive")(function* (sessionID: SessionSchema.ID) {
    const session = yield* get(sessionID)
    if (!session.time.archived) return
    yield* guardRows([sessionID])
    yield* bus
      .publish(SessionEvent.Unarchived, { sessionID })
      .pipe(Effect.ensuring(releaseRows([sessionID])))
  })
  const archivePreview = Effect.fn("Session.archivePreview")(function* (sessionID: SessionSchema.ID) {
    const root = yield* database.db
      .select()
      .from(SessionTable)
      .where(eq(SessionTable.id, sessionID))
      .get()
      .pipe(Effect.orDie)
    if (!root) return yield* new NotFoundError({ sessionID })
    if (isDirectFamilyMember(root)) {
      return yield* new FamilyMemberError({
        sessionID: root.id,
        familyRootID: root.family_archive_id!,
        message: `Session ${root.id} belongs to family ${root.family_archive_id}; preview the family root instead`,
      })
    }
    const rows = orderSubtree(yield* loadSubtree(database.db, root.id))
    const ids = rows.map((row) => row.id)
    const leases = yield* liveLeases(database.db, ids)
    const active = new Set<SessionSchema.ID>(leases.map((lease) => lease.session_id))
    for (const id of ids) {
      if (yield* execution.isActive(id)) active.add(id)
    }
    const activeIDs = [...active].filter((id) => ids.includes(id)).sort()
    return {
      root_id: root.id,
      descendant_count: Math.max(rows.length - 1, 0),
      active_count: activeIDs.length,
      active_session_ids: activeIDs,
    }
  })
  const beginArchive = (ids: SessionSchema.ID[], marked: SessionSchema.ID[], at: number) =>
    database.db
      .transaction((tx) =>
        Effect.gen(function* () {
          yield* tx
            .insert(SessionArchiveGuardTable)
            .values(ids.map((id) => ({ session_id: id })))
            .onConflictDoNothing()
            .run()
          if (marked.length > 0) {
            yield* tx
              .update(SessionTable)
              .set({ archive_state: "archiving", archive_state_at: at })
              .where(inArray(SessionTable.id, marked))
              .run()
          }
        }),
      )
      .pipe(Effect.orDie)
  const clearArchive = (
    ids: SessionSchema.ID[],
    leases: (typeof SessionRunLeaseTable.$inferSelect)[],
    rootID: SessionSchema.ID,
    rootWasArchived: boolean,
  ) =>
    database.db
      .transaction((tx) =>
        Effect.gen(function* () {
          const current = yield* tx.select().from(SessionTable).where(inArray(SessionTable.id, ids)).all()
          const marked = current.filter((row) => row.archive_state === "archiving").map((row) => row.id)
          if (marked.length > 0) {
            yield* tx
              .update(SessionTable)
              .set({ archive_state: null, archive_state_at: null })
              .where(inArray(SessionTable.id, marked))
              .run()
          }
          yield* tx.delete(SessionArchiveGuardTable).where(inArray(SessionArchiveGuardTable.session_id, ids)).run()
          if (!rootWasArchived) {
            yield* tx
              .delete(FamilyArchiveMigrationTable)
              .where(eq(FamilyArchiveMigrationTable.root_session_id, rootID))
              .run()
          }
          for (const lease of leases) {
            yield* tx
              .update(SessionRunLeaseTable)
              .set({ cancel_requested: lease.cancel_requested })
              .where(
                and(
                  eq(SessionRunLeaseTable.session_id, lease.session_id),
                  eq(SessionRunLeaseTable.lease_id, lease.lease_id),
                ),
              )
              .run()
          }
        }),
      )
      .pipe(Effect.orDie)
  const setArchived = Effect.fn("Session.setArchived")(function* (sessionID: SessionSchema.ID, time?: number) {
    const row = yield* database.db
      .select()
      .from(SessionTable)
      .where(eq(SessionTable.id, sessionID))
      .get()
      .pipe(Effect.orDie)
    if (!row) return yield* new NotFoundError({ sessionID })
    if (isDirectFamilyMember(row)) {
      return yield* new FamilyMemberError({
        sessionID: row.id,
        familyRootID: row.family_archive_id!,
        message: `Session ${row.id} belongs to family ${row.family_archive_id}; archive the family root instead`,
      })
    }
    yield* guardRows([sessionID])
    yield* bus
      .publish(SessionEvent.Archived, {
        sessionID,
        timeArchived: time ?? null,
        archiveCause: "user",
        familyArchiveID: null,
        archiveState: null,
        archiveStateAt: null,
      })
      .pipe(Effect.ensuring(releaseRows([sessionID])))
  })
  const archiveSubtree = Effect.fn("Session.archiveSubtree")(function* (
    sessionID: SessionSchema.ID,
    options: { archivedAt?: number; waitTimeoutMs?: number } = {},
  ) {
    const now = Date.now()
    const root = yield* database.db
      .select()
      .from(SessionTable)
      .where(eq(SessionTable.id, sessionID))
      .get()
      .pipe(Effect.orDie)
    if (!root) return yield* new NotFoundError({ sessionID })
    if (isDirectFamilyMember(root)) {
      return yield* new FamilyMemberError({
        sessionID: root.id,
        familyRootID: root.family_archive_id!,
        message: `Session ${root.id} belongs to family ${root.family_archive_id}; archive the family root instead`,
      })
    }
    const rows = yield* loadSubtree(database.db, root.id)
    const archiving = rows.filter((row) => row.archive_state === "archiving")
    if (archiving.length > 0) {
      return yield* new ArchiveInProgressError({
        sessionID: root.id,
        sessionIDs: archiving.map((row) => row.id),
        message: `Archive already in progress for ${archiving.map((row) => row.id).join(", ")}`,
      })
    }
    const allArchived = rows.every((row) => row.time_archived !== null)
    const ids = rows.map((row) => row.id)
    const leases = yield* liveLeases(database.db, ids)
    let archiveTime = root.time_archived ?? now
    if (root.time_archived === null && options.archivedAt !== undefined) {
      if (options.archivedAt < root.time_created) {
        return yield* new InvalidArchiveTimeError({
          sessionID: root.id,
          time: options.archivedAt,
          message: `archive time ${options.archivedAt} precedes session creation ${root.time_created}`,
        })
      }
      archiveTime = Math.min(options.archivedAt, now)
    }
    if (allArchived && leases.length === 0) {
      return {
        root_id: root.id,
        archived_ids: [],
        already_archived_ids: rows.map((row) => row.id),
        cancelled_ids: [],
        archive_time: archiveTime,
      }
    }

    const marked = allArchived ? ids : rows.filter((row) => row.time_archived === null).map((row) => row.id)
    const cancelled = new Set<SessionSchema.ID>(leases.map((lease) => lease.session_id))
    for (const id of ids) {
      if (yield* execution.isActive(id)) cancelled.add(id)
    }
    const rootWasArchived = root.time_archived !== null

    const work = Effect.gen(function* () {
      yield* beginArchive(ids, marked, now)
      if (leases.length > 0) {
        yield* database.db
          .update(SessionRunLeaseTable)
          .set({ cancel_requested: 1 })
          .where(
            inArray(
              SessionRunLeaseTable.session_id,
              leases.map((lease) => lease.session_id),
            ),
          )
          .run()
          .pipe(Effect.orDie)
      }
      yield* Effect.forEach(
        [...cancelled],
        (id) => execution.interrupt(id).pipe(Effect.catchCause(() => Effect.void)),
        { concurrency: "unbounded", discard: true },
      )
      if (leases.length > 0) {
        const deadline = Date.now() + (options.waitTimeoutMs ?? 30_000)
        let remaining = yield* liveLeases(database.db, ids)
        while (remaining.length > 0 && Date.now() < deadline) {
          yield* Effect.sleep("25 millis")
          remaining = yield* liveLeases(database.db, ids)
        }
        if (remaining.length > 0) {
          const blocking = remaining.map((lease) => lease.session_id)
          return yield* new ArchiveBlockedError({
            sessionID: root.id,
            sessionIDs: blocking,
            message: `Active sessions did not stop: ${blocking.join(", ")}`,
          })
        }
      }

      const ordered = orderSubtree(yield* loadSubtree(database.db, root.id))
      const finalized = new Map<string, { familyKey: SessionSchema.ID; archiveTime: number }>()
      const archivedIDs: SessionSchema.ID[] = []
      const alreadyArchivedIDs: SessionSchema.ID[] = []
      for (const row of ordered) {
        let timeArchived = row.time_archived
        let familyArchiveID = row.family_archive_id
        let archiveCause = row.archive_cause
        if (row.time_archived === null) {
          const parent = row.parent_id ? finalized.get(row.parent_id) : undefined
          const familyKey = row.id === root.id ? root.id : (parent?.familyKey ?? root.id)
          timeArchived = row.id === root.id ? archiveTime : (parent?.archiveTime ?? archiveTime)
          familyArchiveID = familyKey
          archiveCause = row.id === root.id ? (row.archive_cause ?? "user") : "cascade"
          archivedIDs.push(row.id)
        } else {
          alreadyArchivedIDs.push(row.id)
          if (row.id === root.id) {
            familyArchiveID = row.family_archive_id ?? root.id
            archiveCause = row.archive_cause ?? "user"
          }
        }
        finalized.set(row.id, {
          familyKey: familyArchiveID ?? row.id,
          archiveTime: timeArchived!,
        })
        yield* bus.publish(SessionEvent.Archived, {
          sessionID: row.id,
          timeArchived,
          familyArchiveID,
          archiveCause,
          archiveState: null,
          archiveStateAt: null,
        })
      }

      yield* database.db
        .insert(FamilyArchiveMigrationTable)
        .values({ root_session_id: root.id, root_archived_at: archiveTime, applied_at: now })
        .onConflictDoUpdate({
          target: FamilyArchiveMigrationTable.root_session_id,
          set: { root_archived_at: archiveTime, applied_at: now },
        })
        .run()
        .pipe(Effect.orDie)

      yield* releaseRows(ids)
      return {
        root_id: root.id,
        archived_ids: archivedIDs,
        already_archived_ids: alreadyArchivedIDs,
        cancelled_ids: [...cancelled].sort(),
        archive_time: archiveTime,
      }
    })

    return yield* work.pipe(
      Effect.onExit((exit) =>
        Exit.isSuccess(exit)
          ? Effect.void
          : Effect.uninterruptible(
              clearArchive(ids, leases, root.id, rootWasArchived).pipe(
                Effect.catchCause((cause) => Effect.logError("family archive cleanup failed", { cause })),
              ),
            ),
      ),
    )
  })
  const unarchiveSubtree = Effect.fn("Session.unarchiveSubtree")(function* (sessionID: SessionSchema.ID) {
    const root = yield* database.db
      .select()
      .from(SessionTable)
      .where(eq(SessionTable.id, sessionID))
      .get()
      .pipe(Effect.orDie)
    if (!root) return yield* new NotFoundError({ sessionID })
    let missingFamilyRoot: SessionSchema.ID | undefined
    if (isDirectFamilyMember(root)) {
      const familyRoot = yield* database.db
        .select({ id: SessionTable.id })
        .from(SessionTable)
        .where(eq(SessionTable.id, root.family_archive_id!))
        .get()
        .pipe(Effect.orDie)
      if (familyRoot) {
        return yield* new FamilyMemberError({
          sessionID: root.id,
          familyRootID: root.family_archive_id!,
          message: `Session ${root.id} belongs to family ${root.family_archive_id}; unarchive the family root instead`,
        })
      }
      missingFamilyRoot = root.family_archive_id!
    }
    const rows = orderSubtree(yield* loadSubtree(database.db, root.id))
    const archiving = rows.filter((row) => row.archive_state === "archiving")
    if (archiving.length > 0) {
      return yield* new ArchiveInProgressError({
        sessionID: root.id,
        sessionIDs: archiving.map((row) => row.id),
        message: `Archive in progress for ${archiving.map((row) => row.id).join(", ")}`,
      })
    }
    const restore = rows.filter((row) => {
      if (row.time_archived === null) return false
      if (row.id === root.id) return true
      if (row.archive_cause !== "cascade" && row.archive_cause !== "migration") return false
      if (missingFamilyRoot) return row.family_archive_id === missingFamilyRoot
      return row.family_archive_id === root.id
    })
    const restored = new Set(restore.map((row) => row.id))
    if (restore.length > 0) {
      yield* Effect.gen(function* () {
        yield* guardRows(restore.map((row) => row.id))
        yield* Effect.forEach(restore, (row) => bus.publish(SessionEvent.Unarchived, { sessionID: row.id }), {
          discard: true,
        })
      }).pipe(Effect.ensuring(releaseRows(restore.map((row) => row.id))))
    }
    const stillArchived = rows.filter((row) => row.time_archived !== null && !restored.has(row.id)).map((row) => row.id)
    return {
      root_id: root.id,
      restored_ids: restore.map((row) => row.id),
      still_archived_ids: stillArchived,
    }
  })
  const setMetadata = Effect.fn("Session.setMetadata")(function* (
    sessionID: SessionSchema.ID,
    input: { metadata: SessionSchema.Metadata },
  ) {
    const session = yield* get(sessionID)
    yield* assertWritable(session)
    yield* bus.publish(SessionEvent.MetadataUpdated, { sessionID, metadata: input.metadata })
  })
  const setPermissions = Effect.fn("Session.setPermissions")(function* (
    sessionID: SessionSchema.ID,
    input: { permissions: Permission.Ruleset },
  ) {
    const session = yield* get(sessionID)
    yield* assertWritable(session)
    yield* bus.publish(SessionEvent.Permissions, { sessionID, permissions: input.permissions })
  })
  const switchAgent = Effect.fn("Session.switchAgent")(function* (
    sessionID: SessionSchema.ID,
    input: { agent: Agent.ID },
  ) {
    const session = yield* get(sessionID)
    yield* assertWritable(session)
    yield* bus.publish(SessionEvent.AgentSelected, { sessionID, agent: input.agent, previous: session.agent })
  })
  const switchModel = Effect.fn("Session.switchModel")(function* (
    sessionID: SessionSchema.ID,
    input: { model: Model.Ref },
  ) {
    const session = yield* get(sessionID)
    yield* assertWritable(session)
    if (
      session.model?.providerID === input.model.providerID &&
      session.model.id === input.model.id &&
      (session.model.variant ?? "default") === (input.model.variant ?? "default")
    )
      return
    yield* bus.publish(SessionEvent.ModelSelected, { sessionID, model: input.model, previous: session.model })
  })
  const mutatePending = (
    sessionID: SessionSchema.ID,
    inboxID: SessionMessage.ID,
    mutation: (input: {
      readonly id: SessionMessage.ID
      readonly sessionID: SessionSchema.ID
    }) => Effect.Effect<void, SessionInbox.LifecycleConflict>,
  ) =>
    mutation({ sessionID, id: inboxID }).pipe(
      Effect.catchTag("SessionInbox.LifecycleConflict", () =>
        Effect.gen(function* () {
          yield* get(sessionID)
          return yield* new InboxConflictError({ sessionID, inboxID })
        }),
      ),
    )

  const inbox = Effect.fn("Session.inbox")(function* (sessionID: SessionSchema.ID) {
    yield* get(sessionID)
    return yield* admission.list(sessionID)
  })
  const cancelInbox = Effect.fn("Session.cancelInbox")(
    (sessionID: SessionSchema.ID, inboxID: SessionMessage.ID) => mutatePending(sessionID, inboxID, admission.cancel),
    Effect.uninterruptible,
  )
  const steerInbox = Effect.fn("Session.steerInbox")(function* (
    sessionID: SessionSchema.ID,
    inboxID: SessionMessage.ID,
  ) {
    yield* mutatePending(sessionID, inboxID, admission.steer)
    yield* execution.wake(sessionID)
  }, Effect.uninterruptible)
  const queueInbox = Effect.fn("Session.queueInbox")(
    (sessionID: SessionSchema.ID, inboxID: SessionMessage.ID) => mutatePending(sessionID, inboxID, admission.queue),
    Effect.uninterruptible,
  )
  const prompt = Effect.fn("Session.prompt")((sessionID: SessionSchema.ID, input: PromptRequest) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const session = yield* get(sessionID)
        yield* assertWritable(session)
        const messageID = input.id ?? SessionMessage.ID.create()
        const admitted = yield* Effect.gen(function* () {
          const existing = yield* admission.reconcile({
            id: messageID,
            sessionID: session.id,
            type: "user",
            delivery: input.delivery ?? "steer",
          })
          if (existing) return existing
          // Preparation, the staged revert commit and admission share one acquisition of the session's location
          // services, so projections see its config whoever called prompt.
          return yield* Effect.gen(function* () {
            const item = yield* restore(SessionPrompt.prepare({ session, messageID, input }))
            // Commit a staged revert only after preparation succeeds, before admitting new work.
            if (session.revert) yield* SessionRevert.commit(bus, session)
            return yield* admission.admit({
              id: messageID,
              sessionID: session.id,
              item,
            })
          }).pipe(
            instances.provide(session),
            Effect.provideService(Database.Service, database),
            Effect.provideService(FSUtil.Service, fs),
          )
        }).pipe(
          Effect.catchTag("SessionInbox.LifecycleConflict", () => new PromptConflictError({ sessionID, messageID })),
        )
        if (input.resume !== false) yield* execution.wake(sessionID)
        return admitted
      }),
    ),
  )
  const shell = Effect.fn("Session.shell")(function* (
    sessionID: SessionSchema.ID,
    input: { id?: SessionMessage.ID; command: string },
  ) {
    const session = yield* get(sessionID)
    yield* assertWritable(session)
    // The server owns completion recording even if the submitting client disconnects.
    const running = yield* Effect.gen(function* () {
      const started = yield* SessionShell.start({ session, command: input.command }).pipe(
        Effect.provideService(Instance.Service, instances),
        Effect.tapError((error) =>
          synthetic(sessionID, {
            text: `User shell command failed to start:\n${input.command}\n\n${error.message}`,
            description: input.command,
            metadata: { source: "shell", state: "error" },
            resume: false,
          }),
        ),
        Effect.orDie,
      )
      yield* bus.publish(
        SessionEvent.Shell.Started,
        {
          sessionID,
          shell: started.info,
        },
        { id: input.id ? Event.ID.make(input.id.replace(/^msg_/, "evt_")) : undefined },
      )
      const terminal = yield* started.result
      const preview = yield* started.output
      yield* bus.publish(SessionEvent.Shell.Ended, {
        sessionID,
        shell: terminal.info,
        output: preview,
      })
      yield* synthetic(sessionID, {
        ...ShellResult.userNotification(terminal),
        resume: false,
      }).pipe(
        Effect.catchTag("Session.NotFoundError", () => Effect.void),
        Effect.orDie,
      )
    }).pipe(Effect.forkIn(scope, { startImmediately: true }))
    yield* Fiber.join(running)
  })
  const skill = Effect.fn("Session.skill")(function* (
    sessionID: SessionSchema.ID,
    input: { messageID?: SessionMessage.ID; skill: Skill.ID; resume?: boolean },
  ) {
    const session = yield* get(sessionID)
    yield* assertWritable(session)
    const skill = yield* SessionSkill.get({ session, skill: input.skill }).pipe(
      Effect.provideService(Instance.Service, instances),
    )
    yield* bus.publish(
      SessionEvent.Skill.Activated,
      {
        sessionID,
        id: skill.id,
        name: skill.name,
        text: skill.content,
      },
      { id: input.messageID ? Event.ID.make(input.messageID.replace(/^msg_/, "evt_")) : undefined },
    )
    if (input.resume !== false)
      yield* execution
        .resume(sessionID)
        .pipe(Effect.ignore, Effect.forkIn(scope, { startImmediately: true }), Effect.asVoid)
  })
  const compact = Effect.fn("Session.compact")(function* (
    sessionID: SessionSchema.ID,
    input: { id?: SessionMessage.ID; delivery?: SessionInbox.Delivery },
  ) {
    const session = yield* get(sessionID)
    yield* assertWritable(session)
    if (session.revert) yield* SessionRevert.commit(bus, session)
    const inputID = input.id ?? SessionMessage.ID.create()
    const admitted = yield* admission
      .admitCompaction({
        id: inputID,
        sessionID,
        delivery: input.delivery ?? "steer",
      })
      .pipe(
        Effect.catchTag("SessionInbox.LifecycleConflict", () => new CompactionConflictError({ sessionID, inputID })),
      )
    yield* execution.wake(sessionID)
    return admitted
  })
  const wait = Effect.fn("Session.wait")(function* (sessionID: SessionSchema.ID) {
    yield* get(sessionID)
    yield* execution.awaitIdle(sessionID)
  })
  const resume = Effect.fn("Session.resume")(function* (sessionID: SessionSchema.ID) {
    yield* get(sessionID)
    yield* execution.resume(sessionID)
  })
  const synthetic = Effect.fn("Session.synthetic")(
    (
      sessionID: SessionSchema.ID,
      input: {
        id?: SessionMessage.ID
        text: string
        description?: string
        metadata?: Record<string, unknown>
        delivery?: SessionInbox.Delivery
        resume?: boolean
      },
    ) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          const session = yield* get(sessionID)
          yield* assertWritable(session)
          const inputID = input.id ?? SessionMessage.ID.create()
          const admittedInput = {
            type: "synthetic",
            payload: SessionInbox.SyntheticPayload.make({
              text: input.text,
              description: input.description,
              metadata: input.metadata,
            }),
            delivery: input.delivery ?? "steer",
          } satisfies SessionInbox.Item
          const admitted = yield* admission
            .admit({
              id: inputID,
              sessionID,
              item: admittedInput,
            })
            .pipe(
              Effect.catchTag(
                "SessionInbox.LifecycleConflict",
                () => new SyntheticConflictError({ sessionID, inputID }),
              ),
            )
          if (input.resume !== false && !(yield* get(sessionID)).revert) yield* execution.wake(sessionID)
          return admitted
        }),
      ),
  )
  const interrupt = Effect.fn("Session.interrupt")(
    (sessionID: SessionSchema.ID, options?: { readonly resume?: boolean }) =>
      Effect.uninterruptible(execution.interrupt(sessionID, options)),
  )
  const stage = Effect.fn("Session.revert.stage")(function* (
    sessionID: SessionSchema.ID,
    input: { messageID: SessionMessage.ID; files?: boolean },
  ) {
    const session = yield* get(sessionID)
    yield* assertWritable(session)
    if (yield* execution.isActive(sessionID)) return yield* new BusyError({ sessionID })
    return yield* SessionRevert.stage({ session, messageID: input.messageID, files: input.files }).pipe(
      Effect.provideService(Instance.Service, instances),
      Effect.provideService(Database.Service, database),
      Effect.provideService(Bus.Service, bus),
    )
  })
  const clear = Effect.fn("Session.revert.clear")(function* (sessionID: SessionSchema.ID) {
    const session = yield* get(sessionID)
    yield* assertWritable(session)
    if (yield* execution.isActive(sessionID)) return yield* new BusyError({ sessionID })
    yield* SessionRevert.clear(session).pipe(
      Effect.provideService(Instance.Service, instances),
      Effect.provideService(Bus.Service, bus),
    )
    return yield* execution.wake(sessionID)
  })
  const commit = Effect.fn("Session.revert.commit")(function* (sessionID: SessionSchema.ID) {
    const session = yield* get(sessionID)
    yield* assertWritable(session)
    if (yield* execution.isActive(sessionID)) return yield* new BusyError({ sessionID })
    return yield* SessionRevert.commit(bus, session)
  })
  const revert = { stage, clear, commit }
  const operations = {
    get,
    message,
    view,
    rename,
    archive,
    unarchive,
    archivePreview,
    setArchived,
    archiveSubtree,
    unarchiveSubtree,
    setMetadata,
    setPermissions,
    switchAgent,
    switchModel,
    inbox,
    prompt,
    synthetic,
    shell,
    skill,
    compact,
    wait,
    resume,
    interrupt,
    cancelInbox,
    steerInbox,
    queueInbox,
    revert,
  }

  const forSession = (sessionID: SessionSchema.ID) => {
    const get = operations.get.bind(undefined, sessionID)
    const message = operations.message.bind(undefined, sessionID)
    const view = operations.view.bind(undefined, sessionID)
    const rename = operations.rename.bind(undefined, sessionID)
    const archive = operations.archive.bind(undefined, sessionID)
    const unarchive = operations.unarchive.bind(undefined, sessionID)
    const archivePreview = operations.archivePreview.bind(undefined, sessionID)
    const setArchived = operations.setArchived.bind(undefined, sessionID)
    const archiveSubtree = operations.archiveSubtree.bind(undefined, sessionID)
    const unarchiveSubtree = operations.unarchiveSubtree.bind(undefined, sessionID)
    const setMetadata = operations.setMetadata.bind(undefined, sessionID)
    const setPermissions = operations.setPermissions.bind(undefined, sessionID)
    const switchAgent = operations.switchAgent.bind(undefined, sessionID)
    const switchModel = operations.switchModel.bind(undefined, sessionID)
    const inbox = operations.inbox.bind(undefined, sessionID)
    const prompt = operations.prompt.bind(undefined, sessionID)
    const synthetic = operations.synthetic.bind(undefined, sessionID)
    const shell = operations.shell.bind(undefined, sessionID)
    const skill = operations.skill.bind(undefined, sessionID)
    const compact = operations.compact.bind(undefined, sessionID)
    const wait = operations.wait.bind(undefined, sessionID)
    const resume = operations.resume.bind(undefined, sessionID)
    const interrupt = operations.interrupt.bind(undefined, sessionID)
    const cancelInbox = operations.cancelInbox.bind(undefined, sessionID)
    const steerInbox = operations.steerInbox.bind(undefined, sessionID)
    const queueInbox = operations.queueInbox.bind(undefined, sessionID)
    const stage = operations.revert.stage.bind(undefined, sessionID)
    const clear = operations.revert.clear.bind(undefined, sessionID)
    const commit = operations.revert.commit.bind(undefined, sessionID)
    const revert = { stage, clear, commit }

    return {
      id: sessionID,
      get,
      message,
      view,
      rename,
      archive,
      unarchive,
      archivePreview,
      setArchived,
      archiveSubtree,
      unarchiveSubtree,
      setMetadata,
      setPermissions,
      switchAgent,
      switchModel,
      inbox,
      prompt,
      synthetic,
      shell,
      skill,
      compact,
      wait,
      resume,
      interrupt,
      cancelInbox,
      steerInbox,
      queueInbox,
      revert,
    }
  }
  return { forSession }
})

export type Handle = ReturnType<Effect.Success<ReturnType<typeof make>>["forSession"]>

// Mirrors the shell tool's in-memory preview safety limit.
