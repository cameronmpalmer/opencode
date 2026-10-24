import { describe, expect } from "bun:test"
import { and, eq } from "drizzle-orm"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, LayerMap, Option } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { makeGlobalNode } from "@opencode-ai/core/effect/app-node"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Location } from "@opencode-ai/core/location"
import { LocationServiceMap, type LocationError, type LocationServices } from "@opencode-ai/core/location-services"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { ContextSnapshotDecodeError } from "@opencode-ai/core/session/error"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionExecutionLocal } from "@opencode-ai/core/session/execution/local"
import { SessionRunner } from "@opencode-ai/core/session/runner"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionRunLeaseTable, SessionTable } from "@opencode-ai/core/session/sql"
import { SessionStore } from "@opencode-ai/core/session/store"
import { testEffect } from "./lib/effect"

const projectID = Project.ID.global
const timing = { liveLeaseMs: 200, heartbeatMs: 20, pollMs: 10 }

let behavior: (input: {
  sessionID: SessionSchema.ID
  force: boolean
}) => Effect.Effect<void, SessionRunner.RunError> = () => Effect.void
let observedDB: Database.Interface["db"] | undefined

const fakeRunner = Layer.succeed(
  SessionRunner.Service,
  SessionRunner.Service.of({
    run: (input) => Effect.suspend(() => behavior(input)),
  }),
)

const locationsNode = makeGlobalNode({
  service: LocationServiceMap.Service,
  layer: Layer.effect(
    LocationServiceMap.Service,
    Effect.gen(function* () {
      return yield* LayerMap.make((_ref: Location.Ref) => fakeRunner, { idleTimeToLive: "1 minute" })
    }) as unknown as Effect.Effect<LayerMap.LayerMap<Location.Ref, LocationServices, LocationError>, never, never>,
  ),
  deps: [],
})

const executionNode = makeGlobalNode({
  service: SessionExecution.Service,
  layer: SessionExecutionLocal.makeLayer(timing),
  deps: [SessionStore.node, LocationServiceMap.node, Database.node],
})

const it = testEffect(
  AppNodeBuilder.build(LayerNode.group([Database.node, executionNode]), [[LocationServiceMap.node, locationsNode]]),
)

const seedSession = (sessionID: SessionSchema.ID, values: Partial<typeof SessionTable.$inferInsert> = {}) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    observedDB = db
    yield* db
      .insert(ProjectTable)
      .values({ id: projectID, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: projectID,
        slug: "test",
        directory: "/project",
        title: "test",
        version: "test",
        time_created: 1,
        time_updated: 1,
        ...values,
      })
      .run()
      .pipe(Effect.orDie)
    return db
  })

const readLease = (sessionID: SessionSchema.ID) =>
  Effect.gen(function* () {
    const db = observedDB!
    return yield* db
      .select()
      .from(SessionRunLeaseTable)
      .where(eq(SessionRunLeaseTable.session_id, sessionID))
      .get()
      .pipe(Effect.orDie)
  })

const waitFor = <A>(label: string, effect: Effect.Effect<A>, predicate: (value: A) => boolean) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 400; attempt++) {
      const value = yield* effect
      if (predicate(value)) return value
      yield* Effect.sleep("5 millis")
    }
    return yield* Effect.die(new Error(`Timed out waiting for ${label}`))
  })

const failureTag = (exit: Exit.Exit<unknown, unknown>) => {
  if (Exit.isSuccess(exit)) return undefined
  const error = Option.getOrUndefined(Cause.findErrorOption(exit.cause))
  return error && typeof error === "object" && "_tag" in error ? error._tag : undefined
}

describe("SessionRunLease", () => {
  it.live("inserts exactly one lease with a unique id and deletes it on success", () =>
    Effect.gen(function* () {
      const sessionID = SessionSchema.ID.make("ses_lease_success")
      yield* seedSession(sessionID)
      const observed: string[] = []
      behavior = () =>
        Effect.gen(function* () {
          const rows = yield* observedDB!
            .select()
            .from(SessionRunLeaseTable)
            .where(eq(SessionRunLeaseTable.session_id, sessionID))
            .all()
            .pipe(Effect.orDie)
          expect(rows).toHaveLength(1)
          observed.push(rows[0].lease_id)
          expect(rows[0].lease_id).toMatch(/^[0-9a-f-]{36}$/)
        })

      yield* (yield* SessionExecution.Service).resume(sessionID)

      expect(observed).toHaveLength(1)
      expect(yield* readLease(sessionID)).toBeUndefined()
    }),
  )

  it.live("heartbeats on an independent timer while the run is blocked, then deletes the lease before idle", () =>
    Effect.gen(function* () {
      const sessionID = SessionSchema.ID.make("ses_lease_heartbeat")
      yield* seedSession(sessionID)
      const gate = yield* Deferred.make<void>()
      behavior = () => Deferred.await(gate)
      const execution = yield* SessionExecution.Service

      const fiber = yield* execution.resume(sessionID).pipe(Effect.forkChild)
      const lease = yield* waitFor("first heartbeat", readLease(sessionID), (row) => row !== undefined)
      const firstHeartbeat = lease!.heartbeat_at
      expect(lease!.lease_id.length).toBeGreaterThan(0)

      yield* waitFor(
        "advanced heartbeat",
        readLease(sessionID),
        (row) => row !== undefined && row.heartbeat_at > firstHeartbeat,
      )
      expect(Array.from(yield* execution.active)).toContain(sessionID)

      yield* Deferred.succeed(gate, undefined)
      yield* Fiber.join(fiber)

      expect(yield* readLease(sessionID)).toBeUndefined()
      expect(Array.from(yield* execution.active)).not.toContain(sessionID)
    }),
  )

  it.live("deletes the lease when the run fails", () =>
    Effect.gen(function* () {
      const sessionID = SessionSchema.ID.make("ses_lease_failure")
      yield* seedSession(sessionID)
      behavior = () => Effect.fail(new ContextSnapshotDecodeError({ sessionID, details: "boom" }))

      const exit = yield* (yield* SessionExecution.Service).resume(sessionID).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBeTrue()
      expect(yield* readLease(sessionID)).toBeUndefined()
    }),
  )

  it.live("deletes the lease when the run is interrupted", () =>
    Effect.gen(function* () {
      const sessionID = SessionSchema.ID.make("ses_lease_interrupt")
      yield* seedSession(sessionID)
      const started = yield* Deferred.make<void>()
      behavior = () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
      const execution = yield* SessionExecution.Service

      const fiber = yield* execution.resume(sessionID).pipe(Effect.forkChild)
      yield* Deferred.await(started)
      yield* waitFor("lease", readLease(sessionID), (row) => row !== undefined)

      yield* execution.interrupt(sessionID)
      yield* Fiber.join(fiber).pipe(Effect.exit)

      expect(yield* readLease(sessionID)).toBeUndefined()
    }),
  )

  it.live("stops the run when cancel_requested is observed, even if it is restored to 0", () =>
    Effect.gen(function* () {
      const sessionID = SessionSchema.ID.make("ses_lease_cancel")
      const db = yield* seedSession(sessionID)
      behavior = () => Effect.never

      const fiber = yield* (yield* SessionExecution.Service).resume(sessionID).pipe(Effect.forkChild)
      const lease = yield* waitFor("lease", readLease(sessionID), (row) => row !== undefined)

      yield* db
        .update(SessionRunLeaseTable)
        .set({ cancel_requested: 1 })
        .where(eq(SessionRunLeaseTable.session_id, sessionID))
        .run()
        .pipe(Effect.orDie)
      // Observe the run stopping before A4's failure path restores the marker.
      yield* waitFor("cancelled run to release the lease", readLease(sessionID), (row) => row === undefined)
      // A4 restores the marker after a failed archive; the run must not re-arm.
      yield* db
        .update(SessionRunLeaseTable)
        .set({ cancel_requested: 0 })
        .where(and(eq(SessionRunLeaseTable.session_id, sessionID), eq(SessionRunLeaseTable.lease_id, lease!.lease_id)))
        .run()
        .pipe(Effect.orDie)

      const exit = yield* Fiber.join(fiber).pipe(Effect.exit)
      expect(failureTag(exit)).toBe("Session.SessionArchivedError")
      expect(yield* readLease(sessionID)).toBeUndefined()
    }),
  )

  it.live("stops the run when the session is marked archiving", () =>
    Effect.gen(function* () {
      const sessionID = SessionSchema.ID.make("ses_lease_archiving")
      const db = yield* seedSession(sessionID)
      behavior = () => Effect.never

      const fiber = yield* (yield* SessionExecution.Service).resume(sessionID).pipe(Effect.forkChild)
      yield* waitFor("lease", readLease(sessionID), (row) => row !== undefined)
      yield* db
        .update(SessionTable)
        .set({ archive_state: "archiving" })
        .where(eq(SessionTable.id, sessionID))
        .run()
        .pipe(Effect.orDie)

      const exit = yield* Fiber.join(fiber).pipe(Effect.exit)
      expect(failureTag(exit)).toBe("Session.SessionArchivedError")
      expect(yield* readLease(sessionID)).toBeUndefined()
    }),
  )

  it.live("stops the run when time_archived is set", () =>
    Effect.gen(function* () {
      const sessionID = SessionSchema.ID.make("ses_lease_archived")
      const db = yield* seedSession(sessionID)
      behavior = () => Effect.never

      const fiber = yield* (yield* SessionExecution.Service).resume(sessionID).pipe(Effect.forkChild)
      yield* waitFor("lease", readLease(sessionID), (row) => row !== undefined)
      yield* db
        .update(SessionTable)
        .set({ time_archived: Date.now() })
        .where(eq(SessionTable.id, sessionID))
        .run()
        .pipe(Effect.orDie)

      const exit = yield* Fiber.join(fiber).pipe(Effect.exit)
      expect(failureTag(exit)).toBe("Session.SessionArchivedError")
      expect(yield* readLease(sessionID)).toBeUndefined()
    }),
  )

  it.live("fails busy without replacing a live lease held by another process", () =>
    Effect.gen(function* () {
      const sessionID = SessionSchema.ID.make("ses_lease_busy")
      const db = yield* seedSession(sessionID)
      yield* db
        .insert(SessionRunLeaseTable)
        .values({
          session_id: sessionID,
          lease_id: "foreign-lease",
          heartbeat_at: Date.now(),
          cancel_requested: 0,
        })
        .run()
        .pipe(Effect.orDie)
      let ran = false
      behavior = () => Effect.sync(() => (ran = true))

      const exit = yield* (yield* SessionExecution.Service).resume(sessionID).pipe(Effect.exit)

      expect(failureTag(exit)).toBe("Session.SessionLeaseBusyError")
      expect(ran).toBeFalse()
      expect((yield* readLease(sessionID))?.lease_id).toBe("foreign-lease")
    }),
  )

  it.live("replaces a stale lease only after the cutoff", () =>
    Effect.gen(function* () {
      const sessionID = SessionSchema.ID.make("ses_lease_stale")
      const db = yield* seedSession(sessionID)
      yield* db
        .insert(SessionRunLeaseTable)
        .values({
          session_id: sessionID,
          lease_id: "stale-lease",
          heartbeat_at: Date.now() - timing.liveLeaseMs - 50,
          cancel_requested: 0,
        })
        .run()
        .pipe(Effect.orDie)
      let observedLeaseID: string | undefined
      behavior = () =>
        Effect.gen(function* () {
          const row = yield* observedDB!
            .select()
            .from(SessionRunLeaseTable)
            .where(eq(SessionRunLeaseTable.session_id, sessionID))
            .get()
            .pipe(Effect.orDie)
          observedLeaseID = row?.lease_id
        })

      yield* (yield* SessionExecution.Service).resume(sessionID)

      expect(observedLeaseID).toBeDefined()
      expect(observedLeaseID).not.toBe("stale-lease")
      expect(observedLeaseID).not.toBeUndefined()
      expect(yield* readLease(sessionID)).toBeUndefined()
    }),
  )

  it.live("does not replace a lease that is older than a poll but inside the cutoff", () =>
    Effect.gen(function* () {
      const sessionID = SessionSchema.ID.make("ses_lease_recent")
      const db = yield* seedSession(sessionID)
      yield* db
        .insert(SessionRunLeaseTable)
        .values({
          session_id: sessionID,
          lease_id: "recent-lease",
          heartbeat_at: Date.now() - timing.liveLeaseMs + 100,
          cancel_requested: 0,
        })
        .run()
        .pipe(Effect.orDie)
      let ran = false
      behavior = () => Effect.sync(() => (ran = true))

      const exit = yield* (yield* SessionExecution.Service).resume(sessionID).pipe(Effect.exit)

      expect(failureTag(exit)).toBe("Session.SessionLeaseBusyError")
      expect(ran).toBeFalse()
      expect((yield* readLease(sessionID))?.lease_id).toBe("recent-lease")
    }),
  )
})
