export * as SessionRunLease from "./run-lease"

import { and, eq } from "drizzle-orm"
import { randomUUID } from "node:crypto"
import { Cause, Deferred, Duration, Effect } from "effect"
import { Database } from "../../database/database"
import { SessionArchivedError, SessionLeaseBusyError } from "../error"
import { SessionSchema } from "../schema"
import { SessionRunLeaseTable, SessionTable } from "../sql"

/**
 * Heartbeats older than this are considered abandoned (design 4.2). Shared with the
 * archive service so both sides use one cutoff.
 */
export const LIVE_LEASE_MS = 60_000
/**
 * A live run renews its lease at least about this often: the timer fires every
 * `HEARTBEAT_MS` plus scheduling latency, so treat it as a lower bound on the renewal
 * period, not an exact interval.
 *
 * Independence limitation: the heartbeat and cancel poll are independent Effect fibers,
 * not worker threads. They run while the run is awaiting async I/O (LLM calls, tool
 * processes, SQLite), but they cannot fire while synchronous CPU work blocks the event
 * loop. `LIVE_LEASE_MS` (60s) and the per-tool timeouts bound that window; a single
 * synchronous block longer than 60s can still make a live lease look stale. A worker
 * thread or external timer would be required to remove that limitation.
 */
export const HEARTBEAT_MS = 5_000
/** How often a live run checks for an archive request. */
export const CANCEL_POLL_MS = 1_000

export interface Timing {
  readonly liveLeaseMs: number
  readonly heartbeatMs: number
  readonly pollMs: number
  readonly now: () => number
}

export const defaultTiming: Timing = {
  liveLeaseMs: LIVE_LEASE_MS,
  heartbeatMs: HEARTBEAT_MS,
  pollMs: CANCEL_POLL_MS,
  now: Date.now,
}

const resolveTiming = (overrides: Partial<Timing> = {}): Timing => ({ ...defaultTiming, ...overrides })

const acquire = (
  db: Database.Interface["db"],
  sessionID: SessionSchema.ID,
  leaseID: string,
  timing: Timing,
): Effect.Effect<void, SessionArchivedError | SessionLeaseBusyError> =>
  db
    .transaction(
      (tx) =>
        Effect.gen(function* () {
          const now = timing.now()
          const session = yield* tx
            .select({ archiveState: SessionTable.archive_state, archivedAt: SessionTable.time_archived })
            .from(SessionTable)
            .where(eq(SessionTable.id, sessionID))
            .get()
            .pipe(Effect.orDie)

          // Checked in the same immediate transaction as the lease insert so A4 cannot
          // commit the archiving marker between this read and the run starting.
          if (!session) {
            return yield* Effect.fail(
              new SessionArchivedError({ sessionID, message: `Session ${sessionID} disappeared before the run` }),
            )
          }
          if (session.archiveState === "archiving" || session.archivedAt !== null) {
            return yield* Effect.fail(
              new SessionArchivedError({
                sessionID,
                message: `Session ${sessionID} is already archived or archiving`,
              }),
            )
          }

          const existing = yield* tx
            .select()
            .from(SessionRunLeaseTable)
            .where(eq(SessionRunLeaseTable.session_id, sessionID))
            .get()
            .pipe(Effect.orDie)

          if (existing) {
            if (existing.heartbeat_at >= now - timing.liveLeaseMs) {
              return yield* Effect.fail(
                new SessionLeaseBusyError({
                  sessionID,
                  message: `Session ${sessionID} already has a live run lease ${existing.lease_id}`,
                }),
              )
            }
            yield* Effect.logWarning("Replacing stale session run lease", {
              sessionID,
              previousLeaseID: existing.lease_id,
              heartbeatAt: existing.heartbeat_at,
              ageMs: now - existing.heartbeat_at,
            })
            // Delete explicitly so a renewed heartbeat keeps its ownership check meaningful.
            yield* tx
              .delete(SessionRunLeaseTable)
              .where(
                and(
                  eq(SessionRunLeaseTable.session_id, sessionID),
                  eq(SessionRunLeaseTable.lease_id, existing.lease_id),
                ),
              )
              .run()
              .pipe(Effect.orDie)
          }

          yield* tx
            .insert(SessionRunLeaseTable)
            .values({ session_id: sessionID, lease_id: leaseID, heartbeat_at: now, cancel_requested: 0 })
            .run()
            .pipe(Effect.orDie)
        }),
      { behavior: "immediate" },
    )
    .pipe(
      Effect.catch((error) => {
        if (error instanceof SessionLeaseBusyError || error instanceof SessionArchivedError) return Effect.fail(error)
        return Effect.die(error)
      }),
    )

const release = (db: Database.Interface["db"], sessionID: SessionSchema.ID, leaseID: string) =>
  Effect.uninterruptible(
    db
      .delete(SessionRunLeaseTable)
      .where(and(eq(SessionRunLeaseTable.session_id, sessionID), eq(SessionRunLeaseTable.lease_id, leaseID)))
      .run()
      .pipe(
        Effect.asVoid,
        // A release defect must not replace the run's original exit; leave the row for
        // stale replacement instead.
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to release session run lease", {
            sessionID,
            leaseID,
            cause: Cause.pretty(cause),
          }),
        ),
      ),
  )

const heartbeatLoop = (
  db: Database.Interface["db"],
  sessionID: SessionSchema.ID,
  leaseID: string,
  timing: Timing,
  stop: Deferred.Deferred<void, SessionArchivedError>,
) =>
  Effect.gen(function* () {
    for (;;) {
      yield* Effect.sleep(Duration.millis(timing.heartbeatMs))
      const renewed = yield* db
        .update(SessionRunLeaseTable)
        .set({ heartbeat_at: timing.now() })
        .where(and(eq(SessionRunLeaseTable.session_id, sessionID), eq(SessionRunLeaseTable.lease_id, leaseID)))
        .returning({ leaseID: SessionRunLeaseTable.lease_id })
        .get()
        .pipe(Effect.orDie)
      if (!renewed) {
        return yield* Deferred.fail(
          stop,
          new SessionArchivedError({
            sessionID,
            message: `Run lease for session ${sessionID} was deleted or stolen`,
          }),
        )
      }
    }
  })

const cancelPollLoop = (
  db: Database.Interface["db"],
  sessionID: SessionSchema.ID,
  leaseID: string,
  timing: Timing,
  stop: Deferred.Deferred<void, SessionArchivedError>,
) =>
  Effect.gen(function* () {
    for (;;) {
      const lease = yield* db
        .select({ leaseID: SessionRunLeaseTable.lease_id, cancelRequested: SessionRunLeaseTable.cancel_requested })
        .from(SessionRunLeaseTable)
        .where(and(eq(SessionRunLeaseTable.session_id, sessionID), eq(SessionRunLeaseTable.lease_id, leaseID)))
        .get()
        .pipe(Effect.orDie)
      if (!lease) {
        return yield* Deferred.fail(
          stop,
          new SessionArchivedError({
            sessionID,
            message: `Run lease for session ${sessionID} was deleted or stolen`,
          }),
        )
      }
      const session = yield* db
        .select({ archiveState: SessionTable.archive_state, archivedAt: SessionTable.time_archived })
        .from(SessionTable)
        .where(eq(SessionTable.id, sessionID))
        .get()
        .pipe(Effect.orDie)
      if (!session) {
        return yield* Deferred.fail(
          stop,
          new SessionArchivedError({ sessionID, message: `Session ${sessionID} disappeared during a run` }),
        )
      }
      // A4 may restore cancel_requested to 0 after a failed archive. Observing 1 once is
      // enough to stop: do not require the marker to still be set on a later poll.
      if (lease.cancelRequested === 1) {
        return yield* Deferred.fail(
          stop,
          new SessionArchivedError({ sessionID, message: `Cancellation requested for session ${sessionID}` }),
        )
      }
      if (session.archiveState === "archiving" || session.archivedAt !== null) {
        return yield* Deferred.fail(
          stop,
          new SessionArchivedError({ sessionID, message: `Session ${sessionID} is archived` }),
        )
      }
      yield* Effect.sleep(Duration.millis(timing.pollMs))
    }
  })

/**
 * Runs one local continuation while holding the session run lease.
 *
 * The lease is acquired in its own immediate transaction, renewed on an independent
 * timer, and deleted in an uninterruptible finalizer on success, failure, defect, or
 * interruption. A cancellation poll stops the run when the archive flow marks the
 * lease, the session, or its archived timestamp.
 */
export const withRunLease = <A, E, R>(input: {
  readonly db: Database.Interface["db"]
  readonly sessionID: SessionSchema.ID
  readonly run: Effect.Effect<A, E, R>
  readonly timing?: Partial<Timing>
}): Effect.Effect<A, E | SessionArchivedError | SessionLeaseBusyError, R> => {
  const timing = resolveTiming(input.timing)
  const leaseID = randomUUID()
  return Effect.scoped(
    Effect.gen(function* () {
      yield* Effect.acquireRelease(acquire(input.db, input.sessionID, leaseID, timing), () =>
        release(input.db, input.sessionID, leaseID),
      )

      const stop = yield* Deferred.make<void, SessionArchivedError>()
      yield* Effect.forkScoped(
        heartbeatLoop(input.db, input.sessionID, leaseID, timing, stop).pipe(
          Effect.catchCause((cause) => Deferred.failCause(stop, cause)),
        ),
      )
      yield* Effect.forkScoped(
        cancelPollLoop(input.db, input.sessionID, leaseID, timing, stop).pipe(
          Effect.catchCause((cause) => Deferred.failCause(stop, cause)),
        ),
      )

      return yield* Effect.raceFirst(input.run, Deferred.await(stop).pipe(Effect.andThen(Effect.never)))
    }),
  )
}
