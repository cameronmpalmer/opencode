export * as SessionRunLease from "./run-lease.js"

import { and, eq } from "drizzle-orm"
import { randomUUID } from "node:crypto"
import { Cause, Duration, Effect } from "effect"
import { Database } from "../../database/database.js"
import { SessionArchivedError, SessionLeaseBusyError } from "../error.js"
import { SessionSchema } from "../schema.js"
import { SessionRunLeaseTable, SessionTable } from "../sql.js"

/** Heartbeats older than this are abandoned. Shared with the archive service. */
export const LIVE_LEASE_MS = 60_000
/** A live run renews its lease at least about this often. */
export const HEARTBEAT_MS = 5_000
/** How often a live run checks for an archive request. */
export const CANCEL_POLL_MS = 1_000

type Db = Database.Interface["db"]

export const acquire = (
  db: Db,
  sessionID: SessionSchema.ID,
  leaseID: string = randomUUID(),
): Effect.Effect<string, SessionArchivedError | SessionLeaseBusyError> =>
  db
    .transaction(
      (tx) =>
        Effect.gen(function* () {
          const now = Date.now()
          const session = yield* tx
            .select({ archiveState: SessionTable.archive_state, archivedAt: SessionTable.time_archived })
            .from(SessionTable)
            .where(eq(SessionTable.id, sessionID))
            .get()
            .pipe(Effect.orDie)
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
            if (existing.heartbeat_at >= now - LIVE_LEASE_MS) {
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
          return leaseID
        }),
      { behavior: "immediate" },
    )
    .pipe(
      Effect.catch((error) => {
        if (error instanceof SessionLeaseBusyError || error instanceof SessionArchivedError) return Effect.fail(error)
        return Effect.die(error)
      }),
    )

export const release = (db: Db, sessionID: SessionSchema.ID, leaseID: string) =>
  Effect.uninterruptible(
    db
      .delete(SessionRunLeaseTable)
      .where(and(eq(SessionRunLeaseTable.session_id, sessionID), eq(SessionRunLeaseTable.lease_id, leaseID)))
      .run()
      .pipe(
        Effect.asVoid,
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to release session run lease", {
            sessionID,
            leaseID,
            cause: Cause.pretty(cause),
          }),
        ),
      ),
  )

export const heartbeatLoop = (db: Db, sessionID: SessionSchema.ID, leaseID: string) =>
  Effect.forever(
    Effect.sleep(Duration.millis(HEARTBEAT_MS)).pipe(
      Effect.andThen(
        db
          .update(SessionRunLeaseTable)
          .set({ heartbeat_at: Date.now() })
          .where(and(eq(SessionRunLeaseTable.session_id, sessionID), eq(SessionRunLeaseTable.lease_id, leaseID)))
          .run(),
      ),
      Effect.orDie,
    ),
  )

export const cancelPollLoop = (
  db: Db,
  sessionID: SessionSchema.ID,
  leaseID: string,
  interrupt: () => Effect.Effect<boolean>,
) =>
  Effect.gen(function* () {
    for (;;) {
      yield* Effect.sleep(Duration.millis(CANCEL_POLL_MS))
      const lease = yield* db
        .select({ leaseID: SessionRunLeaseTable.lease_id, cancelRequested: SessionRunLeaseTable.cancel_requested })
        .from(SessionRunLeaseTable)
        .where(and(eq(SessionRunLeaseTable.session_id, sessionID), eq(SessionRunLeaseTable.lease_id, leaseID)))
        .get()
        .pipe(Effect.orDie)
      const session = yield* db
        .select({ archiveState: SessionTable.archive_state, archivedAt: SessionTable.time_archived })
        .from(SessionTable)
        .where(eq(SessionTable.id, sessionID))
        .get()
        .pipe(Effect.orDie)
      if (
        !lease ||
        !session ||
        lease.cancelRequested === 1 ||
        session.archiveState === "archiving" ||
        session.archivedAt !== null
      ) {
        yield* interrupt()
        return
      }
    }
  })
