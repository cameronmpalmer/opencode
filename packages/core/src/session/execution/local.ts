import { Cause, Effect, Layer } from "effect"
import { Database } from "../../database/database"
import { LocationServiceMap } from "../../location-service-map"
import { makeGlobalNode } from "../../effect/app-node"
import { SessionRunCoordinator } from "../run-coordinator"
import { SessionRunner } from "../runner"
import { SessionExecution } from "../execution"
import { SessionSchema } from "../schema"
import { SessionStore } from "../store"
import { withRunLease, type Timing } from "./run-lease"

/** Current-process routing for implicit-local Locations. Future remote placement belongs here. */
export const makeLayer = (timing: Partial<Timing> = {}) =>
  Layer.effect(
    SessionExecution.Service,
    Effect.gen(function* () {
      const store = yield* SessionStore.Service
      const locations = yield* LocationServiceMap.Service
      const { db } = yield* Database.Service
      const coordinator = yield* SessionRunCoordinator.make<SessionSchema.ID, SessionRunner.RunError>({
        drain: Effect.fnUntraced(function* (sessionID: SessionSchema.ID, force) {
          const session = yield* store.get(sessionID)
          if (!session) return yield* Effect.die(`Session not found: ${sessionID}`)
          return yield* SessionRunner.Service.use((runner) =>
            withRunLease({
              db,
              sessionID,
              run: runner.run({ sessionID, force }),
              timing,
            }),
          ).pipe(
            Effect.provide(locations.get(session.location)),
            Effect.tapCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.void
                : Effect.logError("Failed to drain Session", cause).pipe(Effect.annotateLogs({ sessionID })),
            ),
          )
        }),
      })

      return SessionExecution.Service.of({
        active: coordinator.active,
        interrupt: coordinator.interrupt,
        resume: coordinator.run,
        wake: coordinator.wake,
      })
    }),
  )

export const node = makeGlobalNode({
  service: SessionExecution.Service,
  layer: makeLayer(),
  deps: [SessionStore.node, LocationServiceMap.node, Database.node],
})

export * as SessionExecutionLocal from "./local"
