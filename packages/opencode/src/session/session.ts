import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Slug } from "@opencode-ai/core/util/slug"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { serviceUse } from "@opencode-ai/core/effect/service-use"
import path from "path"
import { BackgroundJob } from "@/background/job"
import { Decimal } from "decimal.js"
import type { ProviderMetadata, Usage } from "@opencode-ai/llm"
import { InstallationVersion } from "@opencode-ai/core/installation/version"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2Bridge } from "@/event-v2-bridge"
import { SessionV2 } from "@opencode-ai/core/session"
import * as SessionExecutionLocal from "@opencode-ai/core/session/execution/local"
import { locationServiceMapLayer } from "@opencode-ai/core/location-services"

import { NotFoundError } from "@/storage/storage"
import { eq } from "drizzle-orm"
import { and } from "drizzle-orm"
import { gte } from "drizzle-orm"
import { isNull } from "drizzle-orm"
import { desc } from "drizzle-orm"
import { like } from "drizzle-orm"
import { sql } from "drizzle-orm"
import { inArray } from "drizzle-orm"
import { lt } from "drizzle-orm"
import { or } from "drizzle-orm"
import type { SQL } from "drizzle-orm"
import {
  FamilyArchiveMigrationTable,
  PartTable,
  SessionArchiveGuardTable,
  SessionRunLeaseTable,
  SessionTable,
} from "@opencode-ai/core/session/sql"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { MessageV2 } from "./message-v2"
import type { InstanceContext } from "../project/instance-context"
import { InstanceState } from "@/effect/instance-state"
import { Snapshot } from "@/snapshot"
import { ProjectV2 } from "@opencode-ai/core/project"
import { WorkspaceV2 } from "@opencode-ai/core/workspace"
import { SessionID, MessageID, PartID } from "./schema"
import { SessionStatus } from "./status"

import type { Provider } from "@/provider/provider"
import { Global } from "@opencode-ai/core/global"
import { Effect, Layer, Option, Context, Schema, Types } from "effect"
import { NonNegativeInt, optional } from "@opencode-ai/core/schema"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { SessionMessage } from "@opencode-ai/schema/session-message"

const parentTitlePrefix = "New session - "
const childTitlePrefix = "Child session - "

export function isDefaultTitle(title: string) {
  return new RegExp(
    `^(${parentTitlePrefix}|${childTitlePrefix})\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$`,
  ).test(title)
}

type SessionRow = typeof SessionTable.$inferSelect

export function fromRow(row: SessionRow): Info {
  const summary =
    row.summary_additions !== null || row.summary_deletions !== null || row.summary_files !== null
      ? {
          additions: row.summary_additions ?? 0,
          deletions: row.summary_deletions ?? 0,
          files: row.summary_files ?? 0,
          diffs: row.summary_diffs ?? undefined,
        }
      : undefined
  const share = row.share_url ? { url: row.share_url } : undefined
  const revert = row.revert
    ? {
        messageID: MessageID.make(row.revert.messageID),
        partID: row.revert.partID ? PartID.make(row.revert.partID) : undefined,
        snapshot: row.revert.snapshot,
        diff: row.revert.diff,
      }
    : undefined
  return {
    id: row.id,
    slug: row.slug,
    projectID: row.project_id,
    workspaceID: row.workspace_id ?? undefined,
    directory: row.directory,
    path: row.path ?? undefined,
    parentID: row.parent_id ?? undefined,
    title: row.title,
    agent: row.agent ?? undefined,
    model: row.model
      ? {
          id: ModelV2.ID.make(row.model.id),
          providerID: ProviderV2.ID.make(row.model.providerID),
          variant: row.model.variant,
        }
      : undefined,
    version: row.version,
    summary,
    cost: row.cost,
    tokens: {
      input: row.tokens_input,
      output: row.tokens_output,
      reasoning: row.tokens_reasoning,
      cache: {
        read: row.tokens_cache_read,
        write: row.tokens_cache_write,
      },
    },
    share,
    metadata: row.metadata ?? undefined,
    revert,
    permission: row.permission ? [...row.permission] : undefined,
    time: {
      created: row.time_created,
      updated: row.time_updated,
      compacting: row.time_compacting ?? undefined,
      archived: row.time_archived ?? undefined,
    },
    familyArchiveID: row.family_archive_id ?? undefined,
    archiveCause: row.archive_cause ?? undefined,
    archiveState: row.archive_state ?? undefined,
    archiveStateAt: row.archive_state_at ?? undefined,
  }
}

export function toRow(info: Info) {
  return {
    id: info.id,
    project_id: info.projectID,
    workspace_id: info.workspaceID,
    parent_id: info.parentID,
    slug: info.slug,
    directory: info.directory,
    path: info.path,
    title: info.title,
    agent: info.agent,
    model: info.model,
    version: info.version,
    share_url: info.share?.url,
    summary_additions: info.summary?.additions,
    summary_deletions: info.summary?.deletions,
    summary_files: info.summary?.files,
    summary_diffs: info.summary?.diffs,
    metadata: info.metadata,
    cost: info.cost ?? 0,
    tokens_input: (info.tokens ?? EmptyTokens).input,
    tokens_output: (info.tokens ?? EmptyTokens).output,
    tokens_reasoning: (info.tokens ?? EmptyTokens).reasoning,
    tokens_cache_read: (info.tokens ?? EmptyTokens).cache.read,
    tokens_cache_write: (info.tokens ?? EmptyTokens).cache.write,
    revert: info.revert
      ? {
          messageID: SessionMessage.ID.make(info.revert.messageID),
          partID: info.revert.partID,
          snapshot: info.revert.snapshot,
          diff: info.revert.diff,
        }
      : null,
    permission: info.permission,
    time_created: info.time.created,
    time_updated: info.time.updated,
    time_compacting: info.time.compacting,
    time_archived: info.time.archived ?? null,
    family_archive_id: info.familyArchiveID ?? null,
    archive_cause: info.archiveCause ?? null,
    archive_state: info.archiveState ?? null,
    archive_state_at: info.archiveStateAt ?? null,
  }
}

function getForkedTitle(title: string): string {
  const match = title.match(/^(.+) \(fork #(\d+)\)$/)
  if (match) {
    const base = match[1]
    const num = parseInt(match[2], 10)
    return `${base} (fork #${num + 1})`
  }
  return `${title} (fork #1)`
}

function sessionPath(worktree: string, cwd: string) {
  return path.relative(path.resolve(worktree), cwd).replaceAll("\\", "/")
}

const Summary = Schema.Struct({
  additions: Schema.Finite,
  deletions: Schema.Finite,
  files: Schema.Finite,
  diffs: optional(Schema.Array(Snapshot.FileDiff)),
})

const Tokens = Schema.Struct({
  input: Schema.Finite,
  output: Schema.Finite,
  reasoning: Schema.Finite,
  cache: Schema.Struct({
    read: Schema.Finite,
    write: Schema.Finite,
  }),
})

const EmptyTokens = { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } }

const Share = Schema.Struct({
  url: Schema.String,
})

// Legacy HTTP accepted negative values here. Keep archive timestamps permissive
// while excluding non-finite values that cannot round-trip through JSON.
export const ArchivedTimestamp = Schema.Finite

const Time = Schema.Struct({
  created: NonNegativeInt,
  updated: NonNegativeInt,
  compacting: optional(NonNegativeInt),
  archived: optional(Schema.NullOr(ArchivedTimestamp)),
})

const Revert = Schema.Struct({
  messageID: MessageID,
  partID: optional(PartID),
  snapshot: optional(Schema.String),
  diff: optional(Schema.String),
})

const Model = Schema.Struct({
  id: ModelV2.ID,
  providerID: ProviderV2.ID,
  variant: optional(Schema.String),
})

export const Metadata = Schema.Record(Schema.String, Schema.Any)

export const Info = Schema.Struct({
  id: SessionID,
  slug: Schema.String,
  projectID: ProjectV2.ID,
  workspaceID: optional(WorkspaceV2.ID),
  directory: Schema.String,
  path: optional(Schema.String),
  parentID: optional(SessionID),
  summary: optional(Summary),
  cost: optional(Schema.Finite),
  tokens: optional(Tokens),
  share: optional(Share),
  title: Schema.String,
  agent: optional(Schema.String),
  model: optional(Model),
  version: Schema.String,
  metadata: optional(Metadata),
  time: Time,
  permission: optional(PermissionV1.Ruleset),
  revert: optional(Revert),
  familyArchiveID: optional(Schema.NullOr(SessionID)),
  archiveCause: optional(Schema.NullOr(Schema.Literals(["user", "cascade", "migration"]))),
  archiveState: optional(Schema.NullOr(Schema.Literal("archiving"))),
  archiveStateAt: optional(Schema.NullOr(Schema.Finite)),
}).annotate({ identifier: "Session" })
export type Info = Types.DeepMutable<Schema.Schema.Type<typeof Info>>

export const ProjectInfo = Schema.Struct({
  id: ProjectV2.ID,
  name: optional(Schema.String),
  worktree: Schema.String,
}).annotate({ identifier: "ProjectSummary" })
export type ProjectInfo = Types.DeepMutable<Schema.Schema.Type<typeof ProjectInfo>>

export const GlobalInfo = Schema.Struct({
  ...Info.fields,
  project: Schema.NullOr(ProjectInfo),
}).annotate({ identifier: "GlobalSession" })
export type GlobalInfo = Types.DeepMutable<Schema.Schema.Type<typeof GlobalInfo>>

export const CreateInput = Schema.optional(
  Schema.Struct({
    parentID: Schema.optional(SessionID),
    title: Schema.optional(Schema.String),
    agent: Schema.optional(Schema.String),
    model: Schema.optional(Model),
    metadata: Schema.optional(Metadata),
    permission: Schema.optional(PermissionV1.Ruleset),
    workspaceID: Schema.optional(WorkspaceV2.ID),
  }),
)
export type CreateInput = Types.DeepMutable<Schema.Schema.Type<typeof CreateInput>>

export const ForkInput = Schema.Struct({
  sessionID: SessionID,
  messageID: Schema.optional(MessageID),
})
export const GetInput = SessionID
export const ChildrenInput = SessionID
export const RemoveInput = SessionID
export const SetTitleInput = Schema.Struct({ sessionID: SessionID, title: Schema.String })
export const SetArchivedInput = Schema.Struct({
  sessionID: SessionID,
  time: Schema.optional(ArchivedTimestamp),
})
export const SetMetadataInput = Schema.Struct({
  sessionID: SessionID,
  metadata: Metadata,
})
export const SetPermissionInput = Schema.Struct({
  sessionID: SessionID,
  permission: PermissionV1.Ruleset,
})
export const SetRevertInput = Schema.Struct({
  sessionID: SessionID,
  revert: Schema.optional(Revert),
  summary: Schema.optional(Summary),
})
export const MessagesInput = Schema.Struct({
  sessionID: SessionID,
  limit: Schema.optional(NonNegativeInt),
})
export type ListInput = {
  directory?: string
  scope?: "project"
  path?: string
  workspaceID?: WorkspaceV2.ID
  roots?: boolean
  start?: number
  search?: string
  limit?: number
}

export type GlobalListInput = {
  directory?: string
  roots?: boolean
  start?: number
  cursor?: number
  search?: string
  limit?: number
  archived?: boolean
}

export const Event = {
  Created: SessionV1.Event.Created,
  Updated: SessionV1.Event.Updated,
  Deleted: SessionV1.Event.Deleted,
  Diff: SessionV1.Event.Diff,
  Error: SessionV1.Event.Error,
}

export function plan(input: { slug: string; time: { created: number } }, instance: InstanceContext) {
  const base = instance.project.vcs
    ? path.join(instance.worktree, ".opencode", "plans")
    : path.join(Global.Path.data, "plans")
  return path.join(base, [input.time.created, input.slug].join("-") + ".md")
}

export const getUsage = (input: { model: Provider.Model; usage: Usage; metadata?: ProviderMetadata }) => {
  const safe = (value: number) => {
    if (!Number.isFinite(value)) return 0
    return Math.max(0, value)
  }
  const inputTokens = safe(input.usage.inputTokens ?? 0)
  const outputTokens = safe(input.usage.outputTokens ?? 0)
  const reasoningTokens = safe(input.usage.reasoningTokens ?? 0)

  const cacheReadInputTokens = safe(input.usage.cacheReadInputTokens ?? 0)
  const cacheWriteInputTokens = safe(
    Number(
      input.usage.cacheWriteInputTokens ??
        input.metadata?.["anthropic"]?.["cacheCreationInputTokens"] ??
        // google-vertex-anthropic returns metadata under "vertex" key
        // (AnthropicMessagesLanguageModel custom provider key from 'vertex.anthropic.messages')
        input.metadata?.["vertex"]?.["cacheCreationInputTokens"] ??
        // @ts-expect-error
        input.metadata?.["bedrock"]?.["usage"]?.["cacheWriteInputTokens"] ??
        // @ts-expect-error
        input.metadata?.["venice"]?.["usage"]?.["cacheCreationInputTokens"] ??
        0,
    ),
  )

  // AI SDK v6 normalized inputTokens to include cached tokens across all providers
  // (including Anthropic/Bedrock which previously excluded them). Always subtract cache
  // tokens to get the non-cached input count for separate cost calculation.
  const adjustedInputTokens = safe(inputTokens - cacheReadInputTokens - cacheWriteInputTokens)

  const total = input.usage.totalTokens

  const tokens = {
    total,
    input: adjustedInputTokens,
    output: safe(outputTokens - reasoningTokens),
    reasoning: reasoningTokens,
    cache: {
      write: cacheWriteInputTokens,
      read: cacheReadInputTokens,
    },
  }

  const contextTokens = inputTokens
  const costInfo =
    input.model.cost?.tiers
      ?.filter((item) => item.tier.type === "context" && contextTokens > item.tier.size)
      .sort((a, b) => b.tier.size - a.tier.size)[0] ??
    (input.model.cost?.experimentalOver200K && contextTokens > 200_000
      ? input.model.cost.experimentalOver200K
      : input.model.cost)
  const totalNanoAiu = input.metadata?.["copilot"]?.["totalNanoAiu"]
  return {
    cost:
      typeof totalNanoAiu === "number" && Number.isFinite(totalNanoAiu) && totalNanoAiu >= 0
        ? new Decimal(totalNanoAiu).div(100_000_000_000).toNumber()
        : safe(
            new Decimal(0)
              .add(new Decimal(tokens.input).mul(costInfo?.input ?? 0).div(1_000_000))
              .add(new Decimal(tokens.output).mul(costInfo?.output ?? 0).div(1_000_000))
              .add(new Decimal(tokens.cache.read).mul(costInfo?.cache?.read ?? 0).div(1_000_000))
              .add(new Decimal(tokens.cache.write).mul(costInfo?.cache?.write ?? 0).div(1_000_000))
              // TODO: update models.dev to have better pricing model, for now:
              // charge reasoning tokens at the same rate as output tokens
              .add(new Decimal(tokens.reasoning).mul(costInfo?.output ?? 0).div(1_000_000))
              .toNumber(),
          ),
    tokens,
  }
}

export class BusyError extends Schema.TaggedErrorClass<BusyError>()("SessionBusyError", {
  sessionID: SessionID,
}) {}

export class FamilyMemberError extends Schema.TaggedErrorClass<FamilyMemberError>()(
  "SessionFamilyMemberError",
  {
    sessionID: SessionID,
    familyRootID: SessionID,
    message: Schema.String,
  },
  { httpApiStatus: 409 },
) {}

export class ArchiveInProgressError extends Schema.TaggedErrorClass<ArchiveInProgressError>()(
  "SessionArchiveInProgressError",
  {
    sessionID: SessionID,
    sessionIDs: Schema.Array(SessionID),
    message: Schema.String,
  },
  { httpApiStatus: 409 },
) {}

export class ArchiveBlockedError extends Schema.TaggedErrorClass<ArchiveBlockedError>()(
  "SessionArchiveBlockedError",
  {
    sessionID: SessionID,
    sessionIDs: Schema.Array(SessionID),
    message: Schema.String,
  },
  { httpApiStatus: 409 },
) {}

export class InvalidArchiveTimeError extends Schema.TaggedErrorClass<InvalidArchiveTimeError>()(
  "SessionInvalidArchiveTimeError",
  {
    sessionID: SessionID,
    message: Schema.String,
  },
  { httpApiStatus: 400 },
) {}

export const ArchivePreview = Schema.Struct({
  root_id: SessionID,
  descendant_count: Schema.Finite,
  active_count: Schema.Finite,
  active_session_ids: Schema.Array(SessionID),
})
export type ArchivePreview = Schema.Schema.Type<typeof ArchivePreview>

export const ArchiveResult = Schema.Struct({
  root_id: SessionID,
  archived_ids: Schema.Array(SessionID),
  already_archived_ids: Schema.Array(SessionID),
  cancelled_ids: Schema.Array(SessionID),
  archive_time: Schema.Finite,
})
export type ArchiveResult = Schema.Schema.Type<typeof ArchiveResult>

export const UnarchiveResult = Schema.Struct({
  root_id: SessionID,
  restored_ids: Schema.Array(SessionID),
  still_archived_ids: Schema.Array(SessionID),
})
export type UnarchiveResult = Schema.Schema.Type<typeof UnarchiveResult>

export type FamilyError = FamilyMemberError | ArchiveInProgressError | ArchiveBlockedError | InvalidArchiveTimeError

const asFamilyError = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, FamilyError | NotFound, R> =>
  effect.pipe(
    Effect.catch((error) => {
      if (
        error instanceof NotFoundError ||
        error instanceof FamilyMemberError ||
        error instanceof ArchiveInProgressError ||
        error instanceof ArchiveBlockedError ||
        error instanceof InvalidArchiveTimeError
      ) {
        return Effect.fail(error)
      }
      return Effect.die(error)
    }),
  )

export type NotFound = NotFoundError

export interface Interface {
  readonly list: (input?: ListInput) => Effect.Effect<Info[]>
  readonly listGlobal: (input?: GlobalListInput) => Effect.Effect<GlobalInfo[]>
  readonly create: (input?: {
    parentID?: SessionID
    title?: string
    agent?: string
    model?: Schema.Schema.Type<typeof Model>
    metadata?: typeof Metadata.Type
    permission?: PermissionV1.Ruleset
    workspaceID?: WorkspaceV2.ID
  }) => Effect.Effect<Info>
  readonly fork: (input: { sessionID: SessionID; messageID?: MessageID }) => Effect.Effect<Info, NotFound>
  readonly touch: (sessionID: SessionID) => Effect.Effect<void>
  readonly get: (id: SessionID) => Effect.Effect<Info, NotFound>
  readonly setTitle: (input: { sessionID: SessionID; title: string }) => Effect.Effect<void>
  readonly setArchived: (input: { sessionID: SessionID; time?: number }) => Effect.Effect<void, FamilyError | NotFound>
  readonly archivePreview: (input: { sessionID: SessionID }) => Effect.Effect<ArchivePreview, FamilyError | NotFound>
  readonly archiveSubtree: (input: {
    sessionID: SessionID
    archivedAt?: number
    waitTimeoutMs?: number
    interrupt?: (sessionID: SessionID) => Effect.Effect<void>
  }) => Effect.Effect<ArchiveResult, FamilyError | NotFound>
  readonly unarchiveSubtree: (input: { sessionID: SessionID }) => Effect.Effect<UnarchiveResult, FamilyError | NotFound>
  readonly setMetadata: (input: typeof SetMetadataInput.Type) => Effect.Effect<void>
  readonly setAgentModel: (input: {
    sessionID: SessionID
    agent: string
    model: NonNullable<Info["model"]>
    time: number
  }) => Effect.Effect<void>
  readonly setPermission: (input: { sessionID: SessionID; permission: PermissionV1.Ruleset }) => Effect.Effect<void>
  readonly setRevert: (input: {
    sessionID: SessionID
    revert: Info["revert"]
    summary: Info["summary"]
  }) => Effect.Effect<void>
  readonly clearRevert: (sessionID: SessionID) => Effect.Effect<void>
  readonly setSummary: (input: { sessionID: SessionID; summary: Info["summary"] }) => Effect.Effect<void>
  readonly setShare: (input: { sessionID: SessionID; share: Info["share"] }) => Effect.Effect<void>
  readonly setWorkspace: (input: { sessionID: SessionID; workspaceID: Info["workspaceID"] }) => Effect.Effect<void>
  readonly diff: (sessionID: SessionID) => Effect.Effect<Snapshot.FileDiff[]>
  readonly messages: (input: { sessionID: SessionID; limit?: number }) => Effect.Effect<SessionV1.WithParts[], NotFound>
  readonly children: (parentID: SessionID) => Effect.Effect<Info[]>
  readonly remove: (sessionID: SessionID) => Effect.Effect<void, NotFound>
  readonly updateMessage: <T extends SessionV1.Info>(msg: T) => Effect.Effect<T>
  readonly removeMessage: (input: { sessionID: SessionID; messageID: MessageID }) => Effect.Effect<MessageID>
  readonly removePart: (input: { sessionID: SessionID; messageID: MessageID; partID: PartID }) => Effect.Effect<PartID>
  readonly getPart: (input: {
    sessionID: SessionID
    messageID: MessageID
    partID: PartID
  }) => Effect.Effect<SessionV1.Part | undefined>
  readonly updatePart: <T extends SessionV1.Part>(part: T) => Effect.Effect<T>
  readonly updatePartDelta: (input: {
    sessionID: SessionID
    messageID: MessageID
    partID: PartID
    field: string
    delta: string
  }) => Effect.Effect<void>
  /** Finds the first message matching the predicate, searching newest-first. */
  readonly findMessage: (
    sessionID: SessionID,
    predicate: (msg: SessionV1.WithParts) => boolean,
  ) => Effect.Effect<Option.Option<SessionV1.WithParts>, NotFound>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Session") {}

export const use = serviceUse(Service)

export type Patch = Omit<Partial<Info>, "time" | "share" | "summary" | "revert" | "permission"> & {
  time?: Partial<Info["time"]>
  share?: Partial<NonNullable<Info["share"]>> | null
  summary?: Info["summary"] | null
  revert?: Info["revert"] | null
  permission?: Info["permission"] | null
}

const layer: Layer.Layer<
  Service,
  never,
  BackgroundJob.Service | RuntimeFlags.Service | Database.Service | EventV2Bridge.Service | SessionStatus.Service
> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const database = yield* Database.Service
    const background = yield* BackgroundJob.Service
    const events = yield* EventV2Bridge.Service
    const flags = yield* RuntimeFlags.Service
    const status = yield* SessionStatus.Service

    const createNext = Effect.fn("Session.createNext")(function* (input: {
      id?: SessionID
      title?: string
      agent?: string
      model?: Schema.Schema.Type<typeof Model>
      parentID?: SessionID
      workspaceID?: WorkspaceV2.ID
      directory: string
      path?: string
      metadata?: typeof Metadata.Type
      permission?: PermissionV1.Ruleset
    }) {
      const ctx = yield* InstanceState.context
      const result: Info = {
        id: SessionID.descending(input.id),
        slug: Slug.create(),
        version: InstallationVersion,
        projectID: ctx.project.id,
        directory: input.directory,
        path: input.path,
        workspaceID: input.workspaceID,
        parentID: input.parentID,
        title: input.title ?? (input.parentID ? childTitlePrefix : parentTitlePrefix) + new Date().toISOString(),
        agent: input.agent,
        model: input.model,
        metadata: input.metadata,
        permission: input.permission ? [...input.permission] : undefined,
        cost: 0,
        tokens: EmptyTokens,
        time: {
          created: Date.now(),
          updated: Date.now(),
        },
      }
      yield* Effect.logInfo("created", result)

      yield* events.publish(SessionV1.Event.Created, { sessionID: result.id, info: result })

      return result
    })

    const get = Effect.fn("Session.get")(function* (id: SessionID) {
      const row = yield* db.select().from(SessionTable).where(eq(SessionTable.id, id)).get().pipe(Effect.orDie)
      if (!row) return yield* Effect.fail(new NotFoundError({ message: `Session not found: ${id}` }))
      return fromRow(row)
    })

    const list = Effect.fn("Session.list")(function* (input?: ListInput) {
      const ctx = yield* InstanceState.context
      return yield* listByProject(db, {
        projectID: ctx.project.id,
        experimentalWorkspaces: flags.experimentalWorkspaces,
        ...input,
      })
    })

    const listGlobal = Effect.fn("Session.listGlobal")(function* (input?: GlobalListInput) {
      const conditions: SQL[] = []
      if (input?.directory) conditions.push(eq(SessionTable.directory, input.directory))
      if (input?.roots) conditions.push(isNull(SessionTable.parent_id))
      if (input?.start) conditions.push(gte(SessionTable.time_updated, input.start))
      if (input?.cursor) conditions.push(lt(SessionTable.time_updated, input.cursor))
      if (input?.search) conditions.push(like(SessionTable.title, `%${input.search}%`))
      if (!input?.archived) conditions.push(isNull(SessionTable.time_archived))

      const query =
        conditions.length > 0
          ? db
              .select()
              .from(SessionTable)
              .where(and(...conditions))
          : db.select().from(SessionTable)
      const rows = yield* query
        .orderBy(desc(SessionTable.time_updated), desc(SessionTable.id))
        .limit(input?.limit ?? 100)
        .all()
        .pipe(Effect.orDie)
      const ids = [...new Set(rows.map((row) => row.project_id))]
      const projects = new Map<string, ProjectInfo>()
      if (ids.length > 0) {
        const items = yield* db
          .select({ id: ProjectTable.id, name: ProjectTable.name, worktree: ProjectTable.worktree })
          .from(ProjectTable)
          .where(inArray(ProjectTable.id, ids))
          .all()
          .pipe(Effect.orDie)
        for (const item of items) {
          projects.set(item.id, {
            id: item.id,
            name: item.name ?? undefined,
            worktree: item.worktree,
          })
        }
      }
      return rows.map((row) => ({ ...fromRow(row), project: projects.get(row.project_id) ?? null }))
    })

    const children = Effect.fn("Session.children")(function* (parentID: SessionID) {
      const rows = yield* db
        .select()
        .from(SessionTable)
        .where(and(eq(SessionTable.parent_id, parentID)))
        .all()
        .pipe(Effect.orDie)
      return rows.map(fromRow)
    })

    const remove: Interface["remove"] = Effect.fnUntraced(function* (sessionID: SessionID) {
      const session = yield* get(sessionID)
      try {
        // `remove` needs to work in all cases, such as broken sessions that
        // run cleanup without instance state.
        const hasInstance = yield* InstanceState.directory.pipe(
          Effect.as(true),
          Effect.catchCause(() => Effect.succeed(false)),
        )

        if (hasInstance) yield* cancelBackgroundJobs(background, sessionID)
        const kids = yield* children(sessionID)
        for (const child of kids) {
          yield* remove(child.id)
        }

        yield* events.publish(SessionV1.Event.Deleted, { sessionID, info: session })
        yield* events.remove(sessionID)
      } catch (error) {
        yield* Effect.logError("failed to remove session", { sessionID, error })
      }
    })

    const updateMessage = <T extends SessionV1.Info>(msg: T): Effect.Effect<T> =>
      Effect.gen(function* () {
        yield* events.publish(SessionV1.Event.MessageUpdated, { sessionID: msg.sessionID, info: msg })
        return msg
      }).pipe(Effect.withSpan("Session.updateMessage"))

    const updatePart = <T extends SessionV1.Part>(part: T): Effect.Effect<T> =>
      Effect.gen(function* () {
        yield* events.publish(SessionV1.Event.PartUpdated, {
          sessionID: part.sessionID,
          part: structuredClone(part),
          time: Date.now(),
        })
        return part
      }).pipe(Effect.withSpan("Session.updatePart"))

    const getPart: Interface["getPart"] = Effect.fn("Session.getPart")(function* (input) {
      const row = yield* db
        .select()
        .from(PartTable)
        .where(
          and(
            eq(PartTable.session_id, input.sessionID),
            eq(PartTable.message_id, input.messageID),
            eq(PartTable.id, input.partID),
          ),
        )
        .get()
        .pipe(Effect.orDie)
      if (!row) return
      return {
        ...row.data,
        id: row.id,
        sessionID: row.session_id,
        messageID: row.message_id,
      } as SessionV1.Part
    })

    const create = Effect.fn("Session.create")(function* (input?: {
      parentID?: SessionID
      title?: string
      agent?: string
      model?: Schema.Schema.Type<typeof Model>
      metadata?: typeof Metadata.Type
      permission?: PermissionV1.Ruleset
      workspaceID?: WorkspaceV2.ID
    }) {
      const ctx = yield* InstanceState.context
      const workspace = yield* InstanceState.workspaceID
      return yield* createNext({
        parentID: input?.parentID,
        directory: ctx.directory,
        path: sessionPath(ctx.worktree, ctx.directory),
        title: input?.title,
        agent: input?.agent,
        model: input?.model,
        metadata: input?.metadata,
        permission: input?.permission,
        workspaceID: input?.workspaceID ?? workspace,
      })
    })

    const fork = Effect.fn("Session.fork")(function* (input: { sessionID: SessionID; messageID?: MessageID }) {
      const ctx = yield* InstanceState.context
      const original = yield* get(input.sessionID)
      const title = getForkedTitle(original.title)
      const session = yield* createNext({
        directory: ctx.directory,
        path: sessionPath(ctx.worktree, ctx.directory),
        workspaceID: original.workspaceID,
        title,
        metadata: structuredClone(original.metadata),
      })
      const msgs = yield* messages({ sessionID: input.sessionID })
      const idMap = new Map<string, MessageID>()
      const target = input.messageID ? msgs.findIndex((msg) => msg.info.id === input.messageID) : msgs.length

      for (const msg of msgs.slice(0, target < 0 ? msgs.length : target)) {
        const newID = MessageID.ascending()
        idMap.set(msg.info.id, newID)

        const parentID = msg.info.role === "assistant" && msg.info.parentID ? idMap.get(msg.info.parentID) : undefined
        const cloned = yield* updateMessage({
          ...msg.info,
          sessionID: session.id,
          id: newID,
          ...(parentID && { parentID }),
        })

        for (const part of msg.parts) {
          const p: SessionV1.Part = {
            ...part,
            id: PartID.ascending(),
            messageID: cloned.id,
            sessionID: session.id,
          }
          if (p.type === "compaction" && p.tail_start_id) {
            p.tail_start_id = idMap.get(p.tail_start_id)
          }
          yield* updatePart(p)
        }
      }
      return session
    })

    const patch = (sessionID: SessionID, info: Patch) =>
      Effect.gen(function* () {
        const current = yield* get(sessionID)
        const next = {
          ...current,
          ...info,
          time: info.time ? { ...current.time, ...info.time } : current.time,
          share: info.share === null ? undefined : info.share ? { ...current.share, ...info.share } : current.share,
          summary: info.summary === null ? undefined : (info.summary ?? current.summary),
          revert: info.revert === null ? undefined : (info.revert ?? current.revert),
          permission: info.permission === null ? undefined : (info.permission ?? current.permission),
        } as Info
        yield* events.publish(SessionV1.Event.Updated, { sessionID, info: next })
      })

    const touch = Effect.fn("Session.touch")(function* (sessionID: SessionID) {
      yield* patch(sessionID, { time: { updated: Date.now() } }).pipe(Effect.orDie)
    })

    const setTitle = Effect.fn("Session.setTitle")(function* (input: { sessionID: SessionID; title: string }) {
      yield* patch(input.sessionID, { title: input.title }).pipe(Effect.orDie)
    })

    type Tx = Parameters<Parameters<typeof db.transaction>[0]>[0]
    type ArchiveOverride = {
      archived?: number | null
      familyArchiveID?: SessionID | null
      archiveCause?: "user" | "cascade" | "migration" | null
      archiveState?: "archiving" | null
      archiveStateAt?: number | null
    }

    const LIVE_LEASE_MS = 60_000

    const subtreeIDs = (tx: Tx, rootID: SessionID) =>
      tx
        .all<{ id: string }>(
          sql`
          WITH RECURSIVE subtree(id) AS (
            SELECT id FROM session WHERE id = ${rootID}
            UNION ALL
            SELECT session.id FROM session JOIN subtree ON session.parent_id = subtree.id
          )
          SELECT id FROM subtree
        `,
        )
        .pipe(Effect.orDie)

    const loadSubtree = (tx: Tx, rootID: SessionID) =>
      Effect.gen(function* () {
        const ids = yield* subtreeIDs(tx, rootID)
        if (ids.length === 0) return [] as SessionRow[]
        return yield* tx
          .select()
          .from(SessionTable)
          .where(
            inArray(
              SessionTable.id,
              ids.map((row) => row.id as SessionID),
            ),
          )
          .all()
          .pipe(Effect.orDie)
      })

    const orderSubtree = (rows: SessionRow[]) => {
      const byID = new Map(rows.map((row) => [row.id, row]))
      const depth = (row: SessionRow) => {
        let value = 0
        let parent = row.parent_id
        const seen = new Set<string>([row.id])
        while (parent && byID.has(parent) && !seen.has(parent)) {
          seen.add(parent)
          value += 1
          parent = byID.get(parent)!.parent_id
        }
        return value
      }
      return [...rows].sort((a, b) => depth(a) - depth(b) || a.id.localeCompare(b.id))
    }

    const liveLeases = (tx: Tx, ids: SessionID[]) =>
      ids.length === 0
        ? Effect.succeed([])
        : tx
            .select()
            .from(SessionRunLeaseTable)
            .where(
              and(
                inArray(SessionRunLeaseTable.session_id, ids),
                gte(SessionRunLeaseTable.heartbeat_at, Date.now() - LIVE_LEASE_MS),
              ),
            )
            .all()
            .pipe(Effect.orDie)

    const infoFor = (row: SessionRow, override?: ArchiveOverride): Info => {
      const base = fromRow(row)
      return {
        ...base,
        time: override?.archived !== undefined ? { ...base.time, archived: override.archived } : base.time,
        ...(override?.familyArchiveID !== undefined ? { familyArchiveID: override.familyArchiveID } : {}),
        ...(override?.archiveCause !== undefined ? { archiveCause: override.archiveCause } : {}),
        ...(override?.archiveState !== undefined ? { archiveState: override.archiveState } : {}),
        ...(override?.archiveStateAt !== undefined ? { archiveStateAt: override.archiveStateAt } : {}),
      } as Info
    }

    // Publish inside the caller's immediate transaction. The guard rows are visible to the
    // projector and are removed before the caller commits.
    const publishRows = (tx: Tx, rows: { row: SessionRow; override?: ArchiveOverride }[]) =>
      Effect.gen(function* () {
        if (rows.length === 0) return
        const ids = rows.map((entry) => entry.row.id as SessionID)
        yield* tx
          .insert(SessionArchiveGuardTable)
          .values(rows.map((entry) => ({ session_id: entry.row.id })))
          .onConflictDoNothing()
          .run()
          .pipe(Effect.orDie)
        for (const entry of rows) {
          const info = infoFor(entry.row, entry.override)
          yield* events.publish(SessionV1.Event.Updated, {
            sessionID: info.id,
            info: info as unknown as SessionV1.SessionInfo,
          })
        }
        yield* tx
          .delete(SessionArchiveGuardTable)
          .where(inArray(SessionArchiveGuardTable.session_id, ids))
          .run()
          .pipe(Effect.orDie)
      })

    const setArchived = Effect.fn("Session.setArchived")(function* (input: { sessionID: SessionID; time?: number }) {
      yield* asFamilyError(
        db.transaction(
          (tx) =>
            Effect.gen(function* () {
              const row = yield* tx
                .select()
                .from(SessionTable)
                .where(eq(SessionTable.id, input.sessionID))
                .get()
                .pipe(Effect.orDie)
              if (!row)
                return yield* Effect.fail(new NotFoundError({ message: `Session not found: ${input.sessionID}` }))
              if (
                input.time !== undefined &&
                row.archive_cause !== null &&
                row.archive_cause !== "user" &&
                row.family_archive_id !== null &&
                row.family_archive_id !== row.id
              ) {
                return yield* Effect.fail(
                  new FamilyMemberError({
                    sessionID: row.id,
                    familyRootID: row.family_archive_id,
                    message: `Session ${row.id} belongs to family ${row.family_archive_id}; archive the family root instead`,
                  }),
                )
              }
              yield* tx
                .insert(SessionArchiveGuardTable)
                .values({ session_id: input.sessionID })
                .onConflictDoNothing()
                .run()
              yield* patch(input.sessionID, {
                time: { archived: input.time ?? null },
                archiveCause: input.time !== undefined ? "user" : null,
                familyArchiveID: null,
                archiveState: null,
                archiveStateAt: null,
              }).pipe(Effect.orDie)
              yield* tx
                .delete(SessionArchiveGuardTable)
                .where(eq(SessionArchiveGuardTable.session_id, input.sessionID))
                .run()
            }),
          { behavior: "immediate" },
        ),
      )
    })

    const isDirectFamilyMember = (row: SessionRow) =>
      row.archive_cause !== null &&
      row.archive_cause !== "user" &&
      row.family_archive_id !== null &&
      row.family_archive_id !== row.id

    const archivePreview = Effect.fn("Session.archivePreview")(function* (input: { sessionID: SessionID }) {
      const root = yield* db
        .select()
        .from(SessionTable)
        .where(eq(SessionTable.id, input.sessionID))
        .get()
        .pipe(Effect.orDie)
      if (!root) return yield* Effect.fail(new NotFoundError({ message: `Session not found: ${input.sessionID}` }))
      if (isDirectFamilyMember(root)) {
        return yield* Effect.fail(
          new FamilyMemberError({
            sessionID: root.id,
            familyRootID: root.family_archive_id!,
            message: `Session ${root.id} belongs to family ${root.family_archive_id}; preview the family root instead`,
          }),
        )
      }
      const rows = yield* loadSubtree(db as unknown as Tx, root.id)
      const ids = rows.map((row) => row.id as SessionID)
      const leases = yield* liveLeases(db as unknown as Tx, ids)
      const local = new Set<string>([...((yield* status.list()).keys() as Iterable<string>)])
      const active = new Set<string>(leases.map((lease) => lease.session_id))
      for (const id of ids) if (local.has(id)) active.add(id)
      if (local.has(root.id)) active.add(root.id)
      const activeIDs = [...active].filter((id) => ids.includes(id as SessionID)).sort()
      return {
        root_id: root.id,
        descendant_count: Math.max(rows.length - 1, 0),
        active_count: activeIDs.length,
        active_session_ids: activeIDs as SessionID[],
      }
    })

    const archiveSubtree = Effect.fn("Session.archiveSubtree")(function* (input: {
      sessionID: SessionID
      archivedAt?: number
      waitTimeoutMs?: number
      interrupt?: (sessionID: SessionID) => Effect.Effect<void>
    }) {
      const now = Date.now()

      const step1 = yield* asFamilyError(
        db.transaction(
          (tx) =>
            Effect.gen(function* () {
              const root = yield* tx
                .select()
                .from(SessionTable)
                .where(eq(SessionTable.id, input.sessionID))
                .get()
                .pipe(Effect.orDie)
              if (!root)
                return yield* Effect.fail(new NotFoundError({ message: `Session not found: ${input.sessionID}` }))
              if (isDirectFamilyMember(root)) {
                return yield* Effect.fail(
                  new FamilyMemberError({
                    sessionID: root.id,
                    familyRootID: root.family_archive_id!,
                    message: `Session ${root.id} belongs to family ${root.family_archive_id}; archive the family root instead`,
                  }),
                )
              }
              const rows = yield* loadSubtree(tx, root.id)
              const archiving = rows.filter((row) => row.archive_state === "archiving")
              if (archiving.length > 0) {
                return yield* Effect.fail(
                  new ArchiveInProgressError({
                    sessionID: root.id,
                    sessionIDs: archiving.map((row) => row.id as SessionID),
                    message: `Archive already in progress for ${archiving.map((row) => row.id).join(", ")}`,
                  }),
                )
              }
              const allArchived = rows.every((row) => row.time_archived !== null)
              const leases = yield* liveLeases(
                tx,
                rows.map((row) => row.id as SessionID),
              )

              let archiveTime = root.time_archived ?? now
              if (root.time_archived === null && input.archivedAt !== undefined) {
                if (input.archivedAt < root.time_created) {
                  return yield* Effect.fail(
                    new InvalidArchiveTimeError({
                      sessionID: root.id,
                      message: `archive time ${input.archivedAt} precedes session creation ${root.time_created}`,
                    }),
                  )
                }
                archiveTime = Math.min(input.archivedAt, now)
              }

              if (allArchived && leases.length === 0) {
                return { shortCircuit: true as const, rows, archiveTime, leases }
              }

              // A fully archived family still walks the cancellation path, but keeps its archive
              // fields and timestamps untouched.
              const marked = allArchived ? rows : rows.filter((row) => row.time_archived === null)
              if (marked.length > 0) {
                yield* tx
                  .update(SessionTable)
                  .set({ archive_state: "archiving", archive_state_at: now })
                  .where(
                    inArray(
                      SessionTable.id,
                      marked.map((row) => row.id as SessionID),
                    ),
                  )
                  .run()
                  .pipe(Effect.orDie)
                yield* publishRows(
                  tx,
                  marked.map((row) => ({
                    row,
                    override: { archiveState: "archiving", archiveStateAt: now },
                  })),
                )
              }
              return { shortCircuit: false as const, rows, archiveTime, leases }
            }),
          { behavior: "immediate" },
        ),
      )

      if (step1.shortCircuit) {
        return {
          root_id: input.sessionID,
          archived_ids: [],
          already_archived_ids: step1.rows.map((row) => row.id),
          cancelled_ids: [],
          archive_time: step1.archiveTime,
        }
      }

      const subtree = step1.rows.map((row) => row.id as SessionID)
      const localActive = new Set<string>([...((yield* status.list()).keys() as Iterable<string>)])
      const localCancelled = subtree.filter((sessionID) => localActive.has(sessionID))
      const cancelled = [...new Set<SessionID>([...step1.leases.map((lease) => lease.session_id), ...localCancelled])]

      if (step1.leases.length > 0 || localCancelled.length > 0) {
        yield* db
          .update(SessionRunLeaseTable)
          .set({ cancel_requested: 1 })
          .where(
            inArray(
              SessionRunLeaseTable.session_id,
              step1.leases.map((lease) => lease.session_id as SessionID),
            ),
          )
          .run()
          .pipe(Effect.orDie)
        if (input.interrupt) {
          const interrupt = input.interrupt
          yield* Effect.forEach(
            subtree,
            (sessionID) => interrupt(sessionID).pipe(Effect.catchCause(() => Effect.void)),
            { concurrency: "unbounded", discard: true },
          )
        }

        if (step1.leases.length > 0) {
          const timeout = input.waitTimeoutMs ?? 30_000
          const deadline = Date.now() + timeout
          let remaining = yield* liveLeases(db as unknown as Tx, subtree)
          while (remaining.length > 0 && Date.now() < deadline) {
            yield* Effect.sleep("25 millis")
            remaining = yield* liveLeases(db as unknown as Tx, subtree)
          }

          if (remaining.length > 0) {
            const blocking = remaining.map((lease) => lease.session_id as SessionID)
            yield* asFamilyError(
              db.transaction(
                (tx) =>
                  Effect.gen(function* () {
                    const marked = step1.rows
                    if (marked.length > 0) {
                      yield* tx
                        .update(SessionTable)
                        .set({ archive_state: null, archive_state_at: null })
                        .where(
                          inArray(
                            SessionTable.id,
                            marked.map((row) => row.id as SessionID),
                          ),
                        )
                        .run()
                        .pipe(Effect.orDie)
                      yield* publishRows(
                        tx,
                        marked.map((row) => ({ row, override: { archiveState: null, archiveStateAt: null } })),
                      )
                    }
                    for (const lease of remaining) {
                      yield* tx
                        .update(SessionRunLeaseTable)
                        .set({ cancel_requested: 0 })
                        .where(eq(SessionRunLeaseTable.session_id, lease.session_id))
                        .run()
                        .pipe(Effect.orDie)
                    }
                  }),
                { behavior: "immediate" },
              ),
            )
            return yield* Effect.fail(
              new ArchiveBlockedError({
                sessionID: input.sessionID,
                sessionIDs: blocking,
                message: `Active sessions did not stop: ${blocking.join(", ")}`,
              }),
            )
          }
        }
      }

      return yield* asFamilyError(
        db.transaction(
          (tx) =>
            Effect.gen(function* () {
              const root = yield* tx
                .select()
                .from(SessionTable)
                .where(eq(SessionTable.id, input.sessionID))
                .get()
                .pipe(Effect.orDie)
              if (!root)
                return yield* Effect.fail(new NotFoundError({ message: `Session not found: ${input.sessionID}` }))
              const rows = orderSubtree(yield* loadSubtree(tx, root.id))
              const marked = new Set(step1.rows.map((row) => row.id))
              const finalized = new Map<string, { familyKey: string; archiveTime: number }>()
              const archivedIDs: SessionID[] = []
              const alreadyArchivedIDs: SessionID[] = []
              const changed: { row: SessionRow; override?: ArchiveOverride }[] = []

              for (const row of rows) {
                const next: Partial<SessionRow> = {}
                if (row.time_archived === null) {
                  const parent = row.parent_id ? finalized.get(row.parent_id) : undefined
                  const familyKey = row.id === root.id ? root.id : (parent?.familyKey ?? root.id)
                  const archiveTime =
                    row.id === root.id ? step1.archiveTime : (parent?.archiveTime ?? step1.archiveTime)
                  next.time_archived = archiveTime
                  next.family_archive_id = familyKey as SessionRow["family_archive_id"]
                  next.archive_cause = row.id === root.id ? (row.archive_cause ?? "user") : "cascade"
                  archivedIDs.push(row.id as SessionID)
                } else {
                  alreadyArchivedIDs.push(row.id as SessionID)
                  if (row.id === root.id) {
                    next.family_archive_id = (row.family_archive_id ?? root.id) as SessionRow["family_archive_id"]
                    next.archive_cause = row.archive_cause ?? "user"
                  }
                }
                if (marked.has(row.id)) {
                  next.archive_state = null
                  next.archive_state_at = null
                }
                if (Object.keys(next).length > 0) {
                  yield* tx
                    .update(SessionTable)
                    .set(next as never)
                    .where(eq(SessionTable.id, row.id))
                    .run()
                    .pipe(Effect.orDie)
                }
                const merged = { ...row, ...next } as SessionRow
                finalized.set(row.id, {
                  familyKey: (merged.family_archive_id ?? merged.id) as string,
                  archiveTime: merged.time_archived as number,
                })
                changed.push({
                  row: merged,
                  override: { archiveState: null, archiveStateAt: null },
                })
              }

              yield* publishRows(tx, changed)
              yield* tx
                .insert(FamilyArchiveMigrationTable)
                .values({ root_session_id: root.id, root_archived_at: step1.archiveTime, applied_at: now })
                .onConflictDoUpdate({
                  target: FamilyArchiveMigrationTable.root_session_id,
                  set: { root_archived_at: step1.archiveTime, applied_at: now },
                })
                .run()
                .pipe(Effect.orDie)

              return {
                root_id: root.id,
                archived_ids: archivedIDs,
                already_archived_ids: alreadyArchivedIDs,
                cancelled_ids: cancelled,
                archive_time: step1.archiveTime,
              }
            }),
          { behavior: "immediate" },
        ),
      )
    })

    const unarchiveSubtree = Effect.fn("Session.unarchiveSubtree")(function* (input: { sessionID: SessionID }) {
      return yield* asFamilyError(
        db.transaction(
          (tx) =>
            Effect.gen(function* () {
              const root = yield* tx
                .select()
                .from(SessionTable)
                .where(eq(SessionTable.id, input.sessionID))
                .get()
                .pipe(Effect.orDie)
              if (!root)
                return yield* Effect.fail(new NotFoundError({ message: `Session not found: ${input.sessionID}` }))

              let missingFamilyRoot: string | undefined
              if (root.family_archive_id !== null && root.family_archive_id !== root.id) {
                const familyRoot = yield* tx
                  .select({ id: SessionTable.id })
                  .from(SessionTable)
                  .where(eq(SessionTable.id, root.family_archive_id))
                  .get()
                  .pipe(Effect.orDie)
                if (familyRoot) {
                  return yield* Effect.fail(
                    new FamilyMemberError({
                      sessionID: root.id,
                      familyRootID: root.family_archive_id,
                      message: `Session ${root.id} belongs to family ${root.family_archive_id}; unarchive the family root instead`,
                    }),
                  )
                }
                missingFamilyRoot = root.family_archive_id
              }

              const rows = orderSubtree(yield* loadSubtree(tx, root.id))
              const archiving = rows.filter((row) => row.archive_state === "archiving")
              if (archiving.length > 0) {
                return yield* Effect.fail(
                  new ArchiveInProgressError({
                    sessionID: root.id,
                    sessionIDs: archiving.map((row) => row.id as SessionID),
                    message: `Archive in progress for ${archiving.map((row) => row.id).join(", ")}`,
                  }),
                )
              }

              const restore = rows.filter((row) => {
                if (row.time_archived === null) return false
                if (row.id === root.id) {
                  if (missingFamilyRoot) return true
                  return row.family_archive_id === null || row.family_archive_id === root.id
                }
                if (row.archive_cause !== "cascade" && row.archive_cause !== "migration") return false
                if (missingFamilyRoot) return row.family_archive_id === missingFamilyRoot
                return row.family_archive_id === root.id
              })

              if (restore.length > 0) {
                yield* tx
                  .update(SessionTable)
                  .set({
                    time_archived: null,
                    family_archive_id: null,
                    archive_cause: null,
                    archive_state: null,
                    archive_state_at: null,
                  })
                  .where(
                    inArray(
                      SessionTable.id,
                      restore.map((row) => row.id as SessionID),
                    ),
                  )
                  .run()
                  .pipe(Effect.orDie)

                const restored = new Set(restore.map((row) => row.id))
                yield* publishRows(
                  tx,
                  rows
                    .filter((row) => restored.has(row.id))
                    .map((row) => ({
                      row: {
                        ...row,
                        time_archived: null,
                        family_archive_id: null,
                        archive_cause: null,
                        archive_state: null,
                        archive_state_at: null,
                      } as SessionRow,
                      override: {
                        archived: null,
                        familyArchiveID: null,
                        archiveCause: null,
                        archiveState: null,
                        archiveStateAt: null,
                      } satisfies ArchiveOverride,
                    })),
                )
                for (const row of rows) {
                  if (restored.has(row.id)) row.time_archived = null
                }
              }

              const stillArchived = rows.filter((row) => row.time_archived !== null).map((row) => row.id as SessionID)
              return {
                root_id: root.id,
                restored_ids: restore.map((row) => row.id as SessionID),
                still_archived_ids: stillArchived,
              }
            }),
          { behavior: "immediate" },
        ),
      )
    })

    const setMetadata = Effect.fn("Session.setMetadata")(function* (input: typeof SetMetadataInput.Type) {
      yield* patch(input.sessionID, { metadata: input.metadata, time: { updated: Date.now() } }).pipe(Effect.orDie)
    })

    const setAgentModel = Effect.fn("Session.setAgentModel")(function* (input: {
      sessionID: SessionID
      agent: string
      model: NonNullable<Info["model"]>
      time: number
    }) {
      yield* patch(input.sessionID, {
        agent: input.agent,
        model: input.model,
        time: { updated: input.time },
      }).pipe(Effect.orDie)
    })

    const setPermission = Effect.fn("Session.setPermission")(function* (input: {
      sessionID: SessionID
      permission: PermissionV1.Ruleset
    }) {
      yield* patch(input.sessionID, { permission: [...input.permission], time: { updated: Date.now() } }).pipe(
        Effect.orDie,
      )
    })

    const setRevert = Effect.fn("Session.setRevert")(function* (input: {
      sessionID: SessionID
      revert: Info["revert"]
      summary: Info["summary"]
    }) {
      yield* patch(input.sessionID, {
        summary: input.summary,
        time: { updated: Date.now() },
        revert: input.revert,
      }).pipe(Effect.orDie)
    })

    const clearRevert = Effect.fn("Session.clearRevert")(function* (sessionID: SessionID) {
      yield* patch(sessionID, { time: { updated: Date.now() }, revert: null }).pipe(Effect.orDie)
    })

    const setSummary = Effect.fn("Session.setSummary")(function* (input: {
      sessionID: SessionID
      summary: Info["summary"]
    }) {
      yield* patch(input.sessionID, { time: { updated: Date.now() }, summary: input.summary }).pipe(Effect.orDie)
    })

    const setShare = Effect.fn("Session.setShare")(function* (input: { sessionID: SessionID; share: Info["share"] }) {
      yield* patch(input.sessionID, { share: input.share ?? null, time: { updated: Date.now() } }).pipe(Effect.orDie)
    })

    const setWorkspace = Effect.fn("Session.setWorkspace")(function* (input: {
      sessionID: SessionID
      workspaceID: Info["workspaceID"]
    }) {
      yield* patch(input.sessionID, { workspaceID: input.workspaceID, time: { updated: Date.now() } }).pipe(
        Effect.orDie,
      )
    })

    const diff = Effect.fn("Session.diff")(function* (sessionID: SessionID) {
      void sessionID
      return [] as Snapshot.FileDiff[]
    })

    const messages: Interface["messages"] = Effect.fn("Session.messages")(function* (input) {
      if (input.limit) {
        return (yield* MessageV2.page({ sessionID: input.sessionID, limit: input.limit }).pipe(
          Effect.provideService(Database.Service, database),
        )).items
      }

      const size = 50
      const result = [] as SessionV1.WithParts[]
      let before: string | undefined
      while (true) {
        const page = yield* MessageV2.page({ sessionID: input.sessionID, limit: size, before }).pipe(
          Effect.provideService(Database.Service, database),
        )
        if (page.items.length === 0) break
        for (let i = page.items.length - 1; i >= 0; i--) {
          const item = page.items[i]
          if (item) result.push(item)
        }
        if (!page.more || !page.cursor) break
        before = page.cursor
      }
      return result.reverse()
    })

    const removeMessage = Effect.fn("Session.removeMessage")(function* (input: {
      sessionID: SessionID
      messageID: MessageID
    }) {
      yield* events.publish(SessionV1.Event.MessageRemoved, {
        sessionID: input.sessionID,
        messageID: input.messageID,
      })
      return input.messageID
    })

    const removePart = Effect.fn("Session.removePart")(function* (input: {
      sessionID: SessionID
      messageID: MessageID
      partID: PartID
    }) {
      yield* events.publish(SessionV1.Event.PartRemoved, {
        sessionID: input.sessionID,
        messageID: input.messageID,
        partID: input.partID,
      })
      return input.partID
    })

    const updatePartDelta = Effect.fnUntraced(function* (input: {
      sessionID: SessionID
      messageID: MessageID
      partID: PartID
      field: string
      delta: string
    }) {
      yield* events.publish(MessageV2.Event.PartDelta, input)
    })

    /** Finds the first message matching the predicate, searching newest-first. */
    const findMessage: Interface["findMessage"] = Effect.fn("Session.findMessage")(function* (sessionID, predicate) {
      const size = 50
      let before: string | undefined
      while (true) {
        const page = yield* MessageV2.page({ sessionID, limit: size, before }).pipe(
          Effect.provideService(Database.Service, database),
        )
        if (page.items.length === 0) break
        for (let i = page.items.length - 1; i >= 0; i--) {
          const item = page.items[i]
          if (item && predicate(item)) return Option.some(item)
        }
        if (!page.more || !page.cursor) break
        before = page.cursor
      }
      return Option.none<SessionV1.WithParts>()
    })

    return Service.of({
      list,
      listGlobal,
      create,
      fork,
      touch,
      get,
      setTitle,
      setArchived,
      archivePreview,
      archiveSubtree,
      unarchiveSubtree,
      setMetadata,
      setAgentModel,
      setPermission,
      setRevert,
      clearRevert,
      setSummary,
      setShare,
      setWorkspace,
      diff,
      messages,
      children,
      remove,
      updateMessage,
      removeMessage,
      removePart,
      updatePart,
      getPart,
      updatePartDelta,
      findMessage,
    })
  }),
)

const cancelBackgroundJobs = Effect.fn("Session.cancelBackgroundJobs")(function* (
  background: BackgroundJob.Interface,
  sessionID: SessionID,
) {
  const jobs = yield* background.list()
  yield* Effect.forEach(
    jobs.filter((job) => {
      if (job.status !== "running") return false
      if (job.id === sessionID) return true
      if (job.metadata?.sessionId === sessionID) return true
      return job.metadata?.parentSessionId === sessionID
    }),
    (job) => background.cancel(job.id),
    { concurrency: "unbounded", discard: true },
  )
})

function listByProject(
  db: Database.Interface["db"],
  input: ListInput & {
    projectID: ProjectV2.ID
    experimentalWorkspaces: boolean
  },
) {
  const conditions = [eq(SessionTable.project_id, input.projectID)]

  if (input.workspaceID) {
    conditions.push(eq(SessionTable.workspace_id, input.workspaceID))
  }
  if (input.path !== undefined) {
    if (input.path) {
      const conds = [
        eq(SessionTable.path, input.path),
        like(SessionTable.path, sql.param(`${input.path}/%`, SessionTable.path)),
      ]

      conditions.push(
        input.directory
          ? or(...conds, and(isNull(SessionTable.path), eq(SessionTable.directory, input.directory))!)!
          : or(...conds)!,
      )
    }
  } else if (input.scope !== "project") {
    if (input.directory) {
      conditions.push(eq(SessionTable.directory, input.directory))
    }
  }
  if (input.roots) {
    conditions.push(isNull(SessionTable.parent_id))
  }
  if (input.start) {
    conditions.push(gte(SessionTable.time_updated, input.start))
  }
  if (input.search) {
    conditions.push(like(SessionTable.title, `%${input.search}%`))
  }

  const limit = input.limit ?? 100

  return db
    .select()
    .from(SessionTable)
    .where(and(...conditions))
    .orderBy(desc(SessionTable.time_updated))
    .limit(limit)
    .all()
    .pipe(
      Effect.orDie,
      Effect.map((rows) => rows.map(fromRow)),
    )
}

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [BackgroundJob.node, RuntimeFlags.node, Database.node, EventV2Bridge.node, SessionStatus.node],
})

export * as Session from "./session"
