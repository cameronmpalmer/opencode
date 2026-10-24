import { sqliteTable, text, integer, index, primaryKey, real, uniqueIndex } from "drizzle-orm/sqlite-core"
import { sql } from "drizzle-orm"
import * as DatabasePath from "../database/path"
import { ProjectTable } from "../project/sql"
import type { SessionMessage } from "./message"
import type { Prompt } from "./prompt"
import type { SessionInput } from "./input"
import type { Snapshot } from "../snapshot"
import { PermissionV1 } from "../v1/permission"
import { ProjectV2 } from "../project"
import type { SessionSchema } from "./schema"
import type { MessageID, PartID, SessionV1 } from "../v1/session"
import { WorkspaceV2 } from "../workspace"
import { Timestamps } from "../database/schema.sql"
import type { SystemContext } from "../system-context/index"
import { AgentV2 } from "../agent"
import type { Revert } from "@opencode-ai/schema/revert"

type SessionMessageData = Omit<(typeof SessionMessage.Message)["Encoded"], "type" | "id">
type V1MessageData = Omit<SessionV1.Info, "id" | "sessionID">
type V1PartData = Omit<SessionV1.Part, "id" | "sessionID" | "messageID">

export const SessionTable = sqliteTable(
  "session",
  {
    id: text().$type<SessionSchema.ID>().primaryKey(),
    project_id: text()
      .$type<ProjectV2.ID>()
      .notNull()
      .references(() => ProjectTable.id, { onDelete: "cascade" }),
    workspace_id: text().$type<WorkspaceV2.ID>(),
    parent_id: text().$type<SessionSchema.ID>(),
    slug: text().notNull(),
    directory: DatabasePath.directoryColumn().notNull(),
    path: DatabasePath.pathColumn(),
    title: text().notNull(),
    version: text().notNull(),
    share_url: text(),
    summary_additions: integer(),
    summary_deletions: integer(),
    summary_files: integer(),
    summary_diffs: text({ mode: "json" }).$type<Snapshot.LegacyFileDiff[]>(),
    metadata: text({ mode: "json" }).$type<Record<string, unknown>>(),
    cost: real().notNull().default(0),
    tokens_input: integer().notNull().default(0),
    tokens_output: integer().notNull().default(0),
    tokens_reasoning: integer().notNull().default(0),
    tokens_cache_read: integer().notNull().default(0),
    tokens_cache_write: integer().notNull().default(0),
    revert: text({ mode: "json" }).$type<Revert.State>(),
    permission: text({ mode: "json" }).$type<PermissionV1.Ruleset>(),
    agent: text(),
    model: text({ mode: "json" }).$type<{
      id: string
      providerID: string
      variant?: string
    }>(),
    ...Timestamps,
    time_compacting: integer(),
    time_archived: integer(),
    family_archive_id: text().$type<SessionSchema.ID>(),
    archive_cause: text().$type<"user" | "cascade" | "migration">(),
    archive_state: text().$type<"archiving">(),
    archive_state_at: integer(),
  },
  (table) => [
    index("session_project_idx").on(table.project_id),
    index("session_workspace_idx").on(table.workspace_id),
    index("session_parent_idx").on(table.parent_id),
    index("session_family_archive_idx").on(table.family_archive_id),
    index("session_archive_state_idx").on(table.archive_state),
  ],
)

export const SessionRunLeaseTable = sqliteTable(
  "session_run_lease",
  {
    session_id: text().$type<SessionSchema.ID>().primaryKey(),
    lease_id: text().notNull(),
    heartbeat_at: integer().notNull(),
    cancel_requested: integer().notNull().default(0),
  },
  (table) => [index("session_run_lease_heartbeat_idx").on(table.heartbeat_at)],
)

export const SessionImportExemptTable = sqliteTable("session_import_exempt", {
  session_id: text().$type<SessionSchema.ID>().primaryKey(),
  expires_at: integer().notNull(),
})

export const SessionArchiveGuardTable = sqliteTable("session_archive_guard", {
  session_id: text().$type<SessionSchema.ID>().primaryKey(),
})

export const FamilyArchiveMigrationTable = sqliteTable("family_archive_migration", {
  root_session_id: text().$type<SessionSchema.ID>().primaryKey(),
  root_archived_at: integer().notNull(),
  applied_at: integer().notNull(),
})

export const MessageTable = sqliteTable(
  "message",
  {
    id: text().$type<MessageID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    ...Timestamps,
    data: text({ mode: "json" }).notNull().$type<V1MessageData>(),
  },
  (table) => [index("message_session_time_created_id_idx").on(table.session_id, table.time_created, table.id)],
)

export const PartTable = sqliteTable(
  "part",
  {
    id: text().$type<PartID>().primaryKey(),
    message_id: text()
      .$type<MessageID>()
      .notNull()
      .references(() => MessageTable.id, { onDelete: "cascade" }),
    session_id: text().$type<SessionSchema.ID>().notNull(),
    ...Timestamps,
    data: text({ mode: "json" }).notNull().$type<V1PartData>(),
  },
  (table) => [
    index("part_message_id_id_idx").on(table.message_id, table.id),
    index("part_session_idx").on(table.session_id),
  ],
)

export const TodoTable = sqliteTable(
  "todo",
  {
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    content: text().notNull(),
    status: text().notNull(),
    priority: text().notNull(),
    position: integer().notNull(),
    ...Timestamps,
  },
  (table) => [
    primaryKey({ columns: [table.session_id, table.position] }),
    index("todo_session_idx").on(table.session_id),
  ],
)

export const SessionMessageTable = sqliteTable(
  "session_message",
  {
    id: text().$type<SessionMessage.ID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    type: text().$type<SessionMessage.Type>().notNull(),
    seq: integer().notNull(),
    ...Timestamps,
    data: text({ mode: "json" }).notNull().$type<SessionMessageData>(),
  },
  (table) => [
    uniqueIndex("session_message_session_seq_idx").on(table.session_id, table.seq),
    index("session_message_session_type_seq_idx").on(table.session_id, table.type, table.seq),
    index("session_message_session_time_created_id_idx").on(table.session_id, table.time_created, table.id),
    index("session_message_time_created_idx").on(table.time_created),
  ],
)

export const SessionInputTable = sqliteTable(
  "session_input",
  {
    id: text().$type<SessionMessage.ID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    prompt: text({ mode: "json" }).notNull().$type<Prompt>(),
    delivery: text().$type<SessionInput.Delivery>().notNull(),
    admitted_seq: integer().notNull(),
    promoted_seq: integer(),
    time_created: integer()
      .notNull()
      .$default(() => Date.now()),
  },
  (table) => [
    index("session_input_session_pending_delivery_seq_idx").on(
      table.session_id,
      table.promoted_seq,
      table.delivery,
      table.admitted_seq,
    ),
    uniqueIndex("session_input_session_admitted_seq_idx").on(table.session_id, table.admitted_seq),
    uniqueIndex("session_input_session_promoted_seq_idx").on(table.session_id, table.promoted_seq),
  ],
)

export const SessionContextEpochTable = sqliteTable("session_context_epoch", {
  session_id: text()
    .$type<SessionSchema.ID>()
    .primaryKey()
    .references(() => SessionTable.id, { onDelete: "cascade" }),
  baseline: text().notNull(),
  snapshot: text({ mode: "json" }).notNull().$type<SystemContext.Snapshot>(),
  baseline_seq: integer().notNull(),
})

/**
 * The SQL definition of the effective family key (design 6.1), shared by every
 * query that groups or gates sessions by family: `familyMemberCount`, the purge
 * gate, and the browser aggregate. Do not re-derive this rule in a caller and do
 * not replace it with `coalesce(family_archive_id, id)`.
 *
 * Yields one row per session with `session_id` and `family_key`:
 *
 * - a non-null `family_archive_id` whose root row exists wins;
 * - a `user`-cause row is its own family;
 * - otherwise the row inherits its nearest archived ancestor's effective key
 *   (recursively, through legacy null/null ancestors);
 * - otherwise it falls back to its own ID.
 *
 * The TypeScript twin is `effectiveFamilyKey` in `schema.ts`; keep them in sync.
 */
export const effectiveFamilyKeySql = sql`
  WITH RECURSIVE
    session_ancestor(id, ancestor_id, depth) AS (
      SELECT id, parent_id, 1
      FROM session
      WHERE parent_id IS NOT NULL
      UNION
      SELECT session_ancestor.id, session.parent_id, session_ancestor.depth + 1
      FROM session_ancestor
      JOIN session ON session.id = session_ancestor.ancestor_id
      WHERE session.parent_id IS NOT NULL
    ),
    family_candidate(id, family_key, priority) AS (
      SELECT session.id,
             CASE
               WHEN family_root.id IS NOT NULL THEN session.family_archive_id
               WHEN session.archive_cause = 'user' THEN session.id
             END,
             0
      FROM session
      LEFT JOIN session AS family_root ON family_root.id = session.family_archive_id
      UNION ALL
      SELECT session_ancestor.id,
             CASE
               WHEN family_root.id IS NOT NULL THEN session.family_archive_id
               WHEN session.archive_cause = 'user' THEN session.id
             END,
             session_ancestor.depth
      FROM session_ancestor
      JOIN session ON session.id = session_ancestor.ancestor_id
      LEFT JOIN session AS family_root ON family_root.id = session.family_archive_id
      WHERE session.time_archived IS NOT NULL
    ),
    resolved_candidate AS (
      SELECT id, family_key
      FROM (
        SELECT id, family_key,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY priority) AS rank
        FROM family_candidate
        WHERE family_key IS NOT NULL
      )
      WHERE rank = 1
    ),
    fallback_ancestor AS (
      SELECT id, ancestor_id
      FROM (
        SELECT session_ancestor.id, session_ancestor.ancestor_id,
               ROW_NUMBER() OVER (PARTITION BY session_ancestor.id ORDER BY session_ancestor.depth DESC) AS rank
        FROM session_ancestor
        JOIN session ON session.id = session_ancestor.ancestor_id
        WHERE session.time_archived IS NOT NULL
      )
      WHERE rank = 1
    )
  SELECT session.id AS session_id,
         COALESCE(resolved_candidate.family_key, fallback_ancestor.ancestor_id, session.id) AS family_key
  FROM session
  LEFT JOIN resolved_candidate ON resolved_candidate.id = session.id
  LEFT JOIN fallback_ancestor ON fallback_ancestor.id = session.id
`
