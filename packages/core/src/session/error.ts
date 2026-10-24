import { Schema } from "effect"
import { SessionMessage } from "./message"
import { SessionSchema } from "./schema"

export class MessageDecodeError extends Schema.TaggedErrorClass<MessageDecodeError>()("Session.MessageDecodeError", {
  sessionID: SessionSchema.ID,
  messageID: SessionMessage.ID,
}) {
  override get message() {
    return `Failed to decode message ${this.messageID} in session ${this.sessionID}`
  }
}

export class ContextSnapshotDecodeError extends Schema.TaggedErrorClass<ContextSnapshotDecodeError>()(
  "Session.ContextSnapshotDecodeError",
  {
    sessionID: SessionSchema.ID,
    details: Schema.String,
  },
) {
  override get message() {
    return `Failed to decode context snapshot for session ${this.sessionID}: ${this.details}`
  }
}

/** A run stopped because its session was archived. Reused by projector rejections in A5. */
export class SessionArchivedError extends Schema.TaggedErrorClass<SessionArchivedError>()(
  "Session.SessionArchivedError",
  {
    sessionID: SessionSchema.ID,
    message: Schema.String,
  },
) {}

/** Another process holds a live run lease for the session. */
export class SessionLeaseBusyError extends Schema.TaggedErrorClass<SessionLeaseBusyError>()(
  "Session.SessionLeaseBusyError",
  {
    sessionID: SessionSchema.ID,
    message: Schema.String,
  },
) {}
