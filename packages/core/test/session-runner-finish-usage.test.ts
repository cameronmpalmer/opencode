import { expect, test } from "bun:test"
import { LanguageModel, LLM, LLMEvent } from "@opencode/ai/effect"
import { OpenAIChat } from "@opencode/ai/protocols/openai-chat"
import { TestLLM } from "@opencode/ai/testing"
import { Agent } from "@opencode/core/agent"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { Model } from "@opencode/core/model"
import { Provider } from "@opencode/core/provider"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { createLLMEventPublisher } from "@opencode/core/session/runner/publish-llm-event"
import { SessionStep } from "@opencode/core/session/runner/step"
import { SessionMessageTable, SessionTable } from "@opencode/core/session/sql"
import { Snapshot } from "@opencode/core/snapshot"
import { ToolOutput } from "@opencode/core/tool-output"
import { Event } from "@opencode/schema/event"
import { Money } from "@opencode/schema/money"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { eq } from "drizzle-orm"
import { Effect, Layer } from "effect"
import { testEffect } from "./lib/effect"

const it = testEffect(
  Layer.merge(
    AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node, SessionProjector.node, ToolOutput.node]), [
      Bus.node.replace(Bus.configured({ persist: true })),
    ]),
    TestLLM.testLayer(),
  ),
)

const model = SessionRunnerModel.resolved(
  LanguageModel.make({ id: "test-model", provider: "test", route: OpenAIChat.route }),
  {
    capabilities: { tools: true, input: ["text"], output: ["text"] },
    limit: { context: 100_000, output: 1_000 },
    cost: [
      {
        input: Money.USDPerMillionTokens.make(1),
        output: Money.USDPerMillionTokens.make(2),
        cache: { read: Money.USDPerMillionTokens.make(0.1), write: Money.USDPerMillionTokens.make(0.5) },
      },
    ],
  },
)

const capture = () => {
  const published: Array<{ readonly type: string; readonly data: unknown }> = []
  const bus: Pick<Bus.Interface, "publish"> = {
    publish: (definition, data) =>
      Effect.sync(() => {
        const event = { id: Event.ID.create(), type: definition.type, data } as Event.Payload<typeof definition>
        published.push({ type: definition.type, data })
        return event
      }),
  }
  return {
    published,
    publisher: createLLMEventPublisher(bus, {
      sessionID: Session.ID.make("ses_finish_usage"),
      agent: Agent.ID.make("build"),
      model: { id: Model.ID.make("test-model"), providerID: Provider.ID.opencode },
      providerMetadataKey: "openai",
      started: 0,
      assistantMessageID: SessionMessage.ID.create(),
    }),
  }
}

it.effect("records usage delivered only on the finish event", () =>
  Effect.gen(function* () {
    const db = (yield* Database.Service).db
    const llm = yield* TestLLM.Test
    const sessionID = Session.ID.create()
    const assistantMessageID = SessionMessage.ID.create()
    const steps = yield* SessionStep.make.pipe(
      Effect.provide(
        Layer.mock(Snapshot.Service)({
          capture: () => Effect.succeed(undefined),
          files: () => Effect.succeed([]),
        }),
      ),
    )
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .run()
    yield* db
      .insert(SessionTable)
      .values({ id: sessionID, project_id: Project.ID.global, slug: "finish", directory: "/project", version: "test" })
      .run()
    yield* llm.push([
      LLMEvent.stepStart({ index: 0 }),
      LLMEvent.textStart({ id: "text" }),
      LLMEvent.textDelta({ id: "text", text: "Hello" }),
      LLMEvent.textEnd({ id: "text" }),
      LLMEvent.finish({
        reason: { normalized: "stop" },
        usage: {
          inputTokens: 15,
          outputTokens: 6,
          nonCachedInputTokens: 10,
          cacheReadInputTokens: 3,
          cacheWriteInputTokens: 2,
          reasoningTokens: 2,
        },
      }),
    ])
    const result = yield* steps.attempt({
      isLocationClosed: () => false,
      sessionID,
      assistantMessageID,
      agent: Agent.defaultID,
      model,
      prepared: {
        retry: () => Effect.void,
        request: LLM.request({ model: model.model, prompt: "Hello" }),
        options: {},
        executeTool: () => Effect.die("no tools"),
      },
      retry: () => Effect.succeed({ retry: false }),
      recoverContinuation: true,
      recoverOverflow: Effect.succeed(false),
    })

    expect(result).toEqual(SessionStep.Outcome.Completed({ needsContinuation: false }))
    const message = yield* db
      .select()
      .from(SessionMessageTable)
      .where(eq(SessionMessageTable.id, assistantMessageID))
      .get()
    expect(message?.data).toMatchObject({
      finish: "stop",
      tokens: { input: 10, output: 4, reasoning: 2, cache: { read: 3, write: 2 } },
      content: [{ type: "text", text: "Hello" }],
    })
  }),
)

test("finish usage fills a step settlement that reported none", async () => {
  const { publisher } = capture()
  await Effect.runPromise(publisher.publish(LLMEvent.stepStart({ index: 0 })))
  await Effect.runPromise(
    publisher.publish(
      LLMEvent.stepFinish({
        index: 0,
        reason: { normalized: "stop", raw: "end_turn" },
        providerMetadata: { openai: { responseId: "step" } },
      }),
    ),
  )
  await Effect.runPromise(
    publisher.publish(
      LLMEvent.finish({
        reason: { normalized: "stop", raw: "end_turn" },
        providerMetadata: { openai: { responseId: "finish" } },
        usage: { nonCachedInputTokens: 10, outputTokens: 4 },
      }),
    ),
  )

  expect(publisher.record().finish).toMatchObject({
    finish: "stop",
    rawFinish: "end_turn",
    native: { responseId: "step" },
    usage: { nonCachedInputTokens: 10, outputTokens: 4 },
  })
})

test("content-filter on the first finish settlement fails the assistant", async () => {
  const { published, publisher } = capture()
  await Effect.runPromise(publisher.publish(LLMEvent.stepStart({ index: 0 })))
  await Effect.runPromise(
    publisher.publish(LLMEvent.finish({ reason: { normalized: "content-filter", raw: "refusal" } })),
  )

  expect(publisher.record().finish).toMatchObject({ finish: "content-filter", rawFinish: "refusal" })
  expect(publisher.record().providerFailed).toBe(true)
  expect(publisher.record().failure).toMatchObject({ type: "provider.content-filter" })
  expect(published.some((event) => event.type === "session.step.failed.1")).toBe(false)
})
