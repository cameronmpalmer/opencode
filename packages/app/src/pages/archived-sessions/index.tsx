import { useNavigate } from "@solidjs/router"
import { Icon } from "@opencode-ai/ui/icon"
import { IconButtonV2 } from "@opencode-ai/ui/v2/icon-button-v2"
import { createMemo, createResource, createSignal, For, Show } from "solid-js"
import { useServerSDK } from "@/context/server-sdk"
import { useServerSync } from "@/context/server-sync"
import { archiveErrorMessage } from "@/utils/session-archive"
import { formatServerError } from "@/utils/server-errors"
import { showToast } from "@/utils/toast"
import {
  groupArchivedSessions,
  loadArchivedSessions,
  unarchiveArchivedEntry,
  type ArchivedFamilyEntry,
  type ArchivedSessionRow,
} from "./archived-sessions"

export function ArchivedSessionsPage() {
  const sdk = useServerSDK()
  const sync = useServerSync()
  const navigate = useNavigate()
  const [restored, setRestored] = createSignal<ReadonlySet<string>>(new Set())
  const [busy, setBusy] = createSignal<string>()

  const [loaded] = createResource(async () => {
    const rows = await loadArchivedSessions(async (input, options) => {
      const result = await sdk().client.experimental.session.list(input, options)
      return { data: (result.data ?? []) as ArchivedSessionRow[] }
    })
    return groupArchivedSessions(rows)
  })

  const entries = createMemo(() => (loaded() ?? []).filter((entry) => !restored().has(entry.key)))

  async function unarchive(entry: ArchivedFamilyEntry) {
    if (entry.archiving || busy()) return
    setBusy(entry.key)
    const outcome = await unarchiveArchivedEntry({
      api: sdk().api.session,
      entry,
      onError: (error) => {
        showToast({
          variant: "error",
          title: "Unarchive failed",
          description: archiveErrorMessage(error, formatServerError(error)),
        })
      },
    })
    setBusy(undefined)
    if (outcome.status !== "restored") return

    setRestored((current) => new Set([...current, entry.key]))
    for (const sessionID of outcome.sessionIDs) {
      void sync()
        .session.resolve(sessionID, { force: true })
        .catch(() => undefined)
    }
  }

  return (
    <div class="flex h-full min-h-0 w-full flex-col overflow-y-auto bg-background-base">
      <div class="mx-auto flex w-full max-w-3xl flex-col gap-4 px-6 py-8">
        <div class="flex items-center gap-3">
          <IconButtonV2
            icon={<Icon name="arrow-left" size="small" />}
            variant="ghost"
            size="small"
            aria-label="Back"
            onClick={() => navigate("/")}
          />
          <div>
            <h1 class="text-16-medium text-text-strong">Archived sessions</h1>
            <p class="text-12-regular text-text-weak">Restore a session family from the archive.</p>
          </div>
        </div>

        <Show when={loaded.error}>
          {(error) => (
            <p class="rounded border border-border-critical-base px-3 py-2 text-12-regular text-text-critical-base">
              {formatServerError(error())}
            </p>
          )}
        </Show>

        <Show
          when={!loaded.loading}
          fallback={<p class="text-12-regular text-text-weak">Loading archived sessions...</p>}
        >
          <Show
            when={entries().length > 0}
            fallback={<p class="text-12-regular text-text-weak">No archived sessions.</p>}
          >
            <ul class="flex flex-col gap-2">
              <For each={entries()}>
                {(entry) => (
                  <li class="flex items-center justify-between gap-3 rounded border border-border-base bg-surface-base px-3 py-2">
                    <div class="min-w-0">
                      <div class="flex items-center gap-2">
                        <span class="truncate text-14-medium text-text-strong">{entry.title ?? entry.key}</span>
                        <Show when={entry.archiving}>
                          <span class="rounded bg-surface-warning-base px-1.5 py-0.5 text-11-medium text-text-warning-strong">
                            Archiving
                          </span>
                        </Show>
                      </div>
                      <p class="truncate text-11-regular text-text-weak">
                        {entry.memberCount} {entry.memberCount === 1 ? "session" : "sessions"}
                        {entry.directory ? ` - ${entry.directory}` : ""}
                      </p>
                    </div>
                    <button
                      type="button"
                      class="shrink-0 rounded border border-border-base px-2 py-1 text-12-medium text-text-strong disabled:opacity-50"
                      disabled={entry.archiving || busy() === entry.key}
                      onClick={() => void unarchive(entry)}
                    >
                      {busy() === entry.key ? "Restoring..." : "Unarchive"}
                    </button>
                  </li>
                )}
              </For>
            </ul>
          </Show>
        </Show>
      </div>
    </div>
  )
}
