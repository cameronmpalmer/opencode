import { Button } from "@opencode-ai/ui/button"
import { Dialog } from "@opencode-ai/ui/dialog"
import type { JSX } from "solid-js"
import { archiveConfirmation, type ArchivePreviewCounts } from "@/utils/session-archive"

type DialogController = {
  show(element: () => JSX.Element, onClose?: () => void): unknown
  close(): void
}

type Translator = {
  t(key: string, vars?: Record<string, string | number>): string
}

export function confirmArchiveSession(
  dialog: DialogController,
  language: Translator,
  preview: ArchivePreviewCounts,
): Promise<boolean> {
  return new Promise((resolve) => {
    let settled = false
    const finish = (value: boolean) => {
      if (settled) return
      settled = true
      resolve(value)
      dialog.close()
    }

    dialog.show(
      () => (
        <Dialog title={language.t("session.archive.title")} fit>
          <div class="flex flex-col gap-4 pl-6 pr-2.5 pb-3">
            <span class="text-14-regular text-text-strong">{archiveConfirmation(preview, language.t)}</span>
            <div class="flex justify-end gap-2">
              <Button variant="ghost" size="large" onClick={() => finish(false)}>
                {language.t("common.cancel")}
              </Button>
              <Button variant="primary" size="large" onClick={() => finish(true)}>
                {language.t("common.archive")}
              </Button>
            </div>
          </div>
        </Dialog>
      ),
      () => finish(false),
    )
  })
}
