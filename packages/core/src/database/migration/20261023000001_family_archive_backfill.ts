import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

// Hand-written migration. Task A5 adds the archive trigger and data backfill.
export default {
  id: "20261023000001_family_archive_backfill",
  runsOnFreshSchema: true,
  up() {
    return Effect.void
  },
} satisfies DatabaseMigration.Migration
