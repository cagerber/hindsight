/**
 * Local journal of codebase-survey baselines — the HEAD sha each survey started at, per bank.
 *
 * The bank's `survey-baseline:<sha>` marker doc is the shared record, but it is written through
 * the retain QUEUE: it becomes a document only once a worker extracts it. While the queue is
 * backlogged (or the worker is paused) the marker does not exist yet, so the next SessionStart
 * still measures commits from the PREVIOUS survey, finds the threshold crossed again, and launches
 * another survey — and queues another marker behind the first. One repository saw 28 surveys in a
 * day that way. This journal is written synchronously when a survey starts, so the very next
 * session sees it whatever the queue is doing. The bank's markers are still read (they carry the
 * state across machines); the two sets are unioned.
 */
import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { cursorRoot } from "./session-cache";

/** Entries kept per bank. The re-survey check takes the minimum distance to any reachable baseline,
 *  so only the most recent few ever matter; older ones are dropped. */
export const MAX_LOCAL_BASELINES = 20;

/** One survey start: the HEAD it surveyed and when (epoch ms). */
export interface LocalBaseline {
  sha: string;
  at: number;
}

export interface SurveyBaselineJournal {
  read(bankId: string): LocalBaseline[];
  record(bankId: string, sha: string, at?: number): void;
}

/** The journal sits beside the retain cursors (`HINDSIGHT_CURSOR_DIR` moves both). */
export function surveyBaselinePath(): string {
  return join(cursorRoot(), "survey-baselines.json");
}

const isEntry = (e: unknown): e is LocalBaseline =>
  !!e &&
  typeof (e as LocalBaseline).sha === "string" &&
  typeof (e as LocalBaseline).at === "number";

function load(path: string): Record<string, LocalBaseline[]> {
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8")) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    const out: Record<string, LocalBaseline[]> = {};
    for (const [bank, entries] of Object.entries(parsed as Record<string, unknown>)) {
      if (Array.isArray(entries)) out[bank] = entries.filter(isEntry);
    }
    return out;
  } catch {
    return {};
  }
}

export function fileSurveyBaselineJournal(
  path: string = surveyBaselinePath()
): SurveyBaselineJournal {
  return {
    read(bankId) {
      return load(path)[bankId] ?? [];
    },
    record(bankId, sha, at = Date.now()) {
      try {
        const all = load(path);
        const prior = (all[bankId] ?? []).filter((e) => e.sha !== sha);
        all[bankId] = [...prior, { sha, at }].slice(-MAX_LOCAL_BASELINES);
        mkdirSync(dirname(path), { recursive: true });
        const tmp = `${path}.${process.pid}.tmp`;
        writeFileSync(tmp, JSON.stringify(all), { mode: 0o600 });
        renameSync(tmp, path); // atomic: a concurrent reader sees the old or the new file, never half
      } catch {
        /* best-effort — the bank marker is still queued; this only closes the queue-lag window */
      }
    },
  };
}
