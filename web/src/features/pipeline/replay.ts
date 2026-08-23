export type ReplayScenario = 'nominal' | 'retry' | 'stale';
export type ReplayStage = 'collect' | 'relevance' | 'enrichment' | 'reputation' | 'export';

export interface ReplayEvent {
  readonly stage: ReplayStage;
  readonly state: 'running' | 'complete' | 'retrying' | 'requeued';
  readonly message: string;
}

const standardEvents: readonly ReplayEvent[] = [
  { stage: 'collect', state: 'running', message: 'Reading bounded synthetic provider records.' },
  { stage: 'collect', state: 'complete', message: 'Content and work state committed atomically.' },
  {
    stage: 'relevance',
    state: 'running',
    message: 'Validating relevance responses against schema v1.',
  },
  {
    stage: 'relevance',
    state: 'complete',
    message: 'Relevant posts are eligible for downstream work.',
  },
  {
    stage: 'enrichment',
    state: 'running',
    message: 'Extracting topics, sentiment, and concern signals.',
  },
  {
    stage: 'enrichment',
    state: 'complete',
    message: 'Enrichment results committed with provenance.',
  },
  {
    stage: 'reputation',
    state: 'running',
    message: 'Estimating bounded content-level perception signals.',
  },
  {
    stage: 'reputation',
    state: 'complete',
    message: 'Dependency revision still matches relevance input.',
  },
  {
    stage: 'export',
    state: 'running',
    message: 'Holding one SQLite snapshot through publication.',
  },
  { stage: 'export', state: 'complete', message: 'Versioned JSONL snapshot published atomically.' },
];

export function buildReplayEvents(scenario: ReplayScenario): readonly ReplayEvent[] {
  if (scenario === 'nominal') return standardEvents;
  const events = [...standardEvents];
  if (scenario === 'retry') {
    events.splice(5, 0, {
      stage: 'enrichment',
      state: 'retrying',
      message: 'Synthetic rate limit classified as transient; bounded backoff scheduled.',
    });
  } else {
    events.splice(7, 0, {
      stage: 'reputation',
      state: 'requeued',
      message: 'Late result rejected because its source revision changed; current work requeued.',
    });
  }
  return events;
}
