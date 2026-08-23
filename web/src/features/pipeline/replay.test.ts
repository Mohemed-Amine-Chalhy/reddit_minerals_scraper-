import { describe, expect, it } from 'vitest';
import { buildReplayEvents } from './replay';

describe('pipeline replay', () => {
  it('keeps all scenarios deterministic and ends with atomic export', () => {
    expect(buildReplayEvents('nominal')).toHaveLength(10);
    expect(buildReplayEvents('retry').some((event) => event.state === 'retrying')).toBe(true);
    expect(buildReplayEvents('stale').some((event) => event.state === 'requeued')).toBe(true);
    expect(buildReplayEvents('nominal').at(-1)).toMatchObject({
      stage: 'export',
      state: 'complete',
    });
  });
});
