import { describe, expect, it } from 'vitest';
import { DEMO_RECORDS } from '../data/fixtures';
import {
  analysisStatuses,
  availableMinerals,
  availableTopics,
  EMPTY_FILTERS,
  filterRecords,
  recordText,
  summarize,
} from './selectors';

describe('research selectors', () => {
  it('filters across URL-compatible fields and summarizes completed analysis', () => {
    const gold = filterRecords(DEMO_RECORDS, { ...EMPTY_FILTERS, mineral: 'gold' });
    expect(gold).toHaveLength(4);
    expect(filterRecords(gold, { ...EMPTY_FILTERS, recordType: 'post' })).toHaveLength(2);
    expect(filterRecords(gold, { ...EMPTY_FILTERS, query: 'transparent' }).length).toBeGreaterThan(
      0,
    );
    expect(filterRecords(gold, { ...EMPTY_FILTERS, topic: 'water' }).length).toBeGreaterThan(0);
    expect(summarize(gold)).toMatchObject({ records: 4, posts: 2, comments: 2 });
  });

  it('derives stable option sets and safe record text', () => {
    const firstRecord = DEMO_RECORDS[0];
    if (!firstRecord) throw new Error('Expected the deterministic fixture to contain records.');
    expect(availableMinerals(DEMO_RECORDS)).toEqual([
      'cobalt',
      'copper',
      'gold',
      'graphite',
      'lithium',
      'nickel',
    ]);
    expect(availableTopics(DEMO_RECORDS)).toContain('circular supply');
    expect(recordText(firstRecord)).toContain('Gold projects');
    expect(analysisStatuses(firstRecord)).toEqual(['complete', 'complete', 'complete']);
  });
});
