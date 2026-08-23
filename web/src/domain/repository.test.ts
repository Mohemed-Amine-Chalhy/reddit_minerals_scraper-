import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiResearchRepository, PublicSampleResearchRepository } from './repository';

const createdAt = '2026-08-23T08:00:00Z';
const source = {
  kind: 'public-research-sample',
  public_sample: true,
  dataset_label: 'Reddit Mining Stance · curated sample',
  dataset_description: 'A deterministic metadata-only sample from the public Kaggle dataset.',
  owner_name: 'Mohamed Amine Chalhy',
  dataset_ref: 'mohamedaminechalhy/reddit-mining-stance',
  source_url: 'https://www.kaggle.com/datasets/mohamedaminechalhy/reddit-mining-stance',
  dataset_version: '2',
  archive_sha256: '3A299CEC89CB091E9AD9E8F4158FD264A761C92BD9CA5B37B94924D99C3D7407',
  license: 'MIT',
  published_at: '2025-09-28T16:47:03.01Z',
  source_note: 'Raw Reddit text and authors are not included.',
  full_counts: { minerals: 26, records: 1_042_563, posts: 15_779, comments: 1_026_784 },
  sample_counts: { minerals: 3, records: 3, posts: 3, comments: 0 },
  published_date_range: { start: '2008-02-19', end: '2025-08-27' },
  sample_method: 'Repository test fixture containing three deterministic post records.',
  raw_text_included: false,
  authors_included: false,
};

function publicRecord(mineral: string, recordSource: typeof source = source) {
  return {
    id: `sample-${mineral}-post`,
    kind: 'post',
    parent_id: null,
    mineral,
    topic_label: 'circular supply',
    title: `${mineral} post metadata`,
    body: `${mineral} post metadata. Raw source text is not included.`,
    subreddit: 'mining',
    created_at: createdAt,
    score: 42,
    comment_count: 1,
    analysis: {
      relevance: { relevant: true, confidence: 96, rationale: 'Direct discussion.' },
      enrichment: {
        sentiment: 'positive',
        stance: 'neutral',
        keywords: [mineral],
        themes: ['circular supply'],
        concerns: [{ name: 'sustainability', score: 0.8 }],
      },
      reputation: null,
    },
    source_note: recordSource.source_note,
    mode: 'public-research-sample',
    synthetic: false,
    public_sample: true,
    content_available: false,
    source: recordSource,
  };
}

function jsonResponse(payload: unknown): Response {
  return { ok: true, status: 200, json: async () => Promise.resolve(payload) } as Response;
}

interface ApiFetchOptions {
  readonly snapshotSource?: typeof source;
  readonly recordSource?: typeof source;
  readonly runSource?: typeof source;
  readonly includeSyntheticRun?: boolean;
}

function apiFetch(options: ApiFetchOptions = {}) {
  const snapshotSource = options.snapshotSource ?? source;
  const recordSource = options.recordSource ?? snapshotSource;
  const runSource = options.runSource ?? source;
  const runItems = options.includeSyntheticRun
    ? [
        {
          id: 'run-drift',
          command: 'scrape',
          status: 'succeeded',
          started_at: createdAt,
          finished_at: createdAt,
          processed: 3,
          failed: 0,
          duration_ms: 1,
          synthetic: true,
        },
      ]
    : [];
  return vi.fn((input: RequestInfo | URL) => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
    if (url.endsWith('/meta')) {
      return Promise.resolve(
        jsonResponse({
          api_version: 'v1',
          application_name: 'MineralLens API',
          application_version: '0.1.0',
          dataset_label: source.dataset_label,
          dataset_description: source.dataset_description,
          mode: 'public-research-sample',
          synthetic: false,
          public_sample: true,
          read_only: true,
          generated_at: createdAt,
          minerals: ['copper', 'gold', 'silver'],
          totals: { minerals: 3, records: 3, posts: 3, comments: 0, analyses: 6, runs: 0 },
          source,
        }),
      );
    }
    if (url.includes('/runs')) {
      return Promise.resolve(
        jsonResponse({
          page: 1,
          page_size: 50,
          total: runItems.length,
          pages: runItems.length === 0 ? 0 : 1,
          items: runItems,
          mode: 'public-research-sample',
          synthetic: false,
          public_sample: true,
          source: runSource,
        }),
      );
    }
    if (url.endsWith('/snapshot')) {
      return Promise.resolve(
        jsonResponse({
          mode: 'public-research-sample',
          synthetic: false,
          public_sample: true,
          source: snapshotSource,
          generated_at: createdAt,
          records: ['gold', 'silver', 'copper'].map((mineral) =>
            publicRecord(mineral, recordSource),
          ),
        }),
      );
    }
    return Promise.reject(new Error(`Unexpected API request: ${url}`));
  });
}

afterEach(() => vi.unstubAllGlobals());

describe('ApiResearchRepository', () => {
  it('adapts strict API DTOs into the frontend snapshot', async () => {
    const fetchMock = apiFetch();
    vi.stubGlobal('fetch', fetchMock);

    const snapshot = await new ApiResearchRepository('/api/v1').load();

    expect(snapshot.delivery).toBe('api');
    expect(snapshot.provenance.publicSample).toBe(true);
    expect(snapshot.provenance.sourceUrl).toBe(source.source_url);
    expect(snapshot.records).toHaveLength(3);
    expect(snapshot.records.map((record) => record.mineral)).toEqual(['gold', 'silver', 'copper']);
    expect(snapshot.records[0]?.content_available).toBe(false);
    expect(snapshot.records[0]?.analyses.reputation).toBeUndefined();
    expect(snapshot.runs).toEqual([]);
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(
      fetchMock.mock.calls.map(([input]) =>
        typeof input === 'string' ? input : input instanceof URL ? input.href : input.url,
      ),
    ).toEqual(['/api/v1/meta', '/api/v1/snapshot', '/api/v1/runs?page_size=50']);
  });

  it('rejects provenance drift at transfer, record, and run levels', async () => {
    const scenarios = [
      apiFetch({ snapshotSource: { ...source, source_url: 'https://example.com/drift' } }),
      apiFetch({ recordSource: { ...source, dataset_version: 'unexpected-version' } }),
      apiFetch({ includeSyntheticRun: true }),
    ];

    for (const fetchMock of scenarios) {
      vi.stubGlobal('fetch', fetchMock);
      const snapshot = await new ApiResearchRepository(
        '/api/v1',
        new PublicSampleResearchRepository(),
      ).load();

      expect(snapshot.delivery).toBe('bundled');
      expect(snapshot.provenance.sourceUrl).toBe(source.source_url);
      expect(snapshot.notice).toContain('bundled deterministic public research sample');
      expect(fetchMock).toHaveBeenCalledTimes(3);
    }
  });

  it('falls back transparently when the optional API is unavailable', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => Promise.reject(new TypeError('offline'))),
    );
    const snapshot = await new ApiResearchRepository(
      '/api/v1',
      new PublicSampleResearchRepository(),
    ).load();
    expect(snapshot.delivery).toBe('bundled');
    expect(snapshot.provenance.kind).toBe('public-research-sample');
    expect(snapshot.provenance.synthetic).toBe(false);
    expect(snapshot.records).toHaveLength(104);
    expect(snapshot.notice).toContain('bundled deterministic public research sample');
  });
});
