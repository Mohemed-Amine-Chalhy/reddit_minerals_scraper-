import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  HttpLiveRedditClient,
  createLiveJobRequestSchema,
  deriveLiveJobId,
  generateLiveJobAccessToken,
  liveCapabilitiesSchema,
  liveSnapshotSchema,
  liveSnapshotToResearchSnapshot,
  type LiveJob,
  type LiveJobRequest,
  type LiveSnapshot,
} from './api';

const now = '2026-08-23T10:00:00Z';
const jobId = 'a'.repeat(32);
const accessToken = 't'.repeat(43);
const creationAccessToken = 'd'.repeat(48);

export const liveCapabilitiesFixture = liveCapabilitiesSchema.parse({
  enabled: true,
  provider: 'reddit',
  library: 'PRAW',
  server_credentials_configured: true,
  byo_credentials_allowed: true,
  credential_modes: ['server', 'provided'],
  creation_access_token_required: true,
  creation_access_token_header: 'X-Live-Access-Token',
  access_token_header: 'X-Live-Job-Token',
  time_filters: ['hour', 'day', 'week', 'month', 'year', 'all'],
  defaults: {
    time_filter: 'week',
    max_posts_per_mineral: 2,
    max_comments_per_post: 3,
  },
  limits: {
    max_targets: 10,
    max_subreddits_per_target: 20,
    max_posts_per_mineral: 100,
    max_comments_per_post: 500,
    max_records_per_job: 10_000,
    max_active_jobs: 2,
    retention_seconds: 900,
  },
});

export const queuedJobFixture: LiveJob = {
  id: jobId,
  status: 'queued',
  stage: 'queued',
  credential_mode: 'server',
  targets: [{ mineral: 'copper', subreddits: ['mining', 'geology'] }],
  time_filter: 'week',
  max_posts_per_mineral: 2,
  max_comments_per_post: 3,
  created_at: now,
  started_at: null,
  finished_at: null,
  expires_at: null,
  progress: {
    minerals_total: 1,
    minerals_completed: 0,
    subreddits_total: 2,
    subreddits_completed: 0,
    posts_discovered: 0,
    posts_stored: 0,
    posts_failed: 0,
    comments_stored: 0,
    searches_failed: 0,
  },
  record_count: 0,
  message: 'Collection queued.',
  error: null,
};

export const succeededJobFixture: LiveJob = {
  ...queuedJobFixture,
  status: 'succeeded',
  stage: 'complete',
  started_at: now,
  finished_at: now,
  expires_at: '2026-08-23T10:15:00Z',
  progress: {
    ...queuedJobFixture.progress,
    minerals_completed: 1,
    subreddits_completed: 2,
    posts_discovered: 1,
    posts_stored: 1,
    comments_stored: 1,
  },
  record_count: 2,
  message: 'Collection complete.',
};

export const liveSnapshotFixture: LiveSnapshot = liveSnapshotSchema.parse({
  job_id: jobId,
  status: 'succeeded',
  generated_at: now,
  records: [
    {
      id: 'post1',
      kind: 'post',
      post_id: null,
      parent_id: null,
      depth: null,
      mineral: 'copper',
      title: 'Copper supply discussion',
      body: 'A raw Reddit post body.',
      subreddit: 'mining',
      created_at: now,
      fetched_at: now,
      score: 42,
      comment_count: 1,
      upvote_ratio: 0.92,
      permalink: '/r/mining/comments/post1/copper_supply_discussion/',
      scrape_status: 'complete',
    },
    {
      id: 'comment1',
      kind: 'comment',
      post_id: 'post1',
      parent_id: 't3_post1',
      depth: 0,
      mineral: 'copper',
      title: null,
      body: 'A raw Reddit comment body.',
      subreddit: 'mining',
      created_at: now,
      fetched_at: now,
      score: 5,
      comment_count: null,
      upvote_ratio: null,
      permalink: '/r/mining/comments/post1/copper_supply_discussion/comment1/',
      scrape_status: 'complete',
    },
  ],
});

function jsonResponse(payload: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => Promise.resolve(payload),
  } as Response;
}

afterEach(() => vi.unstubAllGlobals());

describe('live Reddit API contracts', () => {
  it('generates capability tokens and derives the backend-compatible job id', async () => {
    expect(generateLiveJobAccessToken()).toMatch(/^[A-Za-z0-9_-]{43}$/u);
    await expect(deriveLiveJobId(accessToken)).resolves.toBe(
      '9f2fd19e539e91033bddb283b2b333e6', // pragma: allowlist secret
    );
  });

  it('validates bounded credential requests and rejects credentials in server mode', () => {
    const provided = createLiveJobRequestSchema.parse({
      targets: [{ mineral: 'copper', subreddits: ['mining'] }],
      time_filter: 'week',
      max_posts_per_mineral: 2,
      max_comments_per_post: 3,
      credential_mode: 'provided',
      credentials: {
        client_id: 'client-id',
        client_secret: 'client-secret', // pragma: allowlist secret
        user_agent: 'script:minerallens:test (by u/researcher)',
      },
    });
    expect(provided.credentials?.client_secret).toBe('client-secret');
    expect(
      createLiveJobRequestSchema.safeParse({ ...provided, credential_mode: 'server' }).success,
    ).toBe(false);
  });

  it('rejects unknown response fields and comment records without a post link', () => {
    expect(
      liveCapabilitiesSchema.safeParse({ ...liveCapabilitiesFixture, unexpected: true }).success,
    ).toBe(false);
    const comment = liveSnapshotFixture.records[1];
    if (!comment) throw new Error('Expected a comment fixture.');
    expect(
      liveSnapshotSchema.safeParse({
        ...liveSnapshotFixture,
        records: [{ ...comment, post_id: null }],
      }).success,
    ).toBe(false);
    expect(
      liveSnapshotSchema.safeParse({ ...liveSnapshotFixture, status: 'running' }).success,
    ).toBe(false);
    expect(
      liveCapabilitiesSchema.safeParse({
        ...liveCapabilitiesFixture,
        enabled: false,
        server_credentials_configured: true,
        byo_credentials_allowed: false,
        credential_modes: [],
      }).success,
    ).toBe(true);
  });

  it('adapts raw posts and comments without fabricating any analysis', () => {
    const snapshot = liveSnapshotToResearchSnapshot(liveSnapshotFixture);

    expect(snapshot.delivery).toBe('live');
    expect(snapshot.provenance.kind).toBe('live-reddit');
    expect(snapshot.records).toHaveLength(2);
    expect(snapshot.records.every((record) => Object.keys(record.analyses).length === 0)).toBe(
      true,
    );
    const comment = snapshot.records.find((record) => record.record_type === 'comment');
    expect(comment?.content).toMatchObject({ post_id: 'post1', parent_id: 't3_post1', depth: 0 });
    expect(snapshot.notice).toContain('No analysis was generated');
  });

  it('sends the job token only in the dedicated header', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({ job: queuedJobFixture, access_token: accessToken }, 202),
      )
      .mockResolvedValueOnce(jsonResponse(queuedJobFixture));
    vi.stubGlobal('fetch', fetchMock);
    const client = new HttpLiveRedditClient('/api/v1/live');
    const request: LiveJobRequest = {
      targets: [{ mineral: 'copper', subreddits: ['mining'] }],
      time_filter: 'week',
      max_posts_per_mineral: 2,
      max_comments_per_post: 3,
      credential_mode: 'server',
    };

    const created = await client.createJob(request, creationAccessToken, accessToken);
    await client.getJob(created.job.id, created.access_token);

    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/v1/live/jobs');
    const createInit = fetchMock.mock.calls[0]?.[1] as RequestInit;
    expect(JSON.parse(createInit.body as string)).toEqual(request);
    const createHeaders = new Headers(createInit.headers);
    expect(createHeaders.get('X-Live-Access-Token')).toBe(creationAccessToken);
    expect(createHeaders.get('X-Live-Job-Token')).toBe(accessToken);
    expect(String(fetchMock.mock.calls[0]?.[0])).not.toContain(creationAccessToken);
    const pollUrl = String(fetchMock.mock.calls[1]?.[0]);
    const pollHeaders = new Headers((fetchMock.mock.calls[1]?.[1] as RequestInit).headers);
    expect(pollUrl).not.toContain(accessToken);
    expect(pollHeaders.get('X-Live-Job-Token')).toBe(accessToken);
  });

  it('treats an unavailable or malformed capability endpoint as disabled', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({ enabled: true })));
    await expect(new HttpLiveRedditClient().capabilities()).resolves.toBeNull();
  });
});
