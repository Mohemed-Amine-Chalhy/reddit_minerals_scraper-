import { z, type ZodType } from 'zod';
import {
  analysisStatusSchema,
  researchRecordSchema,
  type ResearchRecord,
  type ResearchSnapshot,
} from '../../domain/schemas';

export const liveCredentialModeSchema = z.enum(['server', 'provided']);
export const liveTimeFilterSchema = z.enum(['hour', 'day', 'week', 'month', 'year', 'all']);
export const liveJobStatusSchema = z.enum([
  'queued',
  'running',
  'cancel_requested',
  'cancelled',
  'succeeded',
  'partial',
  'failed',
]);
export const liveSnapshotStatusSchema = z.enum(['succeeded', 'partial']);
export const liveJobStageSchema = z.enum([
  'queued',
  'searching',
  'collecting',
  'finalizing',
  'complete',
]);

const positiveInteger = z.number().int().positive();
const nonnegativeInteger = z.number().int().nonnegative();
const liveJobAccessTokenSchema = z.string().regex(/^[A-Za-z0-9_-]{43}$/u);

const liveTargetSchema = z
  .object({
    mineral: z.string().trim().min(1).max(128),
    subreddits: z
      .array(
        z
          .string()
          .trim()
          .regex(/^[A-Za-z0-9_]{2,64}$/u),
      )
      .min(1)
      .max(20),
  })
  .strict();

const liveCredentialsSchema = z
  .object({
    client_id: z.string().trim().min(1).max(256),
    client_secret: z.string().min(1).max(512),
    user_agent: z.string().trim().min(10).max(256),
  })
  .strict();

export const liveCapabilitiesSchema = z
  .object({
    enabled: z.boolean(),
    provider: z.literal('reddit'),
    library: z.literal('PRAW'),
    server_credentials_configured: z.boolean(),
    byo_credentials_allowed: z.boolean(),
    credential_modes: z.array(liveCredentialModeSchema),
    creation_access_token_required: z.literal(true),
    creation_access_token_header: z.literal('X-Live-Access-Token'),
    access_token_header: z.literal('X-Live-Job-Token'),
    time_filters: z.array(liveTimeFilterSchema).min(1),
    defaults: z
      .object({
        time_filter: liveTimeFilterSchema,
        max_posts_per_mineral: positiveInteger,
        max_comments_per_post: nonnegativeInteger,
      })
      .strict(),
    limits: z
      .object({
        max_targets: positiveInteger,
        max_subreddits_per_target: positiveInteger,
        max_posts_per_mineral: positiveInteger,
        max_comments_per_post: nonnegativeInteger,
        max_records_per_job: positiveInteger,
        max_active_jobs: positiveInteger,
        retention_seconds: positiveInteger,
      })
      .strict(),
  })
  .strict()
  .superRefine((capabilities, context) => {
    const modes = new Set(capabilities.credential_modes);
    if (
      (capabilities.enabled && capabilities.server_credentials_configured) !== modes.has('server')
    ) {
      context.addIssue({
        code: 'custom',
        message: 'Server credential availability does not match its advertised mode.',
        path: ['credential_modes'],
      });
    }
    if ((capabilities.enabled && capabilities.byo_credentials_allowed) !== modes.has('provided')) {
      context.addIssue({
        code: 'custom',
        message: 'Provided credential availability does not match its advertised mode.',
        path: ['credential_modes'],
      });
    }
    if (!capabilities.time_filters.includes(capabilities.defaults.time_filter)) {
      context.addIssue({
        code: 'custom',
        message: 'The default time filter must be advertised.',
        path: ['defaults', 'time_filter'],
      });
    }
  });

export const createLiveJobRequestSchema = z
  .object({
    targets: z.array(liveTargetSchema).min(1).max(10),
    time_filter: liveTimeFilterSchema,
    max_posts_per_mineral: positiveInteger,
    max_comments_per_post: nonnegativeInteger,
    credential_mode: liveCredentialModeSchema,
    credentials: liveCredentialsSchema.optional(),
  })
  .strict()
  .superRefine((request, context) => {
    const shouldIncludeCredentials = request.credential_mode === 'provided';
    if (shouldIncludeCredentials !== (request.credentials !== undefined)) {
      context.addIssue({
        code: 'custom',
        message: 'Credentials must be supplied only for provided-credential jobs.',
        path: ['credentials'],
      });
    }
  });

const liveJobProgressSchema = z
  .object({
    minerals_total: positiveInteger,
    minerals_completed: nonnegativeInteger,
    subreddits_total: positiveInteger,
    subreddits_completed: nonnegativeInteger,
    posts_discovered: nonnegativeInteger,
    posts_stored: nonnegativeInteger,
    posts_failed: nonnegativeInteger,
    comments_stored: nonnegativeInteger,
    searches_failed: nonnegativeInteger,
  })
  .strict();

const liveJobErrorSchema = z
  .object({
    code: z.string().min(1).max(80),
    message: z.string().min(1).max(240),
  })
  .strict();

export const liveJobSchema = z
  .object({
    id: z.string().regex(/^[0-9a-f]{32}$/u),
    status: liveJobStatusSchema,
    stage: liveJobStageSchema,
    credential_mode: liveCredentialModeSchema,
    targets: z.array(liveTargetSchema).min(1),
    time_filter: liveTimeFilterSchema,
    max_posts_per_mineral: positiveInteger,
    max_comments_per_post: nonnegativeInteger,
    created_at: z.iso.datetime({ offset: true }),
    started_at: z.iso.datetime({ offset: true }).nullable(),
    finished_at: z.iso.datetime({ offset: true }).nullable(),
    expires_at: z.iso.datetime({ offset: true }).nullable(),
    progress: liveJobProgressSchema,
    record_count: nonnegativeInteger,
    message: z.string().min(1).max(240),
    error: liveJobErrorSchema.nullable(),
  })
  .strict();

export const createLiveJobResponseSchema = z
  .object({
    job: liveJobSchema,
    access_token: liveJobAccessTokenSchema,
  })
  .strict();

const liveRecordSchema = z
  .object({
    id: z.string().min(1).max(32),
    kind: z.enum(['post', 'comment']),
    post_id: z.string().max(32).nullable(),
    parent_id: z.string().max(64).nullable(),
    depth: nonnegativeInteger.nullable(),
    mineral: z.string().min(1).max(128),
    title: z.string().max(1_000).nullable(),
    body: z.string().max(1_000_000),
    subreddit: z.string().min(1).max(64),
    created_at: z.iso.datetime({ offset: true }),
    fetched_at: z.iso.datetime({ offset: true }),
    score: z.number().int(),
    comment_count: nonnegativeInteger.nullable(),
    upvote_ratio: z.number().min(0).max(1).nullable(),
    permalink: z.string().max(2_048),
    scrape_status: analysisStatusSchema,
  })
  .strict()
  .superRefine((record, context) => {
    if (record.kind === 'comment' && !record.post_id) {
      context.addIssue({
        code: 'custom',
        message: 'A live comment must identify its Reddit post.',
        path: ['post_id'],
      });
    }
    if (
      record.kind === 'post' &&
      (record.post_id !== null ||
        record.parent_id !== null ||
        record.depth !== null ||
        record.title === null ||
        record.comment_count === null)
    ) {
      context.addIssue({
        code: 'custom',
        message: 'A live post has invalid kind-specific fields.',
        path: ['parent_id'],
      });
    }
    if (
      record.kind === 'comment' &&
      (record.depth === null ||
        record.title !== null ||
        record.comment_count !== null ||
        record.upvote_ratio !== null)
    ) {
      context.addIssue({
        code: 'custom',
        message: 'A live comment has invalid kind-specific fields.',
        path: ['depth'],
      });
    }
  });

export const liveSnapshotSchema = z
  .object({
    job_id: z.string().regex(/^[0-9a-f]{32}$/u),
    status: liveSnapshotStatusSchema,
    generated_at: z.iso.datetime({ offset: true }),
    records: z.array(liveRecordSchema).max(10_000),
  })
  .strict();

export type LiveCapabilities = z.infer<typeof liveCapabilitiesSchema>;
export type LiveCredentialMode = z.infer<typeof liveCredentialModeSchema>;
export type LiveJob = z.infer<typeof liveJobSchema>;
export type LiveJobRequest = z.infer<typeof createLiveJobRequestSchema>;
export type LiveSnapshot = z.infer<typeof liveSnapshotSchema>;
export type LiveTimeFilter = z.infer<typeof liveTimeFilterSchema>;

export interface LiveRedditClient {
  capabilities(): Promise<LiveCapabilities | null>;
  createJob(
    request: LiveJobRequest,
    creationAccessToken: string,
    jobAccessToken: string,
  ): Promise<z.infer<typeof createLiveJobResponseSchema>>;
  getJob(jobId: string, accessToken: string): Promise<LiveJob>;
  cancelJob(jobId: string, accessToken: string): Promise<LiveJob>;
  getSnapshot(jobId: string, accessToken: string): Promise<LiveSnapshot>;
}

export class LiveApiError extends Error {
  public constructor(
    message: string,
    public readonly status: number | null = null,
  ) {
    super(message);
    this.name = 'LiveApiError';
  }
}

function safeApiMessage(status: number): string {
  if (status === 401 || status === 403) return 'The deployment key or job token was rejected.';
  if (status === 409) return 'This collection job cannot perform that action now.';
  if (status === 422) return 'Check the collection configuration and try again.';
  if (status === 429) return 'Live collection is busy. Wait briefly, then try again.';
  if (status === 503) return 'Live Reddit collection is temporarily unavailable.';
  return 'The live collection service could not complete the request.';
}

async function fetchAndParse<Output>(
  url: string,
  schema: ZodType<Output>,
  init: RequestInit = {},
  timeoutMs = 10_000,
): Promise<Output> {
  let response: Response;
  try {
    response = await fetch(url, { ...init, signal: AbortSignal.timeout(timeoutMs) });
  } catch {
    throw new LiveApiError('The live collection service is unavailable.');
  }
  if (!response.ok) throw new LiveApiError(safeApiMessage(response.status), response.status);
  try {
    return schema.parse(await response.json());
  } catch {
    throw new LiveApiError('The live collection service returned an invalid response.');
  }
}

export class HttpLiveRedditClient implements LiveRedditClient {
  public constructor(private readonly basePath = '/api/v1/live') {}

  public async capabilities(): Promise<LiveCapabilities | null> {
    try {
      return await fetchAndParse(`${this.basePath}/capabilities`, liveCapabilitiesSchema, {
        headers: { Accept: 'application/json' },
      });
    } catch {
      return null;
    }
  }

  public async createJob(
    request: LiveJobRequest,
    creationAccessToken: string,
    jobAccessToken: string,
  ): Promise<z.infer<typeof createLiveJobResponseSchema>> {
    const validated = createLiveJobRequestSchema.parse(request);
    const validatedCreationAccessToken = z.string().min(32).max(512).parse(creationAccessToken);
    const validatedJobAccessToken = liveJobAccessTokenSchema.parse(jobAccessToken);
    return fetchAndParse(`${this.basePath}/jobs`, createLiveJobResponseSchema, {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'X-Live-Access-Token': validatedCreationAccessToken,
        'X-Live-Job-Token': validatedJobAccessToken,
      },
      body: JSON.stringify(validated),
    });
  }

  public async getJob(jobId: string, accessToken: string): Promise<LiveJob> {
    return fetchAndParse(`${this.basePath}/jobs/${encodeURIComponent(jobId)}`, liveJobSchema, {
      headers: this.jobHeaders(accessToken),
    });
  }

  public async cancelJob(jobId: string, accessToken: string): Promise<LiveJob> {
    return fetchAndParse(`${this.basePath}/jobs/${encodeURIComponent(jobId)}`, liveJobSchema, {
      method: 'DELETE',
      headers: this.jobHeaders(accessToken),
    });
  }

  public async getSnapshot(jobId: string, accessToken: string): Promise<LiveSnapshot> {
    return fetchAndParse(
      `${this.basePath}/jobs/${encodeURIComponent(jobId)}/snapshot`,
      liveSnapshotSchema,
      { headers: this.jobHeaders(accessToken) },
      60_000,
    );
  }

  private jobHeaders(accessToken: string): HeadersInit {
    return {
      Accept: 'application/json',
      'X-Live-Job-Token': liveJobAccessTokenSchema.parse(accessToken),
    };
  }
}

export function generateLiveJobAccessToken(): string {
  const bytes = globalThis.crypto.getRandomValues(new Uint8Array(32));
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return liveJobAccessTokenSchema.parse(
    globalThis.btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/u, ''),
  );
}

export async function deriveLiveJobId(jobAccessToken: string): Promise<string> {
  const validated = liveJobAccessTokenSchema.parse(jobAccessToken);
  const input = new TextEncoder().encode(`minerallens-live-job-v1\0${validated}`);
  const digest = new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', input));
  return [...digest.slice(0, 16)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

const liveSourceNote =
  'Collected directly from Reddit through PRAW. No relevance, sentiment, stance, topic, or reputation analysis has been generated.';

function liveRecordToResearchRecord(record: LiveSnapshot['records'][number]): ResearchRecord {
  if (record.kind === 'comment' && !record.post_id) {
    throw new LiveApiError('The live collection service returned an invalid comment reference.');
  }
  if (record.kind === 'comment' && record.depth === null) {
    throw new LiveApiError('The live collection service returned an invalid comment depth.');
  }
  if (record.kind === 'post' && (record.title === null || record.comment_count === null)) {
    throw new LiveApiError('The live collection service returned invalid post fields.');
  }
  const common = {
    id: record.id,
    subreddit: record.subreddit,
    score: record.score,
    created_at: record.created_at,
    fetched_at: record.fetched_at,
    permalink: record.permalink,
  };
  const raw =
    record.kind === 'post'
      ? {
          export_schema_version: 1,
          record_type: 'post',
          mineral: record.mineral,
          content_available: true,
          source_note: liveSourceNote,
          content: {
            ...common,
            title: record.title,
            selftext: record.body,
            num_comments: record.comment_count,
            upvote_ratio: record.upvote_ratio,
            scrape_status: record.scrape_status,
          },
          analyses: {},
        }
      : {
          export_schema_version: 1,
          record_type: 'comment',
          mineral: record.mineral,
          content_available: true,
          source_note: liveSourceNote,
          content: {
            ...common,
            post_id: record.post_id,
            parent_id: record.parent_id,
            body: record.body,
            depth: record.depth,
          },
          analyses: {},
        };
  return researchRecordSchema.parse(raw);
}

export function liveSnapshotToResearchSnapshot(snapshot: LiveSnapshot): ResearchSnapshot {
  const records = snapshot.records.map(liveRecordToResearchRecord);
  const minerals = [...new Set(records.map((record) => record.mineral))].sort();
  return {
    records,
    runs: [],
    delivery: 'live',
    provenance: {
      kind: 'live-reddit',
      datasetLabel: `Live Reddit collection · ${minerals.join(', ') || 'no records'}`,
      datasetDescription: 'A bounded, user-initiated Reddit collection held in browser memory.',
      synthetic: false,
      publicSample: false,
      sourceUrl: null,
      datasetVersion: null,
      sourceNote: liveSourceNote,
    },
    notice: `${records.length.toLocaleString('en')} raw Reddit records collected for this job. No analysis was generated.`,
  };
}

export const defaultLiveRedditClient = new HttpLiveRedditClient();
