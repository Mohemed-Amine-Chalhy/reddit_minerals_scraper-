import { z } from 'zod';

export const analysisStatusSchema = z.enum([
  'pending',
  'partial',
  'complete',
  'retryable_failure',
  'permanent_failure',
  'blocked',
]);

export const relevanceResultSchema = z.object({
  relevant: z.boolean(),
  confidence: z.number().min(0).max(100),
  rationale: z.string().min(1),
  matched_topics: z.array(z.string()),
});

export const enrichmentResultSchema = z.object({
  sentiment: z.enum(['positive', 'negative', 'neutral', 'mixed']),
  keywords: z.array(z.string()),
  themes: z.array(z.string()),
  concerns: z.record(z.string(), z.number().min(0).max(1)),
  mining_stance: z.enum(['pro-mining', 'anti-mining', 'neutral', 'mixed']),
  topic_classification: z.string().min(1),
  relevance_score: z.number().min(0).max(1),
});

export const reputationResultSchema = z.object({
  overall_reputation_score: z.number().min(0).max(100),
  sentiment: z.enum(['positive', 'negative', 'neutral', 'mixed']),
  sentiment_score: z.number().min(0).max(100).nullable(),
  credibility: z.enum(['high', 'medium', 'low', 'unknown']),
  credibility_score: z.number().min(0).max(100).nullable(),
  market_impact: z.enum(['positive', 'negative', 'neutral', 'unclear']),
  market_impact_score: z.number().min(0).max(100).nullable(),
  controversy_level: z.enum(['high', 'medium', 'low']),
  rationale: z.string().min(1),
  evidence_signals: z.array(z.string()),
});

const analysisMetadataSchema = z.object({
  schema_version: z.number().int().nullable(),
  prompt_version: z.string().nullable(),
  model: z.string().nullable(),
  status: analysisStatusSchema,
  error: z.string().nullable().optional(),
  input_tokens: z.number().int().nonnegative().nullable(),
  output_tokens: z.number().int().nonnegative().nullable(),
  latency_ms: z.number().int().nonnegative().nullable(),
  updated_at: z.iso.datetime({ offset: true }).nullable(),
});

export const analysisBundleSchema = z
  .object({
    relevance: analysisMetadataSchema
      .extend({ result: relevanceResultSchema.nullable() })
      .optional(),
    enrichment: analysisMetadataSchema
      .extend({ result: enrichmentResultSchema.nullable() })
      .optional(),
    reputation: analysisMetadataSchema
      .extend({ result: reputationResultSchema.nullable() })
      .optional(),
  })
  .loose();

const postContentSchema = z
  .object({
    id: z.string().min(1),
    title: z.string(),
    selftext: z.string(),
    subreddit: z.string().min(1),
    created_at: z.iso.datetime({ offset: true }),
    score: z.number().int(),
    num_comments: z.number().int().nonnegative(),
    upvote_ratio: z.number().min(0).max(1).nullable(),
    permalink: z.string(),
    fetched_at: z.iso.datetime({ offset: true }).nullable(),
    scrape_status: analysisStatusSchema.optional(),
  })
  .loose();

const commentContentSchema = z
  .object({
    id: z.string().min(1),
    post_id: z.string().min(1),
    parent_id: z.string().nullable(),
    body: z.string(),
    score: z.number().int(),
    created_at: z.iso.datetime({ offset: true }),
    depth: z.number().int().nonnegative(),
    subreddit: z.string().min(1),
    permalink: z.string(),
    fetched_at: z.iso.datetime({ offset: true }).nullable(),
  })
  .loose();

const recordBase = {
  export_schema_version: z.literal(1),
  mineral: z.string().min(1),
  analyses: analysisBundleSchema,
  content_available: z.boolean().default(true),
  source_note: z.string().min(1).nullable().optional(),
};

export const postRecordSchema = z.object({
  ...recordBase,
  record_type: z.literal('post'),
  content: postContentSchema,
});

export const commentRecordSchema = z.object({
  ...recordBase,
  record_type: z.literal('comment'),
  content: commentContentSchema,
});

export const researchRecordSchema = z.discriminatedUnion('record_type', [
  postRecordSchema,
  commentRecordSchema,
]);

export const pipelineStageSchema = z.object({
  name: z.enum(['collect', 'relevance', 'enrichment', 'reputation', 'export']),
  status: z.enum(['complete', 'running', 'retrying', 'requeued', 'waiting', 'failed']),
  completed: z.number().int().nonnegative(),
  total: z.number().int().nonnegative(),
  duration_ms: z.number().int().nonnegative(),
});

export const pipelineRunSchema = z.object({
  id: z.string().min(1),
  mineral: z.string().min(1),
  started_at: z.iso.datetime({ offset: true }),
  status: z.enum(['complete', 'partial', 'failed']),
  stages: z.array(pipelineStageSchema),
});

export const apiSnapshotSchema = z.object({
  label: z.string().min(1),
  records: z.array(researchRecordSchema),
  runs: z.array(pipelineRunSchema).default([]),
});

const apiSentimentSchema = z.enum(['positive', 'negative', 'neutral', 'mixed']);
const apiStanceSchema = z.enum(['pro-mining', 'anti-mining', 'neutral', 'mixed']);
const apiControversySchema = z.enum(['high', 'medium', 'low']);

export const datasetModeSchema = z.enum(['public-research-sample', 'synthetic-demo']);

const sourceCountsSchema = z
  .object({
    minerals: z.number().int().nonnegative(),
    records: z.number().int().nonnegative(),
    posts: z.number().int().nonnegative(),
    comments: z.number().int().nonnegative(),
  })
  .strict()
  .superRefine((counts, context) => {
    if (counts.records !== counts.posts + counts.comments) {
      context.addIssue({
        code: 'custom',
        message: 'Record count must equal posts plus comments.',
        path: ['records'],
      });
    }
  });

const publishedDateRangeSchema = z
  .object({
    start: z.iso.date(),
    end: z.iso.date(),
  })
  .strict();

export const apiDatasetSourceSchema = z
  .object({
    kind: datasetModeSchema,
    public_sample: z.boolean(),
    dataset_label: z.string().min(1),
    dataset_description: z.string().min(1),
    owner_name: z.string().min(1).nullable(),
    dataset_ref: z.string().min(1).nullable(),
    source_url: z.url().nullable(),
    dataset_version: z.string().min(1),
    archive_sha256: z
      .string()
      .regex(/^[A-Fa-f0-9]{64}$/u)
      .nullable(),
    license: z.string().min(1).nullable(),
    published_at: z.iso.datetime({ offset: true }).nullable(),
    source_note: z.string().min(1),
    full_counts: sourceCountsSchema,
    sample_counts: sourceCountsSchema,
    published_date_range: publishedDateRangeSchema.nullable(),
    sample_method: z.string().min(1),
    raw_text_included: z.boolean(),
    authors_included: z.boolean(),
  })
  .strict()
  .superRefine((source, context) => {
    const shouldBePublic = source.kind === 'public-research-sample';
    if (source.public_sample !== shouldBePublic) {
      context.addIssue({
        code: 'custom',
        message: 'Dataset kind and public-sample flag disagree.',
        path: ['public_sample'],
      });
    }
    if (
      source.sample_counts.records > source.full_counts.records ||
      source.sample_counts.posts > source.full_counts.posts ||
      source.sample_counts.comments > source.full_counts.comments ||
      source.sample_counts.minerals > source.full_counts.minerals
    ) {
      context.addIssue({
        code: 'custom',
        message: 'Sample counts cannot exceed full-dataset counts.',
        path: ['sample_counts'],
      });
    }
    if (shouldBePublic && (source.raw_text_included || source.authors_included)) {
      context.addIssue({
        code: 'custom',
        message: 'The public sample must preserve the metadata-only publication boundary.',
        path: ['raw_text_included'],
      });
    }
  });

const canonicalProvenanceSchema = z
  .object({
    kind: z.literal('public-research-sample'),
    dataset_label: z.string().min(1),
    dataset_description: z.string().min(1),
    owner_name: z.string().min(1),
    dataset_ref: z.string().min(1),
    dataset_slug: z.string().min(1),
    dataset_url: z.url(),
    dataset_version: z.number().int().positive(),
    published_at: z.iso.datetime({ offset: true }),
    archive_sha256: z.string().regex(/^[A-Fa-f0-9]{64}$/u),
    license: z.string().min(1),
    published_totals: sourceCountsSchema,
    published_date_range: publishedDateRangeSchema,
    sample_totals: sourceCountsSchema,
    sample_method: z.string().min(1),
    raw_text_included: z.literal(false),
    authors_included: z.literal(false),
    source_note: z.string().min(1),
  })
  .strict();

export const apiMetaSchema = z
  .object({
    api_version: z.literal('v1'),
    dataset_label: z.string().min(1),
    dataset_description: z.string().min(1),
    mode: datasetModeSchema,
    synthetic: z.boolean(),
    public_sample: z.boolean(),
    read_only: z.literal(true),
    generated_at: z.iso.datetime({ offset: true }),
    minerals: z.array(z.string()),
    totals: z
      .object({
        minerals: z.number().int().nonnegative(),
        records: z.number().int().nonnegative(),
        posts: z.number().int().nonnegative(),
        comments: z.number().int().nonnegative(),
        analyses: z.number().int().nonnegative(),
        runs: z.number().int().nonnegative(),
      })
      .strict(),
    application_name: z.string(),
    application_version: z.string(),
    source: apiDatasetSourceSchema,
  })
  .strict()
  .superRefine((metadata, context) => {
    const shouldBePublic = metadata.mode === 'public-research-sample';
    if (
      metadata.synthetic === shouldBePublic ||
      metadata.public_sample !== shouldBePublic ||
      metadata.source.kind !== metadata.mode ||
      metadata.dataset_label !== metadata.source.dataset_label ||
      metadata.dataset_description !== metadata.source.dataset_description
    ) {
      context.addIssue({
        code: 'custom',
        message: 'Dataset metadata contains inconsistent provenance flags.',
        path: ['mode'],
      });
    }
  });

const apiRecordSummarySchema = z
  .object({
    id: z.string().min(1),
    kind: z.enum(['post', 'comment']),
    parent_id: z.string().nullable(),
    mineral: z.string().min(1),
    topic_label: z.string().min(1).max(120),
    title: z.string().nullable(),
    body_preview: z.string(),
    subreddit: z.string().min(1),
    created_at: z.iso.datetime({ offset: true }),
    score: z.number().int(),
    comment_count: z.number().int().nonnegative().nullable(),
    sentiment: apiSentimentSchema,
    stance: apiStanceSchema,
    relevance_confidence: z.number().min(0).max(100).nullable(),
    reputation_score: z.number().min(0).max(100).nullable(),
    controversy: apiControversySchema.nullable(),
    themes: z.array(z.string()),
    mode: datasetModeSchema,
    synthetic: z.boolean(),
    public_sample: z.boolean(),
    content_available: z.boolean(),
    source: apiDatasetSourceSchema,
  })
  .strict();

export const apiRecordPageSchema = z
  .object({
    page: z.number().int().positive(),
    page_size: z.number().int().positive(),
    total: z.number().int().nonnegative(),
    pages: z.number().int().nonnegative(),
    items: z.array(apiRecordSummarySchema),
    mode: datasetModeSchema,
    synthetic: z.boolean(),
    public_sample: z.boolean(),
    source: apiDatasetSourceSchema,
  })
  .strict();

export const apiRecordDetailSchema = z
  .object({
    id: z.string().min(1),
    kind: z.enum(['post', 'comment']),
    parent_id: z.string().nullable(),
    mineral: z.string().min(1),
    topic_label: z.string().min(1).max(120),
    title: z.string().nullable(),
    body: z.string(),
    subreddit: z.string().min(1),
    created_at: z.iso.datetime({ offset: true }),
    score: z.number().int(),
    comment_count: z.number().int().nonnegative().nullable(),
    analysis: z
      .object({
        relevance: z
          .object({
            relevant: z.boolean(),
            confidence: z.number().min(0).max(100),
            rationale: z.string(),
          })
          .strict()
          .nullable(),
        enrichment: z
          .object({
            sentiment: apiSentimentSchema,
            stance: apiStanceSchema,
            keywords: z.array(z.string()),
            themes: z.array(z.string()),
            concerns: z.array(
              z.object({ name: z.string().min(1), score: z.number().min(0).max(1) }).strict(),
            ),
          })
          .strict(),
        reputation: z
          .object({
            score: z.number().min(0).max(100),
            controversy: apiControversySchema,
            rationale: z.string(),
          })
          .strict()
          .nullable(),
      })
      .strict(),
    source_note: z.string().min(1),
    mode: datasetModeSchema,
    synthetic: z.boolean(),
    public_sample: z.boolean(),
    content_available: z.boolean(),
    source: apiDatasetSourceSchema,
  })
  .strict();

export const apiDatasetSnapshotSchema = z
  .object({
    mode: datasetModeSchema,
    synthetic: z.boolean(),
    public_sample: z.boolean(),
    source: apiDatasetSourceSchema,
    generated_at: z.iso.datetime({ offset: true }),
    records: z.array(apiRecordDetailSchema).max(5_000),
  })
  .strict();

const apiRunSummarySchema = z
  .object({
    id: z.string().min(1),
    command: z.enum(['scrape', 'relevance', 'enrichment', 'reputation', 'export']),
    status: z.enum(['succeeded', 'partial', 'failed']),
    started_at: z.iso.datetime({ offset: true }),
    finished_at: z.iso.datetime({ offset: true }),
    processed: z.number().int().nonnegative(),
    failed: z.number().int().nonnegative(),
    duration_ms: z.number().int().nonnegative(),
    synthetic: z.boolean(),
  })
  .strict();

export const apiRunPageSchema = z
  .object({
    page: z.number().int().positive(),
    page_size: z.number().int().positive(),
    total: z.number().int().nonnegative(),
    pages: z.number().int().nonnegative(),
    items: z.array(apiRunSummarySchema),
    mode: datasetModeSchema,
    synthetic: z.boolean(),
    public_sample: z.boolean(),
    source: apiDatasetSourceSchema,
  })
  .strict();

const canonicalRecordDetailSchema = z
  .object({
    id: z.string().min(1),
    kind: z.enum(['post', 'comment']),
    parent_id: z.string().nullable(),
    mineral: z.string().min(1),
    topic_label: z.string().min(1).max(120),
    title: z.string().nullable(),
    body: z.string(),
    subreddit: z.string().min(1),
    created_at: z.iso.datetime({ offset: true }),
    score: z.number().int(),
    comment_count: z.number().int().nonnegative().nullable(),
    analysis: apiRecordDetailSchema.shape.analysis,
    source_note: z.string().min(1),
    synthetic: z.literal(false),
    content_available: z.literal(false),
  })
  .strict();

export const canonicalResearchSampleSchema = z
  .object({
    schema_version: z.literal(1),
    provenance: canonicalProvenanceSchema,
    records: z.array(canonicalRecordDetailSchema).min(1),
  })
  .strict()
  .superRefine((sample, context) => {
    if (sample.records.length !== sample.provenance.sample_totals.records) {
      context.addIssue({
        code: 'custom',
        message: 'Canonical sample count does not match its provenance manifest.',
        path: ['records'],
      });
    }
  });

export type AnalysisStatus = z.infer<typeof analysisStatusSchema>;
export type ApiDatasetSource = z.infer<typeof apiDatasetSourceSchema>;
export type ApiRecordDetail = z.infer<typeof apiRecordDetailSchema>;
export type ApiRunPage = z.infer<typeof apiRunPageSchema>;
export type CanonicalResearchSample = z.infer<typeof canonicalResearchSampleSchema>;
export type EnrichmentResult = z.infer<typeof enrichmentResultSchema>;
export type PipelineRun = z.infer<typeof pipelineRunSchema>;
export type ResearchRecord = z.infer<typeof researchRecordSchema>;

export interface SnapshotProvenance {
  readonly kind: 'public-research-sample' | 'synthetic-demo' | 'local-import' | 'live-reddit';
  readonly datasetLabel: string;
  readonly datasetDescription: string;
  readonly synthetic: boolean;
  readonly publicSample: boolean;
  readonly sourceUrl: string | null;
  readonly datasetVersion: string | null;
  readonly sourceNote: string;
}

export interface ResearchSnapshot {
  readonly records: readonly ResearchRecord[];
  readonly runs: readonly PipelineRun[];
  readonly delivery: 'api' | 'bundled' | 'local' | 'live';
  readonly provenance: SnapshotProvenance;
  readonly notice?: string;
}
