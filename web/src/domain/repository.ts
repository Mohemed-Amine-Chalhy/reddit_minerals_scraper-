import type { ZodType } from 'zod';
import { DEMO_RUNS, DEMO_RECORDS } from '../data/fixtures';
import canonicalSampleDocument from '../../../src/reddit_minerals/web/data/kaggle_sample.json';
import {
  canonicalResearchSampleSchema,
  apiDatasetSnapshotSchema,
  apiMetaSchema,
  apiRunPageSchema,
  pipelineRunSchema,
  researchRecordSchema,
  type ApiDatasetSource,
  type ApiRecordDetail,
  type ApiRunPage,
  type CanonicalResearchSample,
  type PipelineRun,
  type ResearchRecord,
  type ResearchSnapshot,
  type SnapshotProvenance,
} from './schemas';

export interface ResearchRepository {
  load(): Promise<ResearchSnapshot>;
}

export class SyntheticFixtureResearchRepository implements ResearchRepository {
  public async load(): Promise<ResearchSnapshot> {
    return Promise.resolve({
      records: DEMO_RECORDS,
      runs: DEMO_RUNS,
      delivery: 'bundled',
      provenance: {
        kind: 'synthetic-demo',
        datasetLabel: 'Synthetic minerals engineering fixture',
        datasetDescription:
          'Hand-authored deterministic records reserved for regression tests and offline pipeline replay.',
        synthetic: true,
        publicSample: false,
        sourceUrl: null,
        datasetVersion: 'synthetic-demo-v1',
        sourceNote: 'These records were not collected from Reddit.',
      },
    });
  }
}

const canonicalSample = canonicalResearchSampleSchema.parse(canonicalSampleDocument);

function canonicalProvenance(sample: CanonicalResearchSample): SnapshotProvenance {
  return {
    kind: sample.provenance.kind,
    datasetLabel: sample.provenance.dataset_label,
    datasetDescription: sample.provenance.dataset_description,
    synthetic: false,
    publicSample: true,
    sourceUrl: sample.provenance.dataset_url,
    datasetVersion: String(sample.provenance.dataset_version),
    sourceNote: sample.provenance.source_note,
  };
}

export class PublicSampleResearchRepository implements ResearchRepository {
  public async load(): Promise<ResearchSnapshot> {
    const records = canonicalSample.records.map(apiDetailToRecord);
    return Promise.resolve({
      records,
      runs: [],
      delivery: 'bundled',
      provenance: canonicalProvenance(canonicalSample),
      notice: `${records.length} deterministic sample records from ${canonicalSample.provenance.published_totals.records.toLocaleString('en')} published rows. Raw Reddit text and authors are not included.`,
    });
  }
}

function analysisMetadata() {
  return {
    schema_version: null,
    prompt_version: null,
    model: null,
    status: 'complete',
    error: null,
    input_tokens: null,
    output_tokens: null,
    latency_ms: null,
    updated_at: null,
  } as const;
}

type PresentationRecordDetail = ApiRecordDetail | CanonicalResearchSample['records'][number];

function apiDetailToRecord(detail: PresentationRecordDetail): ResearchRecord {
  const enrichment = detail.analysis.enrichment;
  const concerns = Object.fromEntries(
    enrichment.concerns.map((concern) => [concern.name.replaceAll(' ', '_'), concern.score]),
  );
  const analyses = {
    ...(detail.analysis.relevance
      ? {
          relevance: {
            ...analysisMetadata(),
            result: {
              ...detail.analysis.relevance,
              matched_topics: [],
            },
          },
        }
      : {}),
    enrichment: {
      ...analysisMetadata(),
      result: {
        sentiment: enrichment.sentiment,
        keywords: enrichment.keywords,
        themes: enrichment.themes,
        concerns,
        mining_stance: enrichment.stance,
        topic_classification: detail.topic_label,
        relevance_score: (detail.analysis.relevance?.confidence ?? 100) / 100,
      },
    },
    ...(detail.analysis.reputation
      ? {
          reputation: {
            ...analysisMetadata(),
            result: {
              overall_reputation_score: detail.analysis.reputation.score,
              sentiment: enrichment.sentiment,
              sentiment_score: null,
              credibility: 'unknown',
              credibility_score: null,
              market_impact: 'unclear',
              market_impact_score: null,
              controversy_level: detail.analysis.reputation.controversy,
              rationale: detail.analysis.reputation.rationale,
              evidence_signals: [],
            },
          },
        }
      : {}),
  };
  const common = {
    id: detail.id,
    subreddit: detail.subreddit,
    score: detail.score,
    created_at: detail.created_at,
    fetched_at: null,
    permalink: '',
    source_note: detail.source_note,
  };
  const raw =
    detail.kind === 'post'
      ? {
          export_schema_version: 1,
          record_type: 'post',
          mineral: detail.mineral,
          content_available: detail.content_available,
          source_note: detail.source_note,
          content: {
            ...common,
            title: detail.title ?? '',
            selftext: detail.body,
            num_comments: detail.comment_count ?? 0,
            upvote_ratio: null,
          },
          analyses,
        }
      : {
          export_schema_version: 1,
          record_type: 'comment',
          mineral: detail.mineral,
          content_available: detail.content_available,
          source_note: detail.source_note,
          content: {
            ...common,
            post_id: detail.parent_id ?? detail.id,
            parent_id: detail.parent_id,
            body: detail.body,
            depth: 0,
          },
          analyses,
        };
  return researchRecordSchema.parse(raw);
}

function apiProvenance(source: ApiDatasetSource, synthetic: boolean): SnapshotProvenance {
  return {
    kind: source.kind,
    datasetLabel: source.dataset_label,
    datasetDescription: source.dataset_description,
    synthetic,
    publicSample: source.public_sample,
    sourceUrl: source.source_url,
    datasetVersion: source.dataset_version,
    sourceNote: source.source_note,
  };
}

const stageOrder = ['scrape', 'relevance', 'enrichment', 'reputation', 'export'] as const;
const stageName = {
  scrape: 'collect',
  relevance: 'relevance',
  enrichment: 'enrichment',
  reputation: 'reputation',
  export: 'export',
} as const;

function aggregateApiRuns(page: ApiRunPage, generatedAt: string): readonly PipelineRun[] {
  if (!page.items.length) return [];
  const overallStatus = page.items.some((run) => run.status === 'failed')
    ? 'failed'
    : page.items.some((run) => run.status === 'partial')
      ? 'partial'
      : 'complete';
  const startedAt =
    [...page.items]
      .map((run) => run.started_at)
      .sort((left, right) => Date.parse(left) - Date.parse(right))[0] ?? generatedAt;
  return [
    pipelineRunSchema.parse({
      id: 'api-synthetic-pipeline',
      mineral: 'all minerals',
      started_at: startedAt,
      status: overallStatus,
      stages: stageOrder.map((command) => {
        const run = page.items.find((candidate) => candidate.command === command);
        return {
          name: stageName[command],
          status:
            run?.status === 'failed'
              ? 'failed'
              : run?.status === 'partial'
                ? 'retrying'
                : run
                  ? 'complete'
                  : 'waiting',
          completed: run?.processed ?? 0,
          total: (run?.processed ?? 0) + (run?.failed ?? 0),
          duration_ms: run?.duration_ms ?? 0,
        };
      }),
    }),
  ];
}

async function fetchAndParse<Output>(url: string, schema: ZodType<Output>): Promise<Output> {
  const response = await fetch(url, {
    headers: { Accept: 'application/json' },
    signal: AbortSignal.timeout(4_000),
  });
  if (!response.ok) throw new Error(`Research API returned ${response.status}`);
  return schema.parse(await response.json());
}

function sameCounts(
  left: ApiDatasetSource['full_counts'],
  right: ApiDatasetSource['full_counts'],
): boolean {
  return (
    left.minerals === right.minerals &&
    left.records === right.records &&
    left.posts === right.posts &&
    left.comments === right.comments
  );
}

function sameDateRange(
  left: ApiDatasetSource['published_date_range'],
  right: ApiDatasetSource['published_date_range'],
): boolean {
  return left === null || right === null
    ? left === right
    : left.start === right.start && left.end === right.end;
}

function sameDatasetSource(left: ApiDatasetSource, right: ApiDatasetSource): boolean {
  return (
    left.kind === right.kind &&
    left.public_sample === right.public_sample &&
    left.dataset_label === right.dataset_label &&
    left.dataset_description === right.dataset_description &&
    left.owner_name === right.owner_name &&
    left.dataset_ref === right.dataset_ref &&
    left.source_url === right.source_url &&
    left.dataset_version === right.dataset_version &&
    left.archive_sha256 === right.archive_sha256 &&
    left.license === right.license &&
    left.published_at === right.published_at &&
    left.source_note === right.source_note &&
    sameCounts(left.full_counts, right.full_counts) &&
    sameCounts(left.sample_counts, right.sample_counts) &&
    sameDateRange(left.published_date_range, right.published_date_range) &&
    left.sample_method === right.sample_method &&
    left.raw_text_included === right.raw_text_included &&
    left.authors_included === right.authors_included
  );
}

export class ApiResearchRepository implements ResearchRepository {
  public constructor(
    private readonly basePath: string,
    private readonly fallback: ResearchRepository = new PublicSampleResearchRepository(),
  ) {}

  public async load(): Promise<ResearchSnapshot> {
    try {
      const [meta, datasetSnapshot, runPage] = await Promise.all([
        fetchAndParse(`${this.basePath}/meta`, apiMetaSchema),
        fetchAndParse(`${this.basePath}/snapshot`, apiDatasetSnapshotSchema),
        fetchAndParse(`${this.basePath}/runs?page_size=50`, apiRunPageSchema),
      ]);
      const transfers = [datasetSnapshot, runPage];
      if (
        transfers.some(
          (transfer) =>
            transfer.mode !== meta.mode ||
            transfer.synthetic !== meta.synthetic ||
            transfer.public_sample !== meta.public_sample ||
            !sameDatasetSource(transfer.source, meta.source),
        )
      ) {
        throw new Error('Research API returned inconsistent dataset provenance.');
      }
      if (
        datasetSnapshot.generated_at !== meta.generated_at ||
        datasetSnapshot.records.length !== meta.totals.records ||
        new Set(datasetSnapshot.records.map((record) => record.id)).size !==
          datasetSnapshot.records.length
      ) {
        throw new Error('Research API returned an inconsistent bounded snapshot.');
      }
      if (
        datasetSnapshot.records.some(
          (detail) =>
            detail.mode !== meta.mode ||
            detail.synthetic !== meta.synthetic ||
            detail.public_sample !== meta.public_sample ||
            !sameDatasetSource(detail.source, meta.source),
        )
      ) {
        throw new Error('Research API returned records from inconsistent dataset sources.');
      }
      if (runPage.items.some((run) => run.synthetic !== meta.synthetic)) {
        throw new Error('Research API returned runs with inconsistent provenance flags.');
      }
      return {
        records: datasetSnapshot.records.map(apiDetailToRecord),
        runs: aggregateApiRuns(runPage, meta.generated_at),
        delivery: 'api',
        provenance: apiProvenance(meta.source, meta.synthetic),
        notice: `${meta.dataset_description} Served by the read-only research API.`,
      };
    } catch {
      const snapshot = await this.fallback.load();
      return {
        ...snapshot,
        notice:
          'The optional local API was unavailable; the bundled deterministic public research sample is shown.',
      };
    }
  }
}
