import type { AnalysisStatus, EnrichmentResult, ResearchRecord } from './schemas';

export interface ExplorerFilters {
  readonly query: string;
  readonly mineral: string;
  readonly recordType: '' | 'post' | 'comment';
  readonly sentiment: '' | 'positive' | 'negative' | 'neutral' | 'mixed';
  readonly status: '' | AnalysisStatus;
  readonly topic: string;
  readonly from: string;
  readonly to: string;
}

export interface OverviewSummary {
  readonly records: number;
  readonly posts: number;
  readonly comments: number;
  readonly relevantPercent: number;
  readonly completeAnalysesPercent: number;
  readonly averagePerception: number | null;
  readonly sentiment: readonly DistributionDatum[];
  readonly concerns: readonly DistributionDatum[];
}

export interface DistributionDatum {
  readonly label: string;
  readonly value: number;
  readonly tone: 'gold' | 'teal' | 'blue' | 'coral' | 'neutral';
}

export const EMPTY_FILTERS: ExplorerFilters = {
  query: '',
  mineral: '',
  recordType: '',
  sentiment: '',
  status: '',
  topic: '',
  from: '',
  to: '',
};

export function recordText(record: ResearchRecord): string {
  if (!record.content_available) return '';
  return record.record_type === 'post'
    ? `${record.content.title} ${record.content.selftext}`
    : record.content.body;
}

export function recordTitle(record: ResearchRecord): string {
  if (!record.content_available) {
    const mineral = record.mineral.replace(/^./u, (letter) => letter.toUpperCase());
    const topic = enrichmentFor(record)?.topic_classification;
    return `${mineral} ${record.record_type} metadata${topic ? ` · ${topic}` : ''}`;
  }
  if (record.record_type === 'post') return record.content.title;
  const body = record.content.body;
  return body.length > 84 ? `${body.slice(0, 81)}…` : body;
}

export function enrichmentFor(record: ResearchRecord): EnrichmentResult | null {
  return record.analyses.enrichment?.result ?? null;
}

export function analysisStatuses(record: ResearchRecord): AnalysisStatus[] {
  return [
    record.analyses.relevance,
    record.analyses.enrichment,
    record.analyses.reputation,
  ].flatMap((analysis) => {
    return analysis ? [analysis.status] : [];
  });
}

export function filterRecords(
  records: readonly ResearchRecord[],
  filters: ExplorerFilters,
): ResearchRecord[] {
  const query = filters.query.trim().toLowerCase();
  const topic = filters.topic.trim().toLowerCase();
  const from = filters.from ? Date.parse(`${filters.from}T00:00:00Z`) : null;
  const to = filters.to ? Date.parse(`${filters.to}T23:59:59Z`) : null;

  return records.filter((record) => {
    const enrichment = enrichmentFor(record);
    const created = Date.parse(record.content.created_at);
    const searchable =
      `${recordText(record)} ${record.mineral} ${record.content.subreddit}`.toLowerCase();
    const topics = enrichment
      ? [enrichment.topic_classification, ...enrichment.themes, ...enrichment.keywords]
          .join(' ')
          .toLowerCase()
      : '';
    return (
      (!query || searchable.includes(query)) &&
      (!filters.mineral || record.mineral === filters.mineral) &&
      (!filters.recordType || record.record_type === filters.recordType) &&
      (!filters.sentiment || enrichment?.sentiment === filters.sentiment) &&
      (!filters.status || analysisStatuses(record).includes(filters.status)) &&
      (!topic || topics.includes(topic)) &&
      (from === null || created >= from) &&
      (to === null || created <= to)
    );
  });
}

export function availableMinerals(records: readonly ResearchRecord[]): string[] {
  return [...new Set(records.map((record) => record.mineral))].sort();
}

export function availableTopics(records: readonly ResearchRecord[]): string[] {
  return [
    ...new Set(
      records.flatMap((record) => {
        const enrichment = enrichmentFor(record);
        return enrichment ? [enrichment.topic_classification, ...enrichment.themes] : [];
      }),
    ),
  ].sort();
}

function humanizeConcern(value: string): string {
  return value.replaceAll('_', ' ').replace(/^./u, (letter) => letter.toUpperCase());
}

export function summarize(records: readonly ResearchRecord[]): OverviewSummary {
  const completed = records.flatMap((record) =>
    [record.analyses.relevance, record.analyses.enrichment, record.analyses.reputation].flatMap(
      (analysis) => {
        return analysis ? [analysis.status === 'complete'] : [];
      },
    ),
  );
  const relevance = records.flatMap((record) => {
    const result = record.analyses.relevance?.result;
    return result ? [result.relevant] : [];
  });
  const perception = records.flatMap((record) => {
    const result = record.analyses.reputation?.result;
    return result ? [result.overall_reputation_score] : [];
  });
  const sentiments = new Map<string, number>();
  const concerns = new Map<string, number[]>();
  for (const record of records) {
    const enrichment = enrichmentFor(record);
    if (!enrichment) continue;
    sentiments.set(enrichment.sentiment, (sentiments.get(enrichment.sentiment) ?? 0) + 1);
    for (const [name, score] of Object.entries(enrichment.concerns)) {
      if (score <= 0.15) continue;
      const values = concerns.get(name) ?? [];
      values.push(score);
      concerns.set(name, values);
    }
  }
  const sentimentTones: Record<string, DistributionDatum['tone']> = {
    positive: 'teal',
    mixed: 'gold',
    neutral: 'blue',
    negative: 'coral',
  };
  return {
    records: records.length,
    posts: records.filter((record) => record.record_type === 'post').length,
    comments: records.filter((record) => record.record_type === 'comment').length,
    relevantPercent: relevance.length
      ? Math.round((relevance.filter(Boolean).length / relevance.length) * 100)
      : 0,
    completeAnalysesPercent: completed.length
      ? Math.round((completed.filter(Boolean).length / completed.length) * 100)
      : 0,
    averagePerception: perception.length
      ? Math.round(perception.reduce((total, score) => total + score, 0) / perception.length)
      : null,
    sentiment: [...sentiments.entries()]
      .map(([label, value]) => ({ label, value, tone: sentimentTones[label] ?? 'neutral' }))
      .sort((left, right) => right.value - left.value),
    concerns: [...concerns.entries()]
      .map(([label, values]) => ({
        label: humanizeConcern(label),
        value: Math.round(
          (values.reduce((total, score) => total + score, 0) / values.length) * 100,
        ),
        tone: 'gold' as const,
      }))
      .sort((left, right) => right.value - left.value)
      .slice(0, 6),
  };
}
