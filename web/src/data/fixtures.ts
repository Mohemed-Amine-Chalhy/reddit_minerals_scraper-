import {
  pipelineRunSchema,
  researchRecordSchema,
  type PipelineRun,
  type ResearchRecord,
} from '../domain/schemas';

interface MineralProfile {
  readonly mineral: string;
  readonly region: string;
  readonly subreddit: string;
  readonly topic: string;
  readonly secondaryTopic: string;
  readonly concern: string;
  readonly concernLabel: string;
  readonly keywords: readonly [string, string];
}

export const MINERAL_PROFILES: readonly MineralProfile[] = [
  {
    mineral: 'gold',
    region: 'Western Australia',
    subreddit: 'mining',
    topic: 'circular supply',
    secondaryTopic: 'water stewardship',
    concern: 'sustainability',
    concernLabel: 'Sustainability',
    keywords: ['recycling', 'refining'],
  },
  {
    mineral: 'lithium',
    region: 'Atacama region',
    subreddit: 'lithium',
    topic: 'battery supply',
    secondaryTopic: 'brine management',
    concern: 'water_contamination',
    concernLabel: 'Water quality',
    keywords: ['batteries', 'brine'],
  },
  {
    mineral: 'copper',
    region: 'Northern Chile',
    subreddit: 'CopperMining',
    topic: 'grid infrastructure',
    secondaryTopic: 'community reporting',
    concern: 'local_employment',
    concernLabel: 'Local employment',
    keywords: ['electrification', 'concentrate'],
  },
  {
    mineral: 'cobalt',
    region: 'Central Africa',
    subreddit: 'cobalt',
    topic: 'traceable sourcing',
    secondaryTopic: 'working conditions',
    concern: 'working_conditions',
    concernLabel: 'Working conditions',
    keywords: ['traceability', 'sourcing'],
  },
  {
    mineral: 'nickel',
    region: 'Sulawesi',
    subreddit: 'NickelMining',
    topic: 'processing capacity',
    secondaryTopic: 'air quality',
    concern: 'air_quality',
    concernLabel: 'Air quality',
    keywords: ['processing', 'capacity'],
  },
  {
    mineral: 'graphite',
    region: 'Northern Mozambique',
    subreddit: 'graphite',
    topic: 'anode materials',
    secondaryTopic: 'local value chains',
    concern: 'economic_benefits',
    concernLabel: 'Economic benefits',
    keywords: ['anodes', 'beneficiation'],
  },
] as const;

function timestamp(profileIndex: number, dayOffset: number, hour = 9): string {
  return new Date(Date.UTC(2026, profileIndex + 1, 4 + dayOffset, hour, 0, 0)).toISOString();
}

function envelope(
  kind: 'relevance' | 'enrichment' | 'reputation',
  result: unknown,
  profileIndex: number,
  status: 'complete' | 'retryable_failure' | 'blocked' = 'complete',
): Record<string, unknown> {
  return {
    schema_version: 1,
    prompt_version: 'portfolio-demo-v1',
    model: 'synthetic-demo-v1',
    status,
    error:
      status === 'retryable_failure'
        ? 'SyntheticRateLimit'
        : status === 'blocked'
          ? 'SyntheticSafetyBlock'
          : null,
    input_tokens: status === 'complete' ? 32 + profileIndex : null,
    output_tokens: status === 'complete' ? 18 + profileIndex : null,
    latency_ms: status === 'complete' ? 42 + profileIndex * 7 : 0,
    updated_at: timestamp(profileIndex, 8, 15),
    result: status === 'complete' ? result : null,
    kind,
  };
}

function relevance(profile: MineralProfile, relevant = true): Record<string, unknown> {
  return {
    relevant,
    confidence: relevant ? 92 : 61,
    rationale: relevant
      ? `The synthetic record directly discusses ${profile.mineral} and ${profile.topic}.`
      : 'The mineral is mentioned, but the main subject is broader infrastructure policy.',
    matched_topics: relevant ? [profile.mineral, profile.topic] : [profile.mineral],
  };
}

function enrichment(
  profile: MineralProfile,
  sentiment: 'positive' | 'negative' | 'neutral' | 'mixed',
  secondary = false,
): Record<string, unknown> {
  return {
    sentiment,
    keywords: [profile.mineral, ...profile.keywords],
    themes: [secondary ? profile.secondaryTopic : profile.topic, 'operational transparency'],
    concerns: {
      [profile.concern]: secondary ? 0.84 : 0.63,
      sustainability: 0.58,
      community_rights: secondary ? 0.55 : 0.24,
      economic_benefits: secondary ? 0.41 : 0.69,
    },
    mining_stance:
      sentiment === 'positive' ? 'pro-mining' : sentiment === 'negative' ? 'anti-mining' : 'mixed',
    topic_classification: secondary ? profile.secondaryTopic : profile.topic,
    relevance_score: secondary ? 0.88 : 0.96,
  };
}

function reputation(
  profile: MineralProfile,
  sentiment: 'positive' | 'negative' | 'neutral' | 'mixed',
  secondary = false,
): Record<string, unknown> {
  const score = sentiment === 'positive' ? 78 : sentiment === 'negative' ? 42 : 64;
  return {
    overall_reputation_score: score,
    sentiment,
    sentiment_score: score,
    credibility: secondary ? 'medium' : 'high',
    credibility_score: secondary ? 67 : 81,
    market_impact:
      sentiment === 'positive' ? 'positive' : sentiment === 'negative' ? 'negative' : 'unclear',
    market_impact_score: secondary ? 48 : 70,
    controversy_level: secondary ? 'medium' : 'low',
    rationale: `The synthetic ${profile.mineral} discussion contains bounded, reviewable perception signals.`,
    evidence_signals: [
      secondary ? profile.secondaryTopic : profile.topic,
      'specific operational detail',
    ],
  };
}

function buildRecords(profile: MineralProfile, index: number): ResearchRecord[] {
  const id = profile.mineral.replaceAll(' ', '-');
  const sentiment =
    (['positive', 'mixed', 'neutral', 'negative', 'mixed', 'positive'] as const)[index] ??
    'neutral';
  const secondRelevant = index !== 5;
  const secondEnrichmentStatus = index === 3 ? 'retryable_failure' : 'complete';
  const secondReputationStatus = index === 4 ? 'blocked' : 'complete';

  const rawRecords: unknown[] = [
    {
      export_schema_version: 1,
      record_type: 'post',
      mineral: profile.mineral,
      content: {
        id: `${id}-supply`,
        title: `${profile.mineral.charAt(0).toUpperCase()}${profile.mineral.slice(1)} projects reshape ${profile.topic}`,
        selftext: `A synthetic briefing from ${profile.region} compares engineering capacity, transparent reporting, and long-term supply resilience.`,
        subreddit: profile.subreddit,
        created_at: timestamp(index, 0),
        score: 96 + index * 13,
        num_comments: 18 + index,
        upvote_ratio: 0.82 + index * 0.02,
        permalink: `https://example.invalid/r/${profile.subreddit}/${id}-supply`,
        fetched_at: timestamp(index, 0, 10),
        scrape_status: 'complete',
      },
      analyses: {
        relevance: envelope('relevance', relevance(profile), index),
        enrichment: envelope('enrichment', enrichment(profile, sentiment), index),
        reputation: envelope('reputation', reputation(profile, sentiment), index),
      },
    },
    {
      export_schema_version: 1,
      record_type: 'post',
      mineral: profile.mineral,
      content: {
        id: `${id}-community`,
        title: `What transparent ${profile.secondaryTopic} could look like in ${profile.region}`,
        selftext: `Researchers and engineers compare measurements, publication intervals, and community feedback in this synthetic scenario.`,
        subreddit: profile.subreddit,
        created_at: timestamp(index, 3, 13),
        score: 54 + index * 8,
        num_comments: 9 + index,
        upvote_ratio: 0.74 + index * 0.02,
        permalink: `https://example.invalid/r/${profile.subreddit}/${id}-community`,
        fetched_at: timestamp(index, 3, 14),
        scrape_status: 'complete',
      },
      analyses: {
        relevance: envelope('relevance', relevance(profile, secondRelevant), index),
        enrichment: envelope(
          'enrichment',
          enrichment(profile, 'mixed', true),
          index,
          secondEnrichmentStatus,
        ),
        ...(secondRelevant
          ? {
              reputation: envelope(
                'reputation',
                reputation(profile, 'mixed', true),
                index,
                secondReputationStatus,
              ),
            }
          : {}),
      },
    },
    {
      export_schema_version: 1,
      record_type: 'comment',
      mineral: profile.mineral,
      content: {
        id: `${id}-c1`,
        post_id: `${id}-supply`,
        parent_id: `t3_${id}-supply`,
        body: `The strongest part of the proposal is its measurable ${profile.topic} target.`,
        score: 21 + index * 3,
        created_at: timestamp(index, 0, 11),
        depth: 0,
        subreddit: profile.subreddit,
        permalink: `https://example.invalid/r/${profile.subreddit}/${id}-supply/${id}-c1`,
        fetched_at: timestamp(index, 0, 12),
      },
      analyses: {
        enrichment: envelope('enrichment', enrichment(profile, 'positive'), index),
      },
    },
    {
      export_schema_version: 1,
      record_type: 'comment',
      mineral: profile.mineral,
      content: {
        id: `${id}-c2`,
        post_id: `${id}-community`,
        parent_id: `t3_${id}-community`,
        body: `Publishing the methodology would make the ${profile.secondaryTopic} claims easier to review.`,
        score: 12 + index * 2,
        created_at: timestamp(index, 3, 15),
        depth: 0,
        subreddit: profile.subreddit,
        permalink: `https://example.invalid/r/${profile.subreddit}/${id}-community/${id}-c2`,
        fetched_at: timestamp(index, 3, 16),
      },
      analyses: {
        enrichment: envelope('enrichment', enrichment(profile, 'mixed', true), index),
      },
    },
  ];
  return rawRecords.map((record) => researchRecordSchema.parse(record));
}

export const DEMO_RECORDS: readonly ResearchRecord[] = MINERAL_PROFILES.flatMap(buildRecords);

export const DEMO_RUNS: readonly PipelineRun[] = MINERAL_PROFILES.map((profile, index) =>
  pipelineRunSchema.parse({
    id: `run-${index + 1}`,
    mineral: profile.mineral,
    started_at: timestamp(index, 7, 8),
    status: index === 3 ? 'partial' : 'complete',
    stages: [
      {
        name: 'collect',
        status: 'complete',
        completed: 4,
        total: 4,
        duration_ms: 460 + index * 15,
      },
      {
        name: 'relevance',
        status: 'complete',
        completed: 2,
        total: 2,
        duration_ms: 180 + index * 9,
      },
      {
        name: 'enrichment',
        status: index === 3 ? 'retrying' : 'complete',
        completed: index === 3 ? 3 : 4,
        total: 4,
        duration_ms: 330 + index * 12,
      },
      {
        name: 'reputation',
        status: index === 4 ? 'waiting' : 'complete',
        completed: 2,
        total: 2,
        duration_ms: 240 + index * 11,
      },
      { name: 'export', status: 'complete', completed: 4, total: 4, duration_ms: 34 + index },
    ],
  }),
);
