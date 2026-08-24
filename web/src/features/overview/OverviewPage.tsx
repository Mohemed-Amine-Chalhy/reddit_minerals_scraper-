import { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { DistributionBars } from '../../components/DistributionBars';
import { ErrorState, LoadingState } from '../../components/LoadingState';
import { MetricCard } from '../../components/MetricCard';
import { PageHeader } from '../../components/PageHeader';
import { useResearch } from '../../app/research';
import {
  availableMinerals,
  availableTopics,
  EMPTY_FILTERS,
  enrichmentFor,
  filterRecords,
  recordTitle,
  summarize,
} from '../../domain/selectors';

type DateWindow = 'all' | '90' | '180';

function dateWindowStart(records: readonly { content: { created_at: string } }[], days: number) {
  if (!records.length) return '';
  const latest = Math.max(...records.map((record) => Date.parse(record.content.created_at)));
  return new Date(latest - days * 86_400_000).toISOString().slice(0, 10);
}

export function OverviewPage() {
  const { snapshot, loading, error } = useResearch();
  const [mineral, setMineral] = useState('');
  const [topic, setTopic] = useState('');
  const [dateWindow, setDateWindow] = useState<DateWindow>('all');

  const filtered = useMemo(() => {
    if (!snapshot) return [];
    return filterRecords(snapshot.records, {
      ...EMPTY_FILTERS,
      mineral,
      topic,
      from: dateWindow === 'all' ? '' : dateWindowStart(snapshot.records, Number(dateWindow)),
    });
  }, [snapshot, mineral, topic, dateWindow]);
  const summary = useMemo(() => summarize(filtered), [filtered]);

  if (loading) return <LoadingState />;
  if (error || !snapshot)
    return <ErrorState message={error ?? 'No research snapshot was loaded.'} />;

  const isLiveSnapshot = snapshot.delivery === 'live';
  const insights = filtered.filter((record) => record.record_type === 'post').slice(0, 3);
  const stages = [
    { label: 'Collected', value: summary.records },
    {
      label: 'Relevant',
      value: isLiveSnapshot
        ? 'not run'
        : filtered.filter((record) => record.analyses.relevance?.result?.relevant).length,
    },
    {
      label: 'Enriched',
      value: isLiveSnapshot
        ? 'not run'
        : filtered.filter((record) => record.analyses.enrichment?.status === 'complete').length,
    },
    {
      label: 'Perception',
      value: isLiveSnapshot
        ? 'not run'
        : filtered.some((record) => record.analyses.reputation)
          ? filtered.filter((record) => record.analyses.reputation?.status === 'complete').length
          : 'not published',
    },
    { label: isLiveSnapshot ? 'Explorer-ready' : 'Export-ready', value: filtered.length },
  ];

  return (
    <div className="page overview-page">
      <section className="hero-section">
        <PageHeader
          eyebrow="Critical-minerals research intelligence"
          title="Public discourse, engineered into inspectable evidence."
          description={
            isLiveSnapshot
              ? 'Shape a bounded Reddit collection, watch the job advance, and move the completed snapshot directly into an analysis-ready workspace.'
              : 'Explore a curated, provenance-rich window into a published 1.04M-record research dataset spanning 26 mineral topics—and the system that collected, structured, and analyzed it.'
          }
          actions={
            <>
              <Link className="button primary" to="/explorer">
                Explore the signals
              </Link>
              <Link className="button secondary" to="/engineering">
                See the engineering
              </Link>
            </>
          }
        />
        <div className="hero-orbit" aria-hidden="true">
          <span className="orbit orbit-one" />
          <span className="orbit orbit-two" />
          <span className="core-gem">ML</span>
          <span className="orbit-label label-a">validated</span>
          <span className="orbit-label label-b">resumable</span>
          <span className="orbit-label label-c">traceable</span>
        </div>
      </section>

      <section className="control-deck" aria-label="Dashboard controls">
        <div className="control-heading">
          <span className="live-dot" aria-hidden="true" />
          <div>
            <strong>{snapshot.provenance.datasetLabel}</strong>
            <span>{snapshot.notice ?? snapshot.provenance.datasetDescription}</span>
          </div>
        </div>
        <label>
          Mineral
          <select value={mineral} onChange={(event) => setMineral(event.target.value)}>
            <option value="">All minerals</option>
            {availableMinerals(snapshot.records).map((item) => (
              <option key={item}>{item}</option>
            ))}
          </select>
        </label>
        <label>
          Date window
          <select
            value={dateWindow}
            onChange={(event) => setDateWindow(event.target.value as DateWindow)}
          >
            <option value="all">All dates</option>
            <option value="90">Latest 90 days</option>
            <option value="180">Latest 180 days</option>
          </select>
        </label>
        <label>
          Topic
          <select value={topic} onChange={(event) => setTopic(event.target.value)}>
            <option value="">All topics</option>
            {availableTopics(snapshot.records).map((item) => (
              <option key={item}>{item}</option>
            ))}
          </select>
        </label>
      </section>

      <section className="metric-grid" aria-label="Research snapshot summary">
        <MetricCard
          label="Content records"
          value={summary.records}
          detail={`${summary.posts} posts · ${summary.comments} comments`}
          tone="gold"
        />
        <MetricCard
          label="Relevant post labels"
          value={isLiveSnapshot ? '—' : `${summary.relevantPercent}%`}
          detail={
            isLiveSnapshot ? 'not run for this raw collection' : 'released model-derived decisions'
          }
          tone="teal"
        />
        <MetricCard
          label="Analysis completion"
          value={isLiveSnapshot ? '—' : `${summary.completeAnalysesPercent}%`}
          detail={isLiveSnapshot ? 'no analysis was generated' : 'across available stages'}
          tone="blue"
        />
        <MetricCard
          label="Perception signal"
          value={summary.averagePerception === null ? '—' : `${summary.averagePerception}/100`}
          detail={
            summary.averagePerception === null
              ? isLiveSnapshot
                ? 'not run for this raw collection'
                : 'not included in this release'
              : 'model-derived content-level average'
          }
        />
      </section>

      <section className="dashboard-grid">
        <DistributionBars
          title="Sentiment composition"
          description="Completed enrichment labels in the current selection."
          data={summary.sentiment}
        />
        <DistributionBars
          title="Strongest concern signals"
          description="Mean model-derived signal, not a verified event count."
          data={summary.concerns}
          percent
        />
      </section>

      <section className="panel pipeline-snapshot">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Pipeline snapshot</p>
            <h2>Every stage leaves an inspectable state.</h2>
          </div>
          <Link className="text-link" to="/pipeline">
            Replay the pipeline <span aria-hidden="true">→</span>
          </Link>
        </div>
        <ol className="stage-rail">
          {stages.map((stage, index) => (
            <li key={stage.label}>
              <span className="stage-node">{String(index + 1).padStart(2, '0')}</span>
              <div>
                <strong>{stage.label}</strong>
                <span>
                  {typeof stage.value === 'number' ? `${stage.value} records` : stage.value}
                </span>
              </div>
            </li>
          ))}
        </ol>
      </section>

      <section className="insights-section">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Selected signals</p>
            <h2>Research records, with their context attached.</h2>
          </div>
        </div>
        <div className="insight-grid">
          {insights.map((record) => {
            const enrichment = enrichmentFor(record);
            return (
              <article className="insight-card" key={`${record.mineral}-${record.content.id}`}>
                <div className="insight-topline">
                  <span className="mineral-chip">{record.mineral}</span>
                  <span>
                    {enrichment?.sentiment ?? (isLiveSnapshot ? 'not analyzed' : 'pending')}
                  </span>
                </div>
                <h3>{recordTitle(record)}</h3>
                <p>
                  {enrichment?.topic_classification ??
                    (isLiveSnapshot
                      ? 'Raw Reddit record · no topic inferred'
                      : 'Awaiting enrichment')}
                </p>
                <Link to={`/explorer?mineral=${encodeURIComponent(record.mineral)}`}>
                  Inspect related records <span aria-hidden="true">↗</span>
                </Link>
              </article>
            );
          })}
        </div>
      </section>
    </div>
  );
}
