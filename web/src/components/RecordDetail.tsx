import { useEffect, useRef } from 'react';
import type { ResearchRecord } from '../domain/schemas';
import { enrichmentFor, recordText, recordTitle } from '../domain/selectors';
import { StatusPill } from './StatusPill';

interface RecordDetailProps {
  readonly record: ResearchRecord;
  readonly onClose: () => void;
}

function formatDate(value: string): string {
  return new Intl.DateTimeFormat('en', {
    dateStyle: 'medium',
    timeStyle: 'short',
    timeZone: 'UTC',
  }).format(new Date(value));
}

export function RecordDetail({ record, onClose }: RecordDetailProps) {
  const headingRef = useRef<HTMLHeadingElement>(null);
  const enrichment = enrichmentFor(record);
  const relevance = record.analyses.relevance;
  const reputation = record.analyses.reputation;
  const hasAnalysis = Object.keys(record.analyses).length > 0;
  useEffect(() => headingRef.current?.focus(), [record.content.id]);

  return (
    <aside className="record-detail" aria-labelledby="record-detail-title">
      <div className="detail-topline">
        <span className="mineral-chip">{record.mineral}</span>
        <button className="close-button" type="button" onClick={onClose} aria-label="Close detail">
          ×
        </button>
      </div>
      <p className="eyebrow">
        {record.record_type} · r/{record.content.subreddit}
      </p>
      <h2 id="record-detail-title" ref={headingRef} tabIndex={-1}>
        {recordTitle(record)}
      </h2>
      <p className="detail-body">
        {record.content_available
          ? recordText(record)
          : 'Raw Reddit text is not included in this public research sample.'}
      </p>
      {!record.content_available && record.source_note ? (
        <p className="detail-source-note">{record.source_note}</p>
      ) : null}
      <div className="detail-meta">
        <span>{formatDate(record.content.created_at)}</span>
        <span>{record.content.score} score</span>
        {record.content.permalink ? (
          <a href={record.content.permalink} target="_blank" rel="noreferrer">
            Source reference <span aria-hidden="true">↗</span>
          </a>
        ) : (
          <span>
            {record.content_available ? 'Source link not included' : 'Metadata-only public sample'}
          </span>
        )}
      </div>

      <div className="analysis-stack">
        {!hasAnalysis ? (
          <section className="analysis-card raw-analysis-card">
            <div className="analysis-heading">
              <div>
                <span className="analysis-index">00</span>
                <h3>Raw collection only</h3>
              </div>
              <span className="raw-status">Not analyzed</span>
            </div>
            <p>
              This record was collected from Reddit, but no sentiment, stance, topic, relevance, or
              reputation result has been generated.
            </p>
          </section>
        ) : null}
        {relevance ? (
          <section className="analysis-card">
            <div className="analysis-heading">
              <div>
                <span className="analysis-index">01</span>
                <h3>Relevance</h3>
              </div>
              <StatusPill status={relevance.status} />
            </div>
            {relevance.result ? (
              <>
                <strong className="signal-number">{relevance.result.confidence}% confidence</strong>
                <p>{relevance.result.rationale}</p>
                {relevance.result.matched_topics.length > 0 ? (
                  <div className="tag-row">
                    {relevance.result.matched_topics.map((topic) => (
                      <span key={topic}>{topic}</span>
                    ))}
                  </div>
                ) : null}
              </>
            ) : (
              <p>No validated result is available for this stage.</p>
            )}
          </section>
        ) : null}

        {record.analyses.enrichment ? (
          <section className="analysis-card">
            <div className="analysis-heading">
              <div>
                <span className="analysis-index">02</span>
                <h3>Enrichment</h3>
              </div>
              <StatusPill status={record.analyses.enrichment.status} />
            </div>
            {enrichment ? (
              <>
                <div className="signal-grid compact">
                  <span>
                    Sentiment <strong>{enrichment.sentiment}</strong>
                  </span>
                  <span>
                    Stance <strong>{enrichment.mining_stance}</strong>
                  </span>
                  <span>
                    Released topic <strong>{enrichment.topic_classification}</strong>
                  </span>
                </div>
                <div className="mini-concerns" aria-label="Strongest concern signals">
                  {Object.entries(enrichment.concerns)
                    .sort((left, right) => right[1] - left[1])
                    .slice(0, 4)
                    .map(([label, score]) => (
                      <div key={label}>
                        <span>{label.replaceAll('_', ' ')}</span>
                        <meter min="0" max="1" value={score}>
                          {Math.round(score * 100)}%
                        </meter>
                      </div>
                    ))}
                </div>
              </>
            ) : (
              <p>No validated result is available for this stage.</p>
            )}
          </section>
        ) : null}

        {reputation ? (
          <section className="analysis-card">
            <div className="analysis-heading">
              <div>
                <span className="analysis-index">03</span>
                <h3>Perception signals</h3>
              </div>
              <StatusPill status={reputation.status} />
            </div>
            {reputation.result ? (
              <>
                <strong className="signal-number">
                  {reputation.result.overall_reputation_score}/100
                </strong>
                <p>{reputation.result.rationale}</p>
                <div className="tag-row">
                  {reputation.result.evidence_signals.map((signal) => (
                    <span key={signal}>{signal}</span>
                  ))}
                </div>
              </>
            ) : (
              <p>No validated result is available for this stage.</p>
            )}
          </section>
        ) : null}
      </div>

      {hasAnalysis ? (
        <section className="provenance-card">
          <p className="eyebrow">Published analysis provenance</p>
          <p className="detail-source-note">
            Model, prompt, schema, and latency metadata were not included in this public release.
          </p>
          <dl>
            <div>
              <dt>Model</dt>
              <dd>{record.analyses.enrichment?.model ?? 'not recorded'}</dd>
            </div>
            <div>
              <dt>Prompt</dt>
              <dd>{record.analyses.enrichment?.prompt_version ?? 'not recorded'}</dd>
            </div>
            <div>
              <dt>Schema</dt>
              <dd>
                {record.analyses.enrichment?.schema_version === null ||
                record.analyses.enrichment?.schema_version === undefined
                  ? 'not recorded'
                  : `v${record.analyses.enrichment.schema_version}`}
              </dd>
            </div>
            <div>
              <dt>Latency</dt>
              <dd>
                {record.analyses.enrichment?.latency_ms === null ||
                record.analyses.enrichment?.latency_ms === undefined
                  ? 'not published'
                  : `${record.analyses.enrichment.latency_ms} ms`}
              </dd>
            </div>
          </dl>
        </section>
      ) : null}
    </aside>
  );
}
