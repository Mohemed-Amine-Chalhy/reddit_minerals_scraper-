import { useMemo, useState, type ChangeEvent } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useResearch } from '../../app/research';
import { ErrorState, LoadingState } from '../../components/LoadingState';
import { PageHeader } from '../../components/PageHeader';
import { RecordDetail } from '../../components/RecordDetail';
import { StatusPill } from '../../components/StatusPill';
import { ImportValidationError } from '../../domain/importer';
import type { AnalysisStatus, ResearchRecord } from '../../domain/schemas';
import {
  availableMinerals,
  availableTopics,
  enrichmentFor,
  filterRecords,
  recordTitle,
  type ExplorerFilters,
} from '../../domain/selectors';

type SortOrder = 'recent' | 'score' | 'mineral';

function filtersFromParams(params: URLSearchParams): ExplorerFilters {
  const kind = params.get('recordType');
  const sentiment = params.get('sentiment');
  const status = params.get('status');
  return {
    query: params.get('query') ?? '',
    mineral: params.get('mineral') ?? '',
    recordType: kind === 'post' || kind === 'comment' ? kind : '',
    sentiment:
      sentiment === 'positive' ||
      sentiment === 'negative' ||
      sentiment === 'neutral' ||
      sentiment === 'mixed'
        ? sentiment
        : '',
    status: status ? (status as AnalysisStatus) : '',
    topic: params.get('topic') ?? '',
    from: params.get('from') ?? '',
    to: params.get('to') ?? '',
  };
}

function sortRecords(records: readonly ResearchRecord[], order: SortOrder): ResearchRecord[] {
  return [...records].sort((left, right) => {
    if (order === 'score') return right.content.score - left.content.score;
    if (order === 'mineral') return left.mineral.localeCompare(right.mineral);
    return Date.parse(right.content.created_at) - Date.parse(left.content.created_at);
  });
}

function shortDate(value: string): string {
  return new Intl.DateTimeFormat('en', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    timeZone: 'UTC',
  }).format(new Date(value));
}

export function ExplorerPage() {
  const { snapshot, loading, error, importFile, resetDataset } = useResearch();
  const [searchParams, setSearchParams] = useSearchParams();
  const [selected, setSelected] = useState<ResearchRecord | null>(null);
  const [importMessage, setImportMessage] = useState<string | null>(null);
  const filters = filtersFromParams(searchParams);
  const sort = (searchParams.get('sort') as SortOrder | null) ?? 'recent';

  const records = useMemo(
    () => (snapshot ? sortRecords(filterRecords(snapshot.records, filters), sort) : []),
    [snapshot, filters, sort],
  );

  if (loading) return <LoadingState />;
  if (error || !snapshot)
    return <ErrorState message={error ?? 'No research snapshot was loaded.'} />;

  const isLiveSnapshot = snapshot.delivery === 'live';

  function updateParam(key: keyof ExplorerFilters | 'sort', value: string) {
    const next = new URLSearchParams(searchParams);
    if (value) next.set(key, value);
    else next.delete(key);
    setSearchParams(next, { replace: true });
  }

  async function handleImport(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (!file) return;
    try {
      await importFile(file);
      setSelected(null);
      setSearchParams({}, { replace: true });
      setImportMessage(`${file.name} loaded locally.`);
    } catch (cause) {
      setImportMessage(
        cause instanceof ImportValidationError
          ? cause.message
          : 'The selected file could not be read.',
      );
    } finally {
      event.target.value = '';
    }
  }

  return (
    <div className="page explorer-page">
      <PageHeader
        eyebrow="Schema-aware content explorer"
        title="Follow every signal back to its source record."
        description={
          isLiveSnapshot
            ? 'Inspect the raw records collected by your live Reddit job. Content is available for this in-memory session, while analysis fields remain empty until a separate validated analysis pipeline is run.'
            : 'Search a deterministic sample of publicly released research metadata or inspect a compatible JSON/JSONL export locally. Filters remain in the URL, so a view can be shared or revisited.'
        }
        actions={
          <>
            <label className="button primary file-button">
              Inspect local export
              <input
                type="file"
                accept=".json,.jsonl,application/json,application/x-ndjson"
                onChange={(event) => void handleImport(event)}
              />
            </label>
            {snapshot.delivery === 'local' || snapshot.delivery === 'live' ? (
              <button
                className="button secondary"
                type="button"
                onClick={() => {
                  setSelected(null);
                  setSearchParams({}, { replace: true });
                  void resetDataset();
                }}
              >
                Restore research sample
              </button>
            ) : null}
          </>
        }
      />

      <div className={`source-banner source-${snapshot.delivery}`} role="status">
        <span className="source-icon" aria-hidden="true">
          {snapshot.delivery === 'local'
            ? '↥'
            : snapshot.delivery === 'api'
              ? '⌁'
              : snapshot.delivery === 'live'
                ? '●'
                : '◆'}
        </span>
        <div>
          <strong>
            {snapshot.delivery === 'local'
              ? 'Local browser preview'
              : snapshot.delivery === 'live'
                ? 'Live Reddit · raw collection'
                : snapshot.provenance.publicSample
                  ? snapshot.delivery === 'api'
                    ? 'Public research sample · read-only API'
                    : 'Bundled public Kaggle research sample'
                  : 'Synthetic regression fixture'}
          </strong>
          <span>
            {snapshot.notice ?? `${snapshot.records.length} deterministic export records`}
          </span>
          {snapshot.provenance.sourceUrl ? (
            <a href={snapshot.provenance.sourceUrl} target="_blank" rel="noreferrer">
              View the published dataset <span aria-hidden="true">↗</span>
            </a>
          ) : null}
        </div>
      </div>
      {importMessage ? (
        <p className="import-message" role="alert">
          {importMessage}
        </p>
      ) : null}

      <section className="filter-panel" aria-label="Explorer filters">
        <label className="search-control">
          <span>Search records</span>
          <input
            type="search"
            value={filters.query}
            placeholder="Mineral, subreddit, topic…"
            onChange={(event) => updateParam('query', event.target.value)}
          />
        </label>
        <label>
          Mineral
          <select
            value={filters.mineral}
            onChange={(event) => updateParam('mineral', event.target.value)}
          >
            <option value="">All</option>
            {availableMinerals(snapshot.records).map((mineral) => (
              <option key={mineral}>{mineral}</option>
            ))}
          </select>
        </label>
        <label>
          Type
          <select
            value={filters.recordType}
            onChange={(event) => updateParam('recordType', event.target.value)}
          >
            <option value="">All</option>
            <option value="post">Posts</option>
            <option value="comment">Comments</option>
          </select>
        </label>
        <label>
          Sentiment
          <select
            value={filters.sentiment}
            onChange={(event) => updateParam('sentiment', event.target.value)}
          >
            <option value="">All</option>
            <option value="positive">Positive</option>
            <option value="mixed">Mixed</option>
            <option value="neutral">Neutral</option>
            <option value="negative">Negative</option>
          </select>
        </label>
        <label>
          Analysis state
          <select
            value={filters.status}
            onChange={(event) => updateParam('status', event.target.value)}
          >
            <option value="">All</option>
            <option value="complete">Complete</option>
            <option value="retryable_failure">Retryable</option>
            <option value="blocked">Blocked</option>
          </select>
        </label>
        <label>
          Topic
          <select
            value={filters.topic}
            onChange={(event) => updateParam('topic', event.target.value)}
          >
            <option value="">All</option>
            {availableTopics(snapshot.records).map((topic) => (
              <option key={topic}>{topic}</option>
            ))}
          </select>
        </label>
        <label>
          From
          <input
            type="date"
            value={filters.from}
            onChange={(event) => updateParam('from', event.target.value)}
          />
        </label>
        <label>
          To
          <input
            type="date"
            value={filters.to}
            onChange={(event) => updateParam('to', event.target.value)}
          />
        </label>
        <label>
          Sort
          <select value={sort} onChange={(event) => updateParam('sort', event.target.value)}>
            <option value="recent">Most recent</option>
            <option value="score">Highest score</option>
            <option value="mineral">Mineral A–Z</option>
          </select>
        </label>
        <button
          className="clear-filters"
          type="button"
          onClick={() => setSearchParams({}, { replace: true })}
          disabled={searchParams.size === 0}
        >
          Clear filters
        </button>
      </section>

      <div className={selected ? 'explorer-layout with-detail' : 'explorer-layout'}>
        <section className="results-panel" aria-labelledby="results-title">
          <div className="results-heading">
            <div>
              <p className="eyebrow">Current selection</p>
              <h2 id="results-title">{records.length} records</h2>
            </div>
            <span>Export schema v1</span>
          </div>
          {records.length ? (
            <div className="table-scroll">
              <table className="records-table">
                <thead>
                  <tr>
                    <th scope="col">Record</th>
                    <th scope="col">Mineral</th>
                    <th scope="col">Topic</th>
                    <th scope="col">Signal</th>
                    <th scope="col">Date</th>
                    <th scope="col">
                      <span className="sr-only">Action</span>
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {records.map((record) => {
                    const enrichment = enrichmentFor(record);
                    return (
                      <tr key={`${record.mineral}-${record.content.id}`}>
                        <td data-label="Record">
                          <span className="record-kind">{record.record_type}</span>
                          <strong>{recordTitle(record)}</strong>
                          <small>r/{record.content.subreddit}</small>
                        </td>
                        <td data-label="Mineral">
                          <span className="mineral-chip">{record.mineral}</span>
                        </td>
                        <td data-label="Topic">{enrichment?.topic_classification ?? '—'}</td>
                        <td data-label="Signal">
                          {Object.keys(record.analyses).length ? (
                            <StatusPill status={record.analyses.enrichment?.status ?? 'pending'} />
                          ) : (
                            <span className="raw-status">Not analyzed</span>
                          )}
                        </td>
                        <td data-label="Date">{shortDate(record.content.created_at)}</td>
                        <td>
                          <button
                            className="inspect-button"
                            type="button"
                            onClick={() => setSelected(record)}
                            aria-label={`Inspect ${recordTitle(record)}`}
                          >
                            Inspect <span aria-hidden="true">→</span>
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="empty-state">
              <span aria-hidden="true">◇</span>
              <h3>No records match this view</h3>
              <p>Clear one or more filters to widen the selection.</p>
            </div>
          )}
        </section>
        {selected ? <RecordDetail record={selected} onClose={() => setSelected(null)} /> : null}
      </div>
    </div>
  );
}
