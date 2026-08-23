import { useEffect, useMemo, useState } from 'react';
import { useResearch } from '../../app/research';
import { ErrorState, LoadingState } from '../../components/LoadingState';
import { PageHeader } from '../../components/PageHeader';
import { buildReplayEvents, type ReplayScenario, type ReplayStage } from './replay';

const stageLabels: Record<ReplayStage, string> = {
  collect: 'Collect',
  relevance: 'Relevance',
  enrichment: 'Enrichment',
  reputation: 'Perception',
  export: 'Export',
};

function formatRunDate(value: string): string {
  return new Intl.DateTimeFormat('en', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    timeZone: 'UTC',
  }).format(new Date(value));
}

export function PipelinePage() {
  const { snapshot, loading, error } = useResearch();
  const [scenario, setScenario] = useState<ReplayScenario>('nominal');
  const [cursor, setCursor] = useState(-1);
  const [running, setRunning] = useState(false);
  const events = useMemo(() => buildReplayEvents(scenario), [scenario]);

  useEffect(() => {
    if (!running) return;
    const timer = window.setTimeout(() => {
      setCursor((current) => {
        if (current >= events.length - 1) {
          setRunning(false);
          return current;
        }
        return current + 1;
      });
    }, 620);
    return () => window.clearTimeout(timer);
  }, [running, cursor, events.length]);

  if (loading) return <LoadingState />;
  if (error || !snapshot)
    return <ErrorState message={error ?? 'No research snapshot was loaded.'} />;

  const visibleEvents = cursor >= 0 ? events.slice(0, cursor + 1) : [];
  const latestEvent = visibleEvents.at(-1);
  const states = (Object.keys(stageLabels) as ReplayStage[]).map((stage) => {
    const event = visibleEvents.filter((item) => item.stage === stage).at(-1);
    return { stage, state: event?.state ?? 'waiting' };
  });

  function startReplay() {
    setCursor(0);
    setRunning(true);
  }

  function resetReplay() {
    setRunning(false);
    setCursor(-1);
  }

  return (
    <div className="page pipeline-page">
      <PageHeader
        eyebrow="Deterministic execution replay"
        title="Reliability is visible in the state transitions."
        description="Replay a synthetic run through the same stages represented by the CLI. Scenarios illustrate bounded retry and optimistic stale-result rejection; they never contact providers."
      />

      <section className="pipeline-lab">
        <div className="lab-controls">
          <div>
            <p className="eyebrow">Replay configuration</p>
            <h2>Choose an execution path</h2>
            <p>
              The event sequence is fixed and repeatable. It visualizes service behavior rather than
              pretending to run the provider SDKs in a browser.
            </p>
          </div>
          <label>
            Scenario
            <select
              value={scenario}
              disabled={running}
              onChange={(event) => {
                setScenario(event.target.value as ReplayScenario);
                resetReplay();
              }}
            >
              <option value="nominal">Nominal run</option>
              <option value="retry">Transient provider retry</option>
              <option value="stale">Stale result rejection</option>
            </select>
          </label>
          <div className="replay-actions">
            <button
              className="button primary"
              type="button"
              onClick={startReplay}
              disabled={running}
            >
              {cursor >= 0 ? 'Replay again' : 'Start replay'}
            </button>
            <button
              className="button ghost"
              type="button"
              onClick={resetReplay}
              disabled={cursor < 0}
            >
              Reset
            </button>
          </div>
        </div>

        <div className="replay-console">
          <div className="console-topline">
            <span>
              <i aria-hidden="true" /> offline-synthetic / schema-v3
            </span>
            <span>{running ? 'RUNNING' : cursor >= events.length - 1 ? 'COMPLETE' : 'READY'}</span>
          </div>
          <ol className="replay-stage-rail" aria-label="Replay stages">
            {states.map(({ stage, state }, index) => (
              <li key={stage} className={`replay-${state}`}>
                <span className="replay-node" aria-hidden="true">
                  {state === 'complete' ? '✓' : index + 1}
                </span>
                <strong>{stageLabels[stage]}</strong>
                <span>{state}</span>
              </li>
            ))}
          </ol>
          <div className="event-terminal" aria-live="polite" aria-atomic="true">
            {latestEvent ? (
              <>
                <span className="terminal-prompt">{stageLabels[latestEvent.stage]}</span>
                <strong>{latestEvent.state}</strong>
                <p>{latestEvent.message}</p>
              </>
            ) : (
              <p>Select a scenario and start the replay.</p>
            )}
          </div>
          <ol className="event-log" aria-label="Completed replay events">
            {visibleEvents.map((event, index) => (
              <li key={`${event.stage}-${event.state}-${index}`}>
                <span>{String(index + 1).padStart(2, '0')}</span>
                <strong>{stageLabels[event.stage]}</strong>
                <span>{event.message}</span>
              </li>
            ))}
          </ol>
        </div>
      </section>

      <section className="run-history panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Recorded run metadata</p>
            <h2>
              {snapshot.provenance.synthetic
                ? 'Synthetic run fixtures'
                : 'No fabricated run history'}
            </h2>
          </div>
          <span className="chart-key">UTC · deterministic fixture</span>
        </div>
        {snapshot.runs.length ? (
          <div className="run-list">
            {snapshot.runs.map((run) => {
              const completed = run.stages.filter((stage) => stage.status === 'complete').length;
              const duration = run.stages.reduce((total, stage) => total + stage.duration_ms, 0);
              return (
                <article key={run.id}>
                  <div className="run-identity">
                    <span className="mineral-chip">{run.mineral}</span>
                    <div>
                      <strong>{run.id}</strong>
                      <span>{formatRunDate(run.started_at)}</span>
                    </div>
                  </div>
                  <div className="run-progress" aria-label={`${completed} of 5 stages complete`}>
                    <span style={{ width: `${(completed / 5) * 100}%` }} />
                  </div>
                  <span className={`run-status run-${run.status}`}>{run.status}</span>
                  <strong>{duration} ms</strong>
                </article>
              );
            })}
          </div>
        ) : (
          <div className="empty-state compact">
            <h3>Run metadata is not part of the public dataset</h3>
            <p>
              The replay above remains explicitly synthetic and demonstrates system behavior without
              claiming historical execution evidence.
            </p>
          </div>
        )}
      </section>

      <section className="reliability-grid">
        <article>
          <span className="decision-number">01</span>
          <h3>Idempotent work</h3>
          <p>Stable IDs and explicit work states make safe resume behavior observable.</p>
        </article>
        <article>
          <span className="decision-number">02</span>
          <h3>Bounded recovery</h3>
          <p>Only classified transient failures receive capped backoff and retry.</p>
        </article>
        <article>
          <span className="decision-number">03</span>
          <h3>Revision-safe writes</h3>
          <p>Late model results are rejected when their input or dependency changed.</p>
        </article>
        <article>
          <span className="decision-number">04</span>
          <h3>Atomic publication</h3>
          <p>Exports are built from one snapshot and published without partial files.</p>
        </article>
      </section>
    </div>
  );
}
