import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type SyntheticEvent,
} from 'react';
import { useNavigate } from 'react-router-dom';
import { useResearch } from '../../app/research';
import {
  createLiveJobRequestSchema,
  defaultLiveRedditClient,
  deriveLiveJobId,
  generateLiveJobAccessToken,
  LiveApiError,
  liveSnapshotToResearchSnapshot,
  type LiveCapabilities,
  type LiveCredentialMode,
  type LiveJob,
  type LiveRedditClient,
  type LiveTimeFilter,
} from './api';

const activeStatuses = new Set<LiveJob['status']>(['queued', 'running', 'cancel_requested']);
const snapshotStatuses = new Set<LiveJob['status']>(['succeeded', 'partial']);
const cleanupStatuses = new Set<LiveJob['status']>(['cancelled', 'failed']);
const stageOrder = ['queued', 'searching', 'collecting', 'finalizing', 'complete'] as const;
const stageLabels = {
  queued: 'Queued',
  searching: 'Search',
  collecting: 'Collect',
  finalizing: 'Finalize',
  complete: 'Ready',
} as const;
const timeFilterLabels: Record<LiveTimeFilter, string> = {
  hour: 'Past hour',
  day: 'Past 24 hours',
  week: 'Past week',
  month: 'Past month',
  year: 'Past year',
  all: 'All available time',
};

interface LiveCollectionPanelProps {
  readonly capabilities: LiveCapabilities;
  readonly client?: LiveRedditClient;
}

interface TargetDraft {
  readonly id: number;
  readonly mineral: string;
  readonly subreddits: string;
}

type CleanupState = 'idle' | 'pending' | 'failed' | 'complete';

function splitValues(value: string): string[] {
  return [
    ...new Map(
      value
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean)
        .map((item) => [item.toLocaleLowerCase('en'), item]),
    ).values(),
  ];
}

function normalizedSubreddits(value: string): string[] {
  const normalized = splitValues(value).map((subreddit) => subreddit.replace(/^r\//iu, ''));
  return [
    ...new Map(
      normalized.map((subreddit) => [subreddit.toLocaleLowerCase('en'), subreddit]),
    ).values(),
  ];
}

function jobProgress(job: LiveJob): number {
  if (!activeStatuses.has(job.status)) return 100;
  const total = job.progress.minerals_total + job.progress.subreddits_total;
  const completed = job.progress.minerals_completed + job.progress.subreddits_completed;
  return total > 0 ? Math.min(95, Math.round((completed / total) * 100)) : 0;
}

function stageState(stage: (typeof stageOrder)[number], current: LiveJob['stage']) {
  const stageIndex = stageOrder.indexOf(stage);
  const currentIndex = stageOrder.indexOf(current);
  if (stageIndex < currentIndex) return 'complete';
  if (stageIndex === currentIndex) return current === 'complete' ? 'complete' : 'running';
  return 'waiting';
}

function errorMessage(cause: unknown): string {
  return cause instanceof LiveApiError
    ? cause.message
    : 'The live collection request could not be completed.';
}

function isAmbiguousTransportFailure(cause: unknown): boolean {
  return !(cause instanceof LiveApiError) || cause.status === null;
}

function isDefinitiveClientFailure(cause: unknown): boolean {
  return (
    cause instanceof LiveApiError &&
    cause.status !== null &&
    cause.status >= 400 &&
    cause.status < 500
  );
}

export function LiveCollectionPanel({
  capabilities,
  client = defaultLiveRedditClient,
}: LiveCollectionPanelProps) {
  const navigate = useNavigate();
  const { useSnapshot } = useResearch();
  const initialCredentialMode: LiveCredentialMode | '' = capabilities.credential_modes.includes(
    'server',
  )
    ? 'server'
    : (capabilities.credential_modes[0] ?? '');
  const [targets, setTargets] = useState<readonly TargetDraft[]>([
    { id: 1, mineral: 'copper', subreddits: 'mining, geology' },
  ]);
  const [timeFilter, setTimeFilter] = useState<LiveTimeFilter>(capabilities.defaults.time_filter);
  const [maxPosts, setMaxPosts] = useState(capabilities.defaults.max_posts_per_mineral);
  const [maxComments, setMaxComments] = useState(capabilities.defaults.max_comments_per_post);
  const [refreshSeconds, setRefreshSeconds] = useState(2);
  const [credentialMode, setCredentialMode] = useState<LiveCredentialMode | ''>(
    initialCredentialMode,
  );
  const [clientId, setClientId] = useState('');
  const [clientSecret, setClientSecret] = useState('');
  const [userAgent, setUserAgent] = useState('');
  const [creationAccessToken, setCreationAccessToken] = useState('');
  const [job, setJob] = useState<LiveJob | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [snapshotReady, setSnapshotReady] = useState(false);
  const [snapshotFailed, setSnapshotFailed] = useState(false);
  const [snapshotAttempt, setSnapshotAttempt] = useState(0);
  const [pollFailures, setPollFailures] = useState(0);
  const [cleanupState, setCleanupState] = useState<CleanupState>('idle');
  const [cleanupError, setCleanupError] = useState<string | null>(null);
  const [createAmbiguous, setCreateAmbiguous] = useState(false);
  const [resolvingAmbiguous, setResolvingAmbiguous] = useState(false);
  const [resolutionNotice, setResolutionNotice] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const accessTokenRef = useRef<string | null>(null);
  const mountedRef = useRef(true);
  const jobRef = useRef<LiveJob | null>(null);
  const nextTargetIdRef = useRef(2);
  const submissionInFlightRef = useRef(false);
  const cleanupRequestRef = useRef<{
    readonly jobId: string;
    readonly promise: ReturnType<LiveRedditClient['cancelJob']>;
  } | null>(null);
  const snapshotRequestRef = useRef<{
    readonly jobId: string;
    readonly promise: ReturnType<LiveRedditClient['getSnapshot']>;
  } | null>(null);
  const pendingSnapshotRef = useRef<ReturnType<typeof liveSnapshotToResearchSnapshot> | null>(null);

  const isActive = job ? activeStatuses.has(job.status) : false;
  const configuredTargets = useMemo(
    () =>
      targets.map((target) => ({
        mineral: target.mineral.trim(),
        subreddits: normalizedSubreddits(target.subreddits),
      })),
    [targets],
  );
  const estimatedMaxRecords =
    Number.isInteger(maxPosts) && Number.isInteger(maxComments)
      ? configuredTargets.length * maxPosts * (1 + maxComments)
      : 0;

  const updateJob = useCallback((nextJob: LiveJob | null) => {
    jobRef.current = nextJob;
    setJob(nextJob);
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      const currentJob = jobRef.current;
      const accessToken = accessTokenRef.current;
      if (currentJob && accessToken && cleanupRequestRef.current?.jobId !== currentJob.id) {
        void client.cancelJob(currentJob.id, accessToken).catch(() => undefined);
      }
    };
  }, [client]);

  const startTerminalCleanup = useCallback(
    (jobId: string, accessToken: string) => {
      if (cleanupRequestRef.current?.jobId === jobId) return;
      setCleanupState('pending');
      setCleanupError(null);
      const request = client.cancelJob(jobId, accessToken);
      cleanupRequestRef.current = { jobId, promise: request };
      void request
        .then(() => {
          if (accessTokenRef.current === accessToken) accessTokenRef.current = null;
          if (!mountedRef.current) return;
          const pendingSnapshot = pendingSnapshotRef.current;
          if (pendingSnapshot) {
            pendingSnapshotRef.current = null;
            useSnapshot(pendingSnapshot);
            setSnapshotReady(true);
          }
          setCleanupState('complete');
          setCleanupError(null);
        })
        .catch(() => {
          if (cleanupRequestRef.current?.promise === request) cleanupRequestRef.current = null;
          if (!mountedRef.current) return;
          setCleanupState('failed');
          setCleanupError(
            'Secure server cleanup did not complete. Job access is retained in memory; retry cleanup before leaving this page.',
          );
        });
    },
    [client, useSnapshot],
  );

  useEffect(() => {
    if (!job || !activeStatuses.has(job.status)) return;
    let active = true;
    const delayMs = Math.min(refreshSeconds * 1_000 * 2 ** Math.min(pollFailures, 3), 10_000);
    const timer = window.setTimeout(() => {
      const accessToken = accessTokenRef.current;
      if (!accessToken) return;
      void client
        .getJob(job.id, accessToken)
        .then((nextJob) => {
          if (!active) return;
          setPollFailures(0);
          setError(null);
          updateJob(nextJob);
        })
        .catch((cause: unknown) => {
          if (!active) return;
          setError(`${errorMessage(cause)} Progress refresh will retry automatically.`);
          setPollFailures((failures) => failures + 1);
        });
    }, delayMs);
    return () => {
      active = false;
      window.clearTimeout(timer);
    };
  }, [client, job, pollFailures, refreshSeconds, updateJob]);

  useEffect(() => {
    if (!job || !snapshotStatuses.has(job.status)) return;
    const accessToken = accessTokenRef.current;
    if (!accessToken) return;
    let active = true;
    const existingRequest = snapshotRequestRef.current;
    const request =
      existingRequest?.jobId === job.id
        ? existingRequest.promise
        : client.getSnapshot(job.id, accessToken);
    snapshotRequestRef.current = { jobId: job.id, promise: request };
    void request
      .then((snapshot) => {
        if (!active) return;
        pendingSnapshotRef.current = liveSnapshotToResearchSnapshot(snapshot);
        setSnapshotFailed(false);
        setError(null);
        startTerminalCleanup(job.id, accessToken);
      })
      .catch((cause: unknown) => {
        if (snapshotRequestRef.current?.promise === request) snapshotRequestRef.current = null;
        if (active) {
          setSnapshotFailed(true);
          setError(errorMessage(cause));
        }
      });
    return () => {
      active = false;
    };
  }, [client, job, snapshotAttempt, startTerminalCleanup]);

  useEffect(() => {
    if (!job || !cleanupStatuses.has(job.status)) return;
    const accessToken = accessTokenRef.current;
    if (!accessToken) return;
    startTerminalCleanup(job.id, accessToken);
  }, [job, startTerminalCleanup]);

  function validateConfiguration() {
    if (!credentialMode) return 'This deployment has no available Reddit credential mode.';
    if (creationAccessToken.length < 32 || creationAccessToken.length > 512) {
      return 'Enter the 32–512 character deployment access token for this live service.';
    }
    if (!configuredTargets.length) return 'Add at least one mineral target.';
    if (configuredTargets.some((target) => !target.mineral)) {
      return 'Every target needs a mineral.';
    }
    if (configuredTargets.some((target) => target.mineral.includes(','))) {
      return 'Use one mineral per target row.';
    }
    const mineralKeys = configuredTargets.map((target) => target.mineral.toLocaleLowerCase('en'));
    if (new Set(mineralKeys).size !== mineralKeys.length) {
      return 'Each mineral may appear only once per job.';
    }
    if (configuredTargets.some((target) => !target.subreddits.length)) {
      return 'Every mineral target needs at least one subreddit.';
    }
    if (configuredTargets.length > capabilities.limits.max_targets) {
      return `Use at most ${capabilities.limits.max_targets} minerals per job.`;
    }
    if (
      configuredTargets.some(
        (target) => target.subreddits.length > capabilities.limits.max_subreddits_per_target,
      )
    ) {
      return `Use at most ${capabilities.limits.max_subreddits_per_target} subreddits per mineral.`;
    }
    if (
      configuredTargets.some((target) =>
        target.subreddits.some((subreddit) => !/^[A-Za-z0-9_]{2,64}$/u.test(subreddit)),
      )
    ) {
      return 'Subreddits may contain only letters, numbers, and underscores.';
    }
    if (
      !Number.isInteger(maxPosts) ||
      maxPosts < 1 ||
      maxPosts > capabilities.limits.max_posts_per_mineral
    ) {
      return `Posts per mineral must be between 1 and ${capabilities.limits.max_posts_per_mineral}.`;
    }
    if (
      !Number.isInteger(maxComments) ||
      maxComments < 0 ||
      maxComments > capabilities.limits.max_comments_per_post
    ) {
      return `Comments per post must be between 0 and ${capabilities.limits.max_comments_per_post}.`;
    }
    if (estimatedMaxRecords > capabilities.limits.max_records_per_job) {
      return `This configuration could collect up to ${estimatedMaxRecords.toLocaleString('en')} records; reduce its targets or limits to stay within the ${capabilities.limits.max_records_per_job.toLocaleString('en')} record job budget.`;
    }
    if (credentialMode === 'provided' && (!clientId || !clientSecret || userAgent.length < 10)) {
      return 'Provide the client ID, client secret, and a descriptive user agent.';
    }
    return null;
  }

  function updateTarget(id: number, field: 'mineral' | 'subreddits', value: string) {
    setTargets((current) =>
      current.map((target) => (target.id === id ? { ...target, [field]: value } : target)),
    );
  }

  function addTarget() {
    if (targets.length >= capabilities.limits.max_targets) return;
    const id = nextTargetIdRef.current;
    nextTargetIdRef.current += 1;
    setTargets((current) => [...current, { id, mineral: '', subreddits: '' }]);
  }

  function removeTarget(id: number) {
    if (targets.length === 1) return;
    setTargets((current) => current.filter((target) => target.id !== id));
  }

  async function submitJob(event: SyntheticEvent<HTMLFormElement>) {
    event.preventDefault();
    if (submissionInFlightRef.current || job) return;
    const validationError = validateConfiguration();
    if (validationError) {
      setError(validationError);
      return;
    }
    const parsedRequest = createLiveJobRequestSchema.safeParse({
      targets: configuredTargets,
      time_filter: timeFilter,
      max_posts_per_mineral: maxPosts,
      max_comments_per_post: maxComments,
      credential_mode: credentialMode,
      ...(credentialMode === 'provided'
        ? {
            credentials: {
              client_id: clientId,
              client_secret: clientSecret,
              user_agent: userAgent,
            },
          }
        : {}),
    });
    if (!parsedRequest.success) {
      setError('Check the collection configuration and credential lengths, then try again.');
      return;
    }
    let jobAccessToken: string;
    try {
      jobAccessToken = generateLiveJobAccessToken();
    } catch {
      setError('Secure browser randomness is unavailable; a live job cannot be started safely.');
      return;
    }
    submissionInFlightRef.current = true;
    const submittedCreationAccessToken = creationAccessToken;
    accessTokenRef.current = jobAccessToken;
    setSubmitting(true);
    setError(null);
    setSnapshotReady(false);
    setSnapshotFailed(false);
    setCreateAmbiguous(false);
    setResolutionNotice(null);
    pendingSnapshotRef.current = null;
    setClientId('');
    setClientSecret('');
    setUserAgent('');
    setCreationAccessToken('');
    try {
      let created: Awaited<ReturnType<LiveRedditClient['createJob']>>;
      try {
        created = await client.createJob(
          parsedRequest.data,
          submittedCreationAccessToken,
          jobAccessToken,
        );
      } catch (firstFailure) {
        if (!isAmbiguousTransportFailure(firstFailure)) throw firstFailure;
        let derivedJobId: string | null = null;
        try {
          derivedJobId = await deriveLiveJobId(jobAccessToken);
        } catch {
          // Fall back to the same-token idempotent POST if Web Crypto digest is unavailable.
        }
        if (derivedJobId) {
          try {
            const recoveredJob = await client.getJob(derivedJobId, jobAccessToken);
            if (recoveredJob.id !== derivedJobId) {
              throw new LiveApiError('The live collection service returned an invalid response.');
            }
            created = { job: recoveredJob, access_token: jobAccessToken };
          } catch (recoveryFailure) {
            if (!(recoveryFailure instanceof LiveApiError) || recoveryFailure.status !== 404) {
              throw recoveryFailure;
            }
            created = await client.createJob(
              parsedRequest.data,
              submittedCreationAccessToken,
              jobAccessToken,
            );
          }
        } else {
          created = await client.createJob(
            parsedRequest.data,
            submittedCreationAccessToken,
            jobAccessToken,
          );
        }
      }
      if (created.access_token !== jobAccessToken) {
        let cleaned = false;
        try {
          await client.cancelJob(created.job.id, jobAccessToken);
          cleaned = true;
        } catch {
          // The requested token is retained below so an uncertain job is never silently forgotten.
        }
        if (cleaned && accessTokenRef.current === jobAccessToken) {
          accessTokenRef.current = null;
        }
        throw new LiveApiError(
          'The live service returned an invalid job token.',
          cleaned ? 409 : null,
        );
      }
      if (!mountedRef.current) {
        void client
          .cancelJob(created.job.id, jobAccessToken)
          .then(() => {
            if (accessTokenRef.current === jobAccessToken) accessTokenRef.current = null;
          })
          .catch(() => undefined);
        return;
      }
      setPollFailures(0);
      setCleanupState('idle');
      setCleanupError(null);
      updateJob(created.job);
    } catch (cause) {
      if (isDefinitiveClientFailure(cause) && accessTokenRef.current === jobAccessToken) {
        accessTokenRef.current = null;
      }
      if (mountedRef.current) {
        if (isDefinitiveClientFailure(cause)) {
          setError(errorMessage(cause));
        } else {
          setCreateAmbiguous(true);
          setError(
            'Job creation remains uncertain after an authenticated recovery attempt. The job token is retained in this page, and a different job cannot be started safely.',
          );
        }
      }
    } finally {
      submissionInFlightRef.current = false;
      if (mountedRef.current) setSubmitting(false);
    }
  }

  async function resolveUncertainJob() {
    const jobAccessToken = accessTokenRef.current;
    if (!createAmbiguous || resolvingAmbiguous || !jobAccessToken) return;
    setResolvingAmbiguous(true);
    setError(null);
    setResolutionNotice(null);
    try {
      const derivedJobId = await deriveLiveJobId(jobAccessToken);
      const recoveredJob = await client.getJob(derivedJobId, jobAccessToken);
      if (recoveredJob.id !== derivedJobId) {
        throw new LiveApiError('The live collection service returned an invalid response.');
      }
      if (!mountedRef.current) {
        void client
          .cancelJob(recoveredJob.id, jobAccessToken)
          .then(() => {
            if (accessTokenRef.current === jobAccessToken) accessTokenRef.current = null;
          })
          .catch(() => undefined);
        return;
      }
      setPollFailures(0);
      setSnapshotReady(false);
      setSnapshotFailed(false);
      setCleanupState('idle');
      setCleanupError(null);
      setCreateAmbiguous(false);
      updateJob(recoveredJob);
    } catch (cause) {
      if (cause instanceof LiveApiError && cause.status === 404) {
        if (accessTokenRef.current === jobAccessToken) accessTokenRef.current = null;
        if (mountedRef.current) {
          setCreateAmbiguous(false);
          setResolutionNotice(
            'No retained job was found. The form is unlocked and a new collection can be started safely.',
          );
        }
      } else if (mountedRef.current) {
        setError(
          'The uncertain job could not be resolved yet. Its access token remains in this page; retry when the service is reachable.',
        );
      }
    } finally {
      if (mountedRef.current) setResolvingAmbiguous(false);
    }
  }

  async function cancelJob() {
    if (!job || !accessTokenRef.current || !activeStatuses.has(job.status)) return;
    setCancelling(true);
    setError(null);
    try {
      const nextJob = await client.cancelJob(job.id, accessTokenRef.current);
      if (mountedRef.current) {
        updateJob(nextJob);
        setPollFailures(0);
      }
    } catch (cause) {
      if (mountedRef.current) setError(errorMessage(cause));
    } finally {
      if (mountedRef.current) setCancelling(false);
    }
  }

  function retryTerminalCleanup() {
    const accessToken = accessTokenRef.current;
    if (!job || !accessToken || cleanupState === 'pending') return;
    startTerminalCleanup(job.id, accessToken);
  }

  function resetJob() {
    accessTokenRef.current = null;
    snapshotRequestRef.current = null;
    cleanupRequestRef.current = null;
    pendingSnapshotRef.current = null;
    updateJob(null);
    setSnapshotReady(false);
    setSnapshotFailed(false);
    setSnapshotAttempt(0);
    setPollFailures(0);
    setCleanupState('idle');
    setCleanupError(null);
    setCreateAmbiguous(false);
    setResolvingAmbiguous(false);
    setResolutionNotice(null);
    setError(null);
  }

  return (
    <section className="live-collection panel" aria-labelledby="live-collection-title">
      <div className="live-collection-heading">
        <div>
          <p className="eyebrow">Opt-in provider operation</p>
          <h2 id="live-collection-title">Collect a bounded Reddit snapshot.</h2>
          <p>
            Search Reddit through {capabilities.library}, persist only this isolated job, and
            inspect the raw result without inventing analysis.
          </p>
        </div>
        <span
          className={
            capabilities.credential_modes.length
              ? 'provider-badge'
              : 'provider-badge provider-unavailable'
          }
        >
          <i aria-hidden="true" />
          {capabilities.credential_modes.length ? 'Reddit API available' : 'Credentials required'}
        </span>
      </div>

      <div className="live-collection-grid">
        <form className="live-form" onSubmit={(event) => void submitJob(event)}>
          <fieldset disabled={submitting || job !== null || createAmbiguous}>
            <legend className="sr-only">Live collection configuration</legend>
            <div className="live-form-section">
              <div className="live-section-label">
                <span>01</span>
                <div>
                  <strong>Research scope</strong>
                  <small>Map each mineral query to its own comma-separated communities.</small>
                </div>
              </div>
              <div className="target-list">
                {targets.map((target, index) => (
                  <div className="target-row" key={target.id}>
                    <span className="target-number" aria-hidden="true">
                      {String(index + 1).padStart(2, '0')}
                    </span>
                    <label>
                      Mineral {index + 1}
                      <input
                        type="text"
                        value={target.mineral}
                        placeholder="copper"
                        onChange={(event) => updateTarget(target.id, 'mineral', event.target.value)}
                      />
                    </label>
                    <label>
                      Subreddits {index + 1}
                      <input
                        type="text"
                        value={target.subreddits}
                        placeholder="mining, geology"
                        onChange={(event) =>
                          updateTarget(target.id, 'subreddits', event.target.value)
                        }
                      />
                    </label>
                    <button
                      className="target-remove"
                      type="button"
                      disabled={targets.length === 1}
                      onClick={() => removeTarget(target.id)}
                      aria-label={`Remove mineral target ${index + 1}`}
                    >
                      ×
                    </button>
                  </div>
                ))}
                <button
                  className="target-add"
                  type="button"
                  disabled={targets.length >= capabilities.limits.max_targets}
                  onClick={addTarget}
                >
                  <span aria-hidden="true">+</span> Add mineral target
                </button>
              </div>
              <label className="target-time-filter">
                Reddit time window
                <select
                  value={timeFilter}
                  onChange={(event) => setTimeFilter(event.target.value as LiveTimeFilter)}
                >
                  {capabilities.time_filters.map((filter) => (
                    <option key={filter} value={filter}>
                      {timeFilterLabels[filter]}
                    </option>
                  ))}
                </select>
              </label>
            </div>

            <div className="live-form-section live-bounds-grid">
              <div className="live-section-label">
                <span>02</span>
                <div>
                  <strong>Safety bounds</strong>
                  <small>Server-enforced caps keep every collection finite.</small>
                </div>
              </div>
              <label>
                Posts per mineral
                <input
                  type="number"
                  min="1"
                  max={capabilities.limits.max_posts_per_mineral}
                  value={maxPosts}
                  onChange={(event) => setMaxPosts(event.currentTarget.valueAsNumber)}
                />
              </label>
              <label>
                Comments per post
                <input
                  type="number"
                  min="0"
                  max={capabilities.limits.max_comments_per_post}
                  value={maxComments}
                  onChange={(event) => setMaxComments(event.currentTarget.valueAsNumber)}
                />
              </label>
              <label>
                Progress refresh
                <select
                  value={refreshSeconds}
                  onChange={(event) => setRefreshSeconds(Number(event.target.value))}
                >
                  <option value="1">Every second</option>
                  <option value="2">Every 2 seconds</option>
                  <option value="5">Every 5 seconds</option>
                  <option value="10">Every 10 seconds</option>
                </select>
              </label>
              <div className="record-budget" role="status">
                <div>
                  <span>Worst-case record budget</span>
                  <strong>
                    {estimatedMaxRecords.toLocaleString('en')} /{' '}
                    {capabilities.limits.max_records_per_job.toLocaleString('en')}
                  </strong>
                </div>
                <meter
                  min="0"
                  max={capabilities.limits.max_records_per_job}
                  value={Math.min(estimatedMaxRecords, capabilities.limits.max_records_per_job)}
                >
                  {estimatedMaxRecords} estimated records
                </meter>
                <small>
                  Targets × posts × (one post plus its comment limit) · up to{' '}
                  {capabilities.limits.max_active_jobs} active jobs per deployment
                </small>
              </div>
            </div>

            <div className="live-form-section">
              <div className="live-section-label">
                <span>03</span>
                <div>
                  <strong>Reddit application</strong>
                  <small>
                    Credentials are used for this job only and never appear in its status.
                  </small>
                </div>
              </div>
              <label className="deployment-access-token">
                Deployment access token
                <input
                  aria-label="Deployment access token"
                  type="password"
                  value={creationAccessToken}
                  minLength={32}
                  maxLength={512}
                  autoComplete="off"
                  spellCheck={false}
                  onChange={(event) => setCreationAccessToken(event.target.value)}
                />
                <small>Authorizes one job submission; it is not the Reddit client secret.</small>
              </label>
              <label>
                Credential mode
                <select
                  value={credentialMode}
                  onChange={(event) => setCredentialMode(event.target.value as LiveCredentialMode)}
                >
                  {!capabilities.credential_modes.length ? (
                    <option value="">No credential mode available</option>
                  ) : null}
                  {capabilities.credential_modes.map((mode) => (
                    <option key={mode} value={mode}>
                      {mode === 'server'
                        ? 'Use server credentials'
                        : 'Use credentials for this run'}
                    </option>
                  ))}
                </select>
              </label>
              {credentialMode === 'provided' ? (
                <div className="credential-grid">
                  <label>
                    Client ID
                    <input
                      type="password"
                      value={clientId}
                      autoComplete="off"
                      spellCheck={false}
                      onChange={(event) => setClientId(event.target.value)}
                    />
                  </label>
                  <label>
                    Client secret
                    <input
                      type="password"
                      value={clientSecret}
                      autoComplete="new-password"
                      spellCheck={false}
                      onChange={(event) => setClientSecret(event.target.value)}
                    />
                  </label>
                  <label className="credential-user-agent">
                    User agent
                    <input
                      type="text"
                      value={userAgent}
                      autoComplete="off"
                      spellCheck={false}
                      placeholder="script:minerallens:0.2.0 (by u/username)"
                      onChange={(event) => setUserAgent(event.target.value)}
                    />
                  </label>
                </div>
              ) : credentialMode === 'server' ? (
                <p className="credential-note">
                  This deployment has a read-only Reddit application configured on the server.
                </p>
              ) : (
                <p className="credential-note unavailable">
                  Live collection is enabled, but this deployment has no usable Reddit credentials.
                </p>
              )}
              <p className="security-note">
                <span aria-hidden="true">◇</span>
                The deployment token and any provided Reddit credentials stay in this component
                only, are cleared when submission begins, and are never placed in browser storage or
                a URL.
              </p>
            </div>
          </fieldset>

          {error ? (
            <p className="live-error" role="alert">
              {error}
            </p>
          ) : null}
          {resolutionNotice ? (
            <p className="live-notice" role="status">
              <span aria-hidden="true">✓</span>
              {resolutionNotice}
            </p>
          ) : null}
          {createAmbiguous ? (
            <div className="uncertain-job-state" role="group" aria-labelledby="uncertain-job-title">
              <div>
                <strong id="uncertain-job-title">Submission status uncertain</strong>
                <p>
                  Resolve the deterministic job handle before starting another collection. No
                  credential values are requested again.
                </p>
              </div>
              <button
                className="button secondary"
                type="button"
                disabled={resolvingAmbiguous}
                onClick={() => void resolveUncertainJob()}
              >
                {resolvingAmbiguous ? 'Resolving…' : 'Resolve uncertain job'}
              </button>
            </div>
          ) : null}
          <div className="live-actions">
            <button
              className="button primary"
              type="submit"
              disabled={
                submitting ||
                job !== null ||
                createAmbiguous ||
                !capabilities.credential_modes.length
              }
            >
              {submitting ? 'Starting securely…' : 'Start live collection'}
            </button>
            {isActive ? (
              <button
                className="button ghost"
                type="button"
                disabled={cancelling || job?.status === 'cancel_requested'}
                onClick={() => void cancelJob()}
              >
                {cancelling || job?.status === 'cancel_requested'
                  ? 'Cancellation requested'
                  : 'Cancel collection'}
              </button>
            ) : null}
            {job && !isActive && cleanupState === 'complete' ? (
              <button className="button ghost" type="button" onClick={resetJob}>
                Configure another run
              </button>
            ) : null}
          </div>
        </form>

        <aside className="live-monitor" aria-label="Live collection progress">
          <div className="console-topline">
            <span>
              <i aria-hidden="true" /> live-reddit / raw-records
            </span>
            <span>{job?.status.replace('_', ' ') ?? 'READY'}</span>
          </div>
          {job ? (
            <div className="job-state" aria-live="polite" aria-atomic="true">
              <div
                className="job-progress-ring"
                style={{ '--progress': jobProgress(job) } as CSSProperties}
              >
                <strong>{jobProgress(job)}%</strong>
                <span>{job.stage}</span>
              </div>
              <div>
                <p className="eyebrow">Job {job.id.slice(0, 8)}</p>
                <h3>{job.message}</h3>
                <span className={`live-job-status status-${job.status}`}>
                  {job.status.replace('_', ' ')}
                </span>
              </div>
            </div>
          ) : (
            <div className="live-ready-state">
              <span aria-hidden="true">⌁</span>
              <h3>Ready for an explicit request</h3>
              <p>
                Nothing contacts Reddit until you submit this form. Results remain isolated to the
                authenticated job.
              </p>
            </div>
          )}

          <ol className="live-stage-rail" aria-label="Collection stages">
            {stageOrder.map((stage, index) => {
              const state = job ? stageState(stage, job.stage) : 'waiting';
              return (
                <li key={stage} className={`live-stage-${state}`}>
                  <span aria-hidden="true">{state === 'complete' ? '✓' : index + 1}</span>
                  <strong>{stageLabels[stage]}</strong>
                </li>
              );
            })}
          </ol>

          <div className="live-metrics">
            <div>
              <span>Posts stored</span>
              <strong>{job?.progress.posts_stored ?? 0}</strong>
            </div>
            <div>
              <span>Comments stored</span>
              <strong>{job?.progress.comments_stored ?? 0}</strong>
            </div>
            <div>
              <span>Records ready</span>
              <strong>{job?.record_count ?? 0}</strong>
            </div>
            <div>
              <span>Failed operations</span>
              <strong>
                {(job?.progress.posts_failed ?? 0) + (job?.progress.searches_failed ?? 0)}
              </strong>
            </div>
          </div>

          {job?.error ? (
            <p className="job-error" role="alert">
              <strong>{job.error.code}</strong> {job.error.message}
            </p>
          ) : null}
          {snapshotFailed ? (
            <button
              className="button secondary snapshot-retry"
              type="button"
              onClick={() => {
                setError(null);
                setSnapshotFailed(false);
                setSnapshotAttempt((attempt) => attempt + 1);
              }}
            >
              Retry result handoff
            </button>
          ) : null}
          {cleanupState === 'pending' ? (
            <p className="cleanup-state cleanup-pending" role="status">
              <span aria-hidden="true">◇</span>
              Removing server-side job metadata and raw SQLite artifacts before handoff…
            </p>
          ) : null}
          {cleanupState === 'failed' && cleanupError ? (
            <div className="cleanup-state cleanup-failed" role="alert">
              <div>
                <strong>Secure cleanup needs attention</strong>
                <p>{cleanupError}</p>
              </div>
              <button className="button secondary" type="button" onClick={retryTerminalCleanup}>
                Retry secure cleanup
              </button>
            </div>
          ) : null}
          {cleanupState === 'complete' && !snapshotReady ? (
            <p className="cleanup-state cleanup-complete" role="status">
              <span aria-hidden="true">✓</span>
              Server-side job data removed; local safe metadata remains visible.
            </p>
          ) : null}
          {snapshotReady ? (
            <div className="snapshot-ready">
              <span aria-hidden="true">✓</span>
              <div>
                <strong>{job?.record_count ?? 0} raw records ready</strong>
                <p>No sentiment, stance, topic, relevance, or reputation fields were inferred.</p>
              </div>
              {cleanupState === 'complete' ? (
                <button
                  className="button primary"
                  type="button"
                  onClick={() => navigate('/explorer')}
                >
                  Open in Explorer <span aria-hidden="true">→</span>
                </button>
              ) : (
                <span className="handoff-waiting">Explorer unlocks after secure cleanup.</span>
              )}
            </div>
          ) : null}
        </aside>
      </div>
    </section>
  );
}
