import { StrictMode } from 'react';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ResearchProvider, useResearch } from '../../app/research';
import { PublicSampleResearchRepository } from '../../domain/repository';
import { ExplorerPage } from '../explorer/ExplorerPage';
import { LiveCollectionPanel } from './LiveCollectionPanel';
import {
  LiveApiError,
  deriveLiveJobId,
  liveCapabilitiesSchema,
  liveSnapshotSchema,
  type LiveCapabilities,
  type LiveJob,
  type LiveRedditClient,
  type LiveSnapshot,
} from './api';

const now = '2026-08-23T10:00:00Z';
const jobId = 'a'.repeat(32);
const creationAccessToken = 'd'.repeat(48);

function capabilities(overrides: Partial<LiveCapabilities> = {}): LiveCapabilities {
  return liveCapabilitiesSchema.parse({
    enabled: true,
    provider: 'reddit',
    library: 'PRAW',
    server_credentials_configured: true,
    byo_credentials_allowed: true,
    credential_modes: ['server', 'provided'],
    creation_access_token_required: true,
    creation_access_token_header: 'X-Live-Access-Token',
    access_token_header: 'X-Live-Job-Token',
    time_filters: ['hour', 'day', 'week', 'month', 'year', 'all'],
    defaults: {
      time_filter: 'week',
      max_posts_per_mineral: 2,
      max_comments_per_post: 3,
    },
    limits: {
      max_targets: 10,
      max_subreddits_per_target: 20,
      max_posts_per_mineral: 100,
      max_comments_per_post: 500,
      max_records_per_job: 10_000,
      max_active_jobs: 2,
      retention_seconds: 900,
    },
    ...overrides,
  });
}

function job(overrides: Partial<LiveJob> = {}): LiveJob {
  return {
    id: jobId,
    status: 'queued',
    stage: 'queued',
    credential_mode: 'server',
    targets: [{ mineral: 'copper', subreddits: ['mining', 'geology'] }],
    time_filter: 'week',
    max_posts_per_mineral: 2,
    max_comments_per_post: 3,
    created_at: now,
    started_at: null,
    finished_at: null,
    expires_at: null,
    progress: {
      minerals_total: 1,
      minerals_completed: 0,
      subreddits_total: 2,
      subreddits_completed: 0,
      posts_discovered: 0,
      posts_stored: 0,
      posts_failed: 0,
      comments_stored: 0,
      searches_failed: 0,
    },
    record_count: 0,
    message: 'Collection queued.',
    error: null,
    ...overrides,
  };
}

const succeededJob = job({
  status: 'succeeded',
  stage: 'complete',
  started_at: now,
  finished_at: now,
  expires_at: '2026-08-23T10:15:00Z',
  progress: {
    minerals_total: 1,
    minerals_completed: 1,
    subreddits_total: 2,
    subreddits_completed: 2,
    posts_discovered: 1,
    posts_stored: 1,
    posts_failed: 0,
    comments_stored: 1,
    searches_failed: 0,
  },
  record_count: 2,
  message: 'Collection complete.',
});

const snapshot: LiveSnapshot = liveSnapshotSchema.parse({
  job_id: jobId,
  status: 'succeeded',
  generated_at: now,
  records: [
    {
      id: 'post1',
      kind: 'post',
      post_id: null,
      parent_id: null,
      depth: null,
      mineral: 'copper',
      title: 'Copper supply discussion',
      body: 'Raw post content from Reddit.',
      subreddit: 'mining',
      created_at: now,
      fetched_at: now,
      score: 42,
      comment_count: 1,
      upvote_ratio: 0.92,
      permalink: '/r/mining/comments/post1/copper_supply_discussion/',
      scrape_status: 'complete',
    },
    {
      id: 'comment1',
      kind: 'comment',
      post_id: 'post1',
      parent_id: 't3_post1',
      depth: 0,
      mineral: 'copper',
      title: null,
      body: 'Raw comment content from Reddit.',
      subreddit: 'mining',
      created_at: now,
      fetched_at: now,
      score: 5,
      comment_count: null,
      upvote_ratio: null,
      permalink: '/r/mining/comments/post1/copper_supply_discussion/comment1/',
      scrape_status: 'complete',
    },
  ],
});

interface ClientSpies {
  readonly client: LiveRedditClient;
  readonly createJob: ReturnType<typeof vi.fn<LiveRedditClient['createJob']>>;
  readonly getJob: ReturnType<typeof vi.fn<LiveRedditClient['getJob']>>;
  readonly cancelJob: ReturnType<typeof vi.fn<LiveRedditClient['cancelJob']>>;
  readonly getSnapshot: ReturnType<typeof vi.fn<LiveRedditClient['getSnapshot']>>;
}

function clientSpies(): ClientSpies {
  const createJob = vi.fn<LiveRedditClient['createJob']>();
  const getJob = vi.fn<LiveRedditClient['getJob']>();
  const cancelJob = vi.fn<LiveRedditClient['cancelJob']>();
  const getSnapshot = vi.fn<LiveRedditClient['getSnapshot']>();
  return {
    client: {
      capabilities: vi.fn().mockResolvedValue(capabilities()),
      createJob,
      getJob,
      cancelJob,
      getSnapshot,
    },
    createJob,
    getJob,
    cancelJob,
    getSnapshot,
  };
}

function acceptJob(spies: ClientSpies, acceptedJob: LiveJob) {
  spies.createJob.mockImplementation((_request, _creationToken, jobToken) =>
    Promise.resolve({ job: acceptedJob, access_token: jobToken }),
  );
}

function submittedJobToken(spies: ClientSpies): string {
  const token = spies.createJob.mock.calls[0]?.[2];
  if (typeof token !== 'string') throw new Error('Expected a submitted job access token.');
  return token;
}

function ResearchDeliveryProbe() {
  const { snapshot: researchSnapshot } = useResearch();
  return <output data-testid="research-delivery">{researchSnapshot?.delivery ?? 'loading'}</output>;
}

function renderPanel(
  client: LiveRedditClient,
  availableCapabilities = capabilities(),
  strict = false,
) {
  const content = (
    <MemoryRouter initialEntries={['/']}>
      <ResearchProvider repository={new PublicSampleResearchRepository()}>
        <ResearchDeliveryProbe />
        <Routes>
          <Route
            path="/"
            element={<LiveCollectionPanel capabilities={availableCapabilities} client={client} />}
          />
          <Route path="/explorer" element={<ExplorerPage />} />
        </Routes>
      </ResearchProvider>
    </MemoryRouter>
  );
  return render(strict ? <StrictMode>{content}</StrictMode> : content);
}

afterEach(() => vi.useRealTimers());

describe('LiveCollectionPanel', () => {
  it('survives Strict Mode, installs raw results, cleans the job, and hands off to Explorer', async () => {
    const user = userEvent.setup();
    const spies = clientSpies();
    const events: string[] = [];
    acceptJob(spies, succeededJob);
    spies.getSnapshot.mockImplementation(() => {
      events.push('snapshot');
      return Promise.resolve(snapshot);
    });
    spies.cancelJob.mockImplementation(() => {
      events.push('cleanup');
      return Promise.resolve({ ...succeededJob, record_count: 0, message: 'Live job deleted.' });
    });
    renderPanel(spies.client, capabilities(), true);

    await user.click(screen.getByRole('button', { name: 'Add mineral target' }));
    await user.type(screen.getByLabelText('Mineral 2'), 'lithium');
    await user.type(screen.getByLabelText('Subreddits 2'), 'batteries');
    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));

    expect(await screen.findByText('2 raw records ready')).toBeVisible();
    const jobAccessToken = submittedJobToken(spies);
    await waitFor(() => expect(spies.cancelJob).toHaveBeenCalledWith(jobId, jobAccessToken));
    expect(events).toEqual(['snapshot', 'cleanup']);
    expect(spies.getSnapshot).toHaveBeenCalledTimes(1);
    expect(screen.getByLabelText('Mineral 1')).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Start live collection' })).toBeDisabled();
    expect(spies.createJob.mock.calls[0]?.[0].targets).toEqual([
      { mineral: 'copper', subreddits: ['mining', 'geology'] },
      { mineral: 'lithium', subreddits: ['batteries'] },
    ]);
    expect(spies.createJob.mock.calls[0]?.[1]).toBe(creationAccessToken);
    expect(jobAccessToken).toMatch(/^[A-Za-z0-9_-]{43}$/u);

    await user.click(await screen.findByRole('button', { name: /open in explorer/i }));
    expect(await screen.findByText('Live Reddit · raw collection')).toBeVisible();
    expect(screen.getByRole('heading', { name: '2 records' })).toBeVisible();
    expect(screen.getAllByText('Not analyzed')).toHaveLength(2);
    await user.click(screen.getByRole('button', { name: 'Inspect Copper supply discussion' }));
    expect(await screen.findByRole('heading', { name: 'Raw collection only' })).toBeVisible();
    expect(
      screen.getByText(/no sentiment, stance, topic, relevance, or reputation/i),
    ).toBeVisible();
    expect(screen.queryByText('Published analysis provenance')).not.toBeInTheDocument();
  });

  it('clears provided credentials as soon as a failed POST begins', async () => {
    const user = userEvent.setup();
    const spies = clientSpies();
    spies.createJob.mockRejectedValue(
      new LiveApiError('The live request was not authorized.', 401),
    );
    const providedOnly = capabilities({
      server_credentials_configured: false,
      credential_modes: ['provided'],
    });
    renderPanel(spies.client, providedOnly);

    await user.type(screen.getByLabelText('Client ID'), 'browser-client-id');
    await user.type(screen.getByLabelText('Client secret'), 'browser-client-secret');
    await user.type(
      screen.getByLabelText('User agent'),
      'script:minerallens:test (by u/researcher)',
    );
    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'The live request was not authorized.',
    );
    expect(screen.getByLabelText('Client ID')).toHaveValue('');
    expect(screen.getByLabelText('Client secret')).toHaveValue('');
    expect(screen.getByLabelText('User agent')).toHaveValue('');
    expect(screen.getByLabelText('Deployment access token')).toHaveValue('');
    const submittedRequest = spies.createJob.mock.calls[0]?.[0];
    expect(submittedRequest?.credential_mode).toBe('provided');
    expect(submittedRequest?.credentials?.client_secret).toBe('browser-client-secret');
    expect(spies.createJob.mock.calls[0]?.[1]).toBe(creationAccessToken);
  });

  it('requests cooperative cancellation with the one-time token', async () => {
    const user = userEvent.setup();
    const spies = clientSpies();
    acceptJob(spies, job());
    spies.cancelJob.mockResolvedValue(
      job({
        status: 'cancel_requested',
        stage: 'collecting',
        message: 'Cancellation requested.',
      }),
    );
    renderPanel(spies.client);

    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));
    await user.click(await screen.findByRole('button', { name: 'Cancel collection' }));

    expect(spies.cancelJob).toHaveBeenCalledWith(jobId, submittedJobToken(spies));
    expect(await screen.findByText('Cancellation requested.')).toBeVisible();
  });

  it('retains job access and retries polling after a transient refresh failure', async () => {
    vi.useFakeTimers();
    const spies = clientSpies();
    acceptJob(spies, job());
    spies.getJob
      .mockRejectedValueOnce(new LiveApiError('The live collection service is unavailable.'))
      .mockResolvedValueOnce(succeededJob);
    spies.getSnapshot.mockResolvedValue(snapshot);
    spies.cancelJob.mockResolvedValue({
      ...succeededJob,
      record_count: 0,
      message: 'Live job deleted.',
    });
    renderPanel(spies.client);

    fireEvent.change(screen.getByLabelText('Progress refresh'), { target: { value: '1' } });
    fireEvent.change(screen.getByLabelText('Deployment access token'), {
      target: { value: creationAccessToken },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Start live collection' }));
    await act(() => Promise.resolve());
    await act(() => vi.advanceTimersByTimeAsync(1_000));

    expect(screen.getByRole('alert')).toHaveTextContent('retry automatically');
    expect(spies.getJob).toHaveBeenCalledTimes(1);
    await act(() => vi.advanceTimersByTimeAsync(2_000));

    expect(spies.getJob).toHaveBeenCalledTimes(2);
    expect(spies.getJob).toHaveBeenLastCalledWith(jobId, submittedJobToken(spies));
    await act(() => Promise.resolve());
    expect(screen.getByText('2 raw records ready')).toBeVisible();
  });

  it('allows a terminal snapshot handoff to be retried without losing its token', async () => {
    const user = userEvent.setup();
    const spies = clientSpies();
    acceptJob(spies, succeededJob);
    spies.getSnapshot
      .mockRejectedValueOnce(new LiveApiError('The live collection service is unavailable.'))
      .mockResolvedValueOnce(snapshot);
    spies.cancelJob.mockResolvedValue({
      ...succeededJob,
      record_count: 0,
      message: 'Live job deleted.',
    });
    renderPanel(spies.client);

    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));
    await user.click(await screen.findByRole('button', { name: 'Retry result handoff' }));

    expect(await screen.findByText('2 raw records ready')).toBeVisible();
    const jobAccessToken = submittedJobToken(spies);
    expect(spies.getSnapshot).toHaveBeenNthCalledWith(1, jobId, jobAccessToken);
    expect(spies.getSnapshot).toHaveBeenNthCalledWith(2, jobId, jobAccessToken);
    expect(spies.cancelJob).toHaveBeenCalledWith(jobId, jobAccessToken);
  });

  it('withholds the ResearchContext handoff until terminal cleanup succeeds', async () => {
    const user = userEvent.setup();
    const spies = clientSpies();
    let resolveCleanup: ((deletedJob: LiveJob) => void) | undefined;
    acceptJob(spies, succeededJob);
    spies.getSnapshot.mockResolvedValue(snapshot);
    spies.cancelJob.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveCleanup = resolve;
        }),
    );
    renderPanel(spies.client);

    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));

    expect(await screen.findByText(/Removing server-side job metadata/i)).toBeVisible();
    expect(screen.getByTestId('research-delivery')).not.toHaveTextContent('live');
    expect(screen.queryByText('2 raw records ready')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /open in explorer/i })).not.toBeInTheDocument();

    await act(() => {
      resolveCleanup?.({
        ...succeededJob,
        record_count: 0,
        message: 'Live job deleted.',
      });
      return Promise.resolve();
    });

    expect(await screen.findByText('2 raw records ready')).toBeVisible();
    expect(screen.getByTestId('research-delivery')).toHaveTextContent('live');
    expect(screen.getByRole('button', { name: /open in explorer/i })).toBeEnabled();
  });

  it('retains the job token and requires successful cleanup before Explorer handoff', async () => {
    const user = userEvent.setup();
    const spies = clientSpies();
    acceptJob(spies, succeededJob);
    spies.getSnapshot.mockResolvedValue(snapshot);
    spies.cancelJob
      .mockRejectedValueOnce(new LiveApiError('The live collection service is unavailable.'))
      .mockResolvedValueOnce({
        ...succeededJob,
        record_count: 0,
        message: 'Live job deleted.',
      });
    renderPanel(spies.client);

    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('Job access is retained in memory');
    expect(screen.queryByText('2 raw records ready')).not.toBeInTheDocument();
    expect(screen.getByTestId('research-delivery')).not.toHaveTextContent('live');
    expect(screen.queryByRole('button', { name: /open in explorer/i })).not.toBeInTheDocument();
    const jobAccessToken = submittedJobToken(spies);
    await user.click(screen.getByRole('button', { name: 'Retry secure cleanup' }));

    expect(await screen.findByRole('button', { name: /open in explorer/i })).toBeEnabled();
    expect(screen.getByTestId('research-delivery')).toHaveTextContent('live');
    expect(spies.cancelJob).toHaveBeenNthCalledWith(1, jobId, jobAccessToken);
    expect(spies.cancelJob).toHaveBeenNthCalledWith(2, jobId, jobAccessToken);
  });

  it('retries an ambiguous create once with the same idempotency token', async () => {
    const user = userEvent.setup();
    const spies = clientSpies();
    spies.createJob
      .mockRejectedValueOnce(new LiveApiError('The live collection service is unavailable.'))
      .mockImplementationOnce((_request, _creationToken, jobToken) =>
        Promise.resolve({ job: succeededJob, access_token: jobToken }),
      );
    spies.getJob.mockRejectedValueOnce(new LiveApiError('Not found.', 404));
    spies.getSnapshot.mockResolvedValue(snapshot);
    spies.cancelJob.mockResolvedValue({
      ...succeededJob,
      record_count: 0,
      message: 'Live job deleted.',
    });
    renderPanel(spies.client, capabilities(), true);

    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));

    expect(await screen.findByRole('button', { name: /open in explorer/i })).toBeEnabled();
    expect(spies.createJob).toHaveBeenCalledTimes(2);
    expect(spies.createJob.mock.calls[0]?.[0]).toEqual(spies.createJob.mock.calls[1]?.[0]);
    expect(spies.createJob.mock.calls[0]?.[1]).toBe(creationAccessToken);
    expect(spies.createJob.mock.calls[1]?.[1]).toBe(creationAccessToken);
    expect(spies.createJob.mock.calls[0]?.[2]).toBe(spies.createJob.mock.calls[1]?.[2]);
  });

  it('recovers an accepted job by derived id when the POST response is lost', async () => {
    const user = userEvent.setup();
    const spies = clientSpies();
    spies.createJob.mockRejectedValueOnce(
      new LiveApiError('The live collection service is unavailable.'),
    );
    spies.getJob.mockImplementationOnce((recoveredJobId) =>
      Promise.resolve({ ...succeededJob, id: recoveredJobId }),
    );
    spies.getSnapshot.mockImplementationOnce((recoveredJobId) =>
      Promise.resolve({ ...snapshot, job_id: recoveredJobId }),
    );
    spies.cancelJob.mockResolvedValue({
      ...succeededJob,
      record_count: 0,
      message: 'Live job deleted.',
    });
    renderPanel(spies.client);

    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));

    expect(await screen.findByRole('button', { name: /open in explorer/i })).toBeEnabled();
    const jobAccessToken = submittedJobToken(spies);
    const derivedJobId = await deriveLiveJobId(jobAccessToken);
    expect(spies.createJob).toHaveBeenCalledTimes(1);
    expect(spies.getJob).toHaveBeenCalledWith(derivedJobId, jobAccessToken);
    expect(spies.getSnapshot).toHaveBeenCalledWith(derivedJobId, jobAccessToken);
  });

  it('resolves an uncertain submission and resumes a retained job', async () => {
    const user = userEvent.setup();
    const spies = clientSpies();
    spies.createJob.mockRejectedValueOnce(
      new LiveApiError('The live collection service is unavailable.'),
    );
    spies.getJob
      .mockRejectedValueOnce(new LiveApiError('The live collection service is unavailable.'))
      .mockImplementationOnce((recoveredJobId) =>
        Promise.resolve({ ...succeededJob, id: recoveredJobId }),
      );
    spies.getSnapshot.mockImplementationOnce((recoveredJobId) =>
      Promise.resolve({ ...snapshot, job_id: recoveredJobId }),
    );
    spies.cancelJob.mockResolvedValue({
      ...succeededJob,
      record_count: 0,
      message: 'Live job deleted.',
    });
    renderPanel(spies.client);

    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));

    const resolveButton = await screen.findByRole('button', { name: 'Resolve uncertain job' });
    const jobAccessToken = submittedJobToken(spies);
    const derivedJobId = await deriveLiveJobId(jobAccessToken);
    await user.click(resolveButton);

    expect(await screen.findByText('2 raw records ready')).toBeVisible();
    expect(spies.getJob).toHaveBeenNthCalledWith(1, derivedJobId, jobAccessToken);
    expect(spies.getJob).toHaveBeenNthCalledWith(2, derivedJobId, jobAccessToken);
    expect(spies.getSnapshot).toHaveBeenCalledWith(derivedJobId, jobAccessToken);
  });

  it('unlocks the form when uncertainty resolution confirms no retained job', async () => {
    const user = userEvent.setup();
    const spies = clientSpies();
    spies.createJob
      .mockRejectedValueOnce(new LiveApiError('The live collection service is unavailable.'))
      .mockImplementationOnce((_request, _creationToken, jobToken) =>
        Promise.resolve({ job: job(), access_token: jobToken }),
      );
    spies.getJob
      .mockRejectedValueOnce(new LiveApiError('The live collection service is unavailable.'))
      .mockRejectedValueOnce(new LiveApiError('Not found.', 404));
    spies.cancelJob.mockResolvedValue(
      job({ status: 'cancel_requested', message: 'Cancellation requested.' }),
    );
    renderPanel(spies.client);

    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));
    const firstJobAccessToken = submittedJobToken(spies);
    await user.click(await screen.findByRole('button', { name: 'Resolve uncertain job' }));

    expect(await screen.findByText(/No retained job was found/i)).toBeVisible();
    expect(screen.queryByRole('button', { name: 'Resolve uncertain job' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Start live collection' })).toBeEnabled();

    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));
    expect(await screen.findByText('Collection queued.')).toBeVisible();
    expect(spies.createJob).toHaveBeenCalledTimes(2);
    expect(spies.createJob.mock.calls[1]?.[2]).not.toBe(firstJobAccessToken);
  });

  it('retains one uncertain token across retryable resolution failures', async () => {
    const user = userEvent.setup();
    const spies = clientSpies();
    spies.createJob.mockRejectedValueOnce(
      new LiveApiError('The live collection service is unavailable.'),
    );
    spies.getJob
      .mockRejectedValueOnce(new LiveApiError('The live collection service is unavailable.'))
      .mockRejectedValueOnce(new LiveApiError('The live collection service is unavailable.'))
      .mockImplementationOnce((recoveredJobId) =>
        Promise.resolve({ ...succeededJob, id: recoveredJobId }),
      );
    spies.getSnapshot.mockImplementationOnce((recoveredJobId) =>
      Promise.resolve({ ...snapshot, job_id: recoveredJobId }),
    );
    spies.cancelJob.mockResolvedValue({
      ...succeededJob,
      record_count: 0,
      message: 'Live job deleted.',
    });
    renderPanel(spies.client);

    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));
    const jobAccessToken = submittedJobToken(spies);
    const derivedJobId = await deriveLiveJobId(jobAccessToken);
    await user.click(await screen.findByRole('button', { name: 'Resolve uncertain job' }));

    expect(await screen.findByText(/could not be resolved yet/i)).toBeVisible();
    const retryButton = screen.getByRole('button', { name: 'Resolve uncertain job' });
    expect(retryButton).toBeEnabled();
    await user.click(retryButton);

    expect(await screen.findByText('2 raw records ready')).toBeVisible();
    expect(spies.getJob).toHaveBeenCalledTimes(3);
    for (const call of spies.getJob.mock.calls) {
      expect(call).toEqual([derivedJobId, jobAccessToken]);
    }
  });

  it('best-effort cancels a job whose create response arrives after unmount', async () => {
    const user = userEvent.setup();
    const spies = clientSpies();
    let resolveCreate:
      ((response: Awaited<ReturnType<LiveRedditClient['createJob']>>) => void) | undefined;
    spies.createJob.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveCreate = (response) => resolve(response);
        }),
    );
    spies.cancelJob.mockResolvedValue(
      job({ status: 'cancel_requested', message: 'Cancellation requested.' }),
    );
    const rendered = renderPanel(spies.client);

    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));
    await waitFor(() => expect(spies.createJob).toHaveBeenCalledTimes(1));
    const jobAccessToken = submittedJobToken(spies);
    rendered.unmount();
    await act(() => {
      resolveCreate?.({ job: job(), access_token: jobAccessToken });
      return Promise.resolve();
    });

    await waitFor(() => expect(spies.cancelJob).toHaveBeenCalledWith(jobId, jobAccessToken));
  });

  it('disables submission when an enabled backend has no usable credential mode', () => {
    const spies = clientSpies();
    renderPanel(
      spies.client,
      capabilities({
        server_credentials_configured: false,
        byo_credentials_allowed: false,
        credential_modes: [],
      }),
    );

    expect(screen.getByLabelText('Credential mode')).toHaveValue('');
    expect(screen.getByRole('button', { name: 'Start live collection' })).toBeDisabled();
  });

  it('preflights the aggregate record budget before creating a job', async () => {
    const user = userEvent.setup();
    const spies = clientSpies();
    const baseCapabilities = capabilities();
    renderPanel(spies.client, {
      ...baseCapabilities,
      limits: { ...baseCapabilities.limits, max_records_per_job: 10 },
    });

    expect(screen.getByText('8 / 10')).toBeVisible();
    await user.click(screen.getByRole('button', { name: 'Add mineral target' }));
    await user.type(screen.getByLabelText('Mineral 2'), 'lithium');
    await user.type(screen.getByLabelText('Subreddits 2'), 'batteries');
    await user.type(screen.getByLabelText('Deployment access token'), creationAccessToken);
    expect(screen.getByText('16 / 10')).toBeVisible();
    await user.click(screen.getByRole('button', { name: 'Start live collection' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('reduce its targets or limits');
    expect(spies.createJob).not.toHaveBeenCalled();
  });
});
