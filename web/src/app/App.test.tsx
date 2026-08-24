import axe from 'axe-core';
import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { DEMO_RECORDS } from '../data/fixtures';
import { MAX_IMPORT_BYTES } from '../domain/importer';
import { SyntheticFixtureResearchRepository } from '../domain/repository';
import { renderApp } from '../test/renderApp';

afterEach(() => vi.unstubAllGlobals());

describe('MineralLens application', () => {
  it('renders an accessible public research overview with configurable controls', async () => {
    const user = userEvent.setup();
    const { container } = renderApp('/');
    expect(
      await screen.findByRole('heading', { name: /from research data to signals/i }),
    ).toBeInTheDocument();
    expect(screen.getByText('Public dataset sample')).toBeVisible();
    expect(screen.getByText('104')).toBeVisible();
    await user.selectOptions(screen.getByLabelText('Mineral'), 'gold');
    expect(screen.getByText('4')).toBeVisible();
    await user.selectOptions(screen.getByLabelText('Date window'), '180');
    const results = await axe.run(container, {
      rules: { 'color-contrast': { enabled: false } },
    });
    expect(results.violations).toEqual([]);
  });

  it('uses URL-backed explorer filters and opens an analysis detail panel', async () => {
    const user = userEvent.setup();
    renderApp('/explorer?mineral=gold&recordType=post');
    expect(await screen.findByRole('heading', { name: '2 records' })).toBeInTheDocument();
    const inspect = screen.getAllByRole('button', { name: /inspect gold post metadata/i })[0];
    if (!inspect) throw new Error('Expected a curated gold record.');
    await user.click(inspect);
    expect(await screen.findByRole('heading', { name: /gold post metadata/i })).toBeVisible();
    expect(screen.getByText(/raw reddit text is not included/i)).toBeVisible();
    expect(screen.getByText('Published analysis provenance')).toBeVisible();
    expect(
      screen.getByText(/model, prompt, schema, and latency metadata were not included/i),
    ).toBeVisible();
  });

  it('rejects an oversized local export before reading it', async () => {
    const user = userEvent.setup();
    renderApp('/explorer');
    await screen.findByRole('heading', { name: '104 records' });
    const file = new File(['{}'], 'oversized.json', { type: 'application/json' });
    Object.defineProperty(file, 'size', { value: MAX_IMPORT_BYTES + 1 });
    const readFile = vi.fn(() => Promise.resolve('{}'));
    Object.defineProperty(file, 'text', { value: readFile });

    await user.upload(screen.getByLabelText('Inspect local export'), file);

    expect(await screen.findByRole('alert')).toHaveTextContent('larger than the 10 MB demo limit');
    expect(readFile).not.toHaveBeenCalled();
  });

  it('labels a browser-local import and restores the public sample', async () => {
    const user = userEvent.setup();
    renderApp('/explorer');
    await screen.findByRole('heading', { name: '104 records' });
    const localRecord = DEMO_RECORDS[0];
    if (!localRecord) throw new Error('Expected a regression fixture record.');

    const fileText = JSON.stringify(localRecord);
    const file = new File([fileText], 'local-export.json', { type: 'application/json' });
    Object.defineProperty(file, 'text', { value: () => Promise.resolve(fileText) });
    await user.upload(screen.getByLabelText('Inspect local export'), file);

    expect(await screen.findByText('Local browser import')).toBeVisible();
    expect(screen.getByText('Local browser preview')).toBeVisible();
    await user.click(screen.getByRole('button', { name: 'Restore research sample' }));
    expect(await screen.findByText('Public dataset sample')).toBeVisible();
    expect(screen.getByText('Bundled public Kaggle research sample')).toBeVisible();
  });

  it('presents the engineering case study and illustrated campus context accessibly', async () => {
    const { container } = renderApp('/engineering');

    expect(
      await screen.findByRole('heading', {
        level: 1,
        name: /from public discourse to decision-ready evidence/i,
      }),
    ).toBeVisible();

    const campusImage = screen.getByRole('img');
    expect(campusImage).toHaveAccessibleName(/um6p/i);
    expect(campusImage).toHaveAccessibleName(/benguerir|campus/i);
    expect(campusImage.getAttribute('src')).toContain('um6p-campus-context-v1');

    const campusFigure = campusImage.closest('figure');
    if (!campusFigure) throw new Error('Expected the campus illustration to use a figure.');
    const campusCaption = campusFigure.querySelector('figcaption');
    if (!campusCaption) throw new Error('Expected the campus illustration to have a caption.');
    expect(campusCaption).toBeVisible();
    expect(campusCaption).toHaveTextContent(/original illustrated (?:um6p )?campus context/i);

    const projectJourney = screen.getByRole('list', { name: /project journey/i });
    const routeStops = within(projectJourney).getAllByRole('listitem');
    expect(routeStops).toHaveLength(3);
    expect(routeStops[0]).toHaveTextContent(/emines\s*·\s*um6p/i);
    expect(routeStops[0]).toHaveTextContent(/benguerir,?\s*morocco/i);
    expect(routeStops[1]).toHaveTextContent(/mines nancy/i);
    expect(routeStops[1]).toHaveTextContent(/nancy,?\s*france/i);
    expect(routeStops[2]).toHaveTextContent(/minerallens/i);

    const repositoryCta = screen.getByRole('link', { name: /browse the repository/i });
    expect(repositoryCta).toHaveAttribute(
      'href',
      'https://github.com/Mohemed-Amine-Chalhy/reddit_minerals_scraper-',
    );
    expect(repositoryCta).toHaveAttribute('target', '_blank');
    expect(repositoryCta.getAttribute('rel')?.split(/\s+/u)).toContain('noreferrer');

    const externalLinks = screen
      .getAllByRole('link')
      .filter((link) => link.getAttribute('target') === '_blank');
    expect(externalLinks.length).toBeGreaterThan(0);
    for (const link of externalLinks) {
      expect(link.getAttribute('rel')?.split(/\s+/u)).toContain('noreferrer');
    }

    const results = await axe.run(container, {
      rules: { 'color-contrast': { enabled: false } },
    });
    expect(results.violations).toEqual([]);
  });

  it('configures and controls a deterministic pipeline replay', async () => {
    const user = userEvent.setup();
    renderApp('/pipeline');
    expect(
      await screen.findByRole('heading', { name: /reliability is visible/i }),
    ).toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText('Scenario'), 'retry');
    await user.click(screen.getByRole('button', { name: 'Start replay' }));
    expect(screen.getAllByText('Reading bounded synthetic provider records.')).toHaveLength(2);
    await user.click(screen.getByRole('button', { name: 'Reset' }));
    expect(screen.getByText('Select a scenario and start the replay.')).toBeVisible();
  });

  it('keeps live controls hidden when the backend capability is disabled', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () =>
        Promise.resolve({
          enabled: false,
          provider: 'reddit',
          library: 'PRAW',
          server_credentials_configured: false,
          byo_credentials_allowed: false,
          credential_modes: [],
          creation_access_token_required: true,
          creation_access_token_header: 'X-Live-Access-Token',
          access_token_header: 'X-Live-Job-Token',
          time_filters: ['week'],
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
        }),
    });
    vi.stubGlobal('fetch', fetchMock);

    renderApp('/pipeline');
    expect(await screen.findByRole('heading', { name: /reliability is visible/i })).toBeVisible();
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(screen.queryByRole('button', { name: /live reddit/i })).not.toBeInTheDocument();
  });

  it('keeps synthetic run history inside an explicitly injected fixture', async () => {
    renderApp('/pipeline', new SyntheticFixtureResearchRepository());

    expect(await screen.findByText('Synthetic fixture')).toBeVisible();
    expect(screen.getByRole('heading', { name: 'Synthetic run fixtures' })).toBeVisible();
    expect(screen.getByText('run-1')).toBeVisible();
  });
});
