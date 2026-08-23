import { PageHeader } from '../../components/PageHeader';

const repositoryRoot =
  'https://github.com/Mohemed-Amine-Chalhy/reddit_minerals_scraper-/blob/main/';

const decisions = [
  {
    number: '01',
    challenge: 'Provider SDK churn and live-test risk',
    decision: 'Narrow injected Reddit and analysis protocols keep orchestration independent.',
    evidence: 'src/reddit_minerals/clients/base.py',
    label: 'Provider protocols',
  },
  {
    number: '02',
    challenge: 'Partial and interrupted batches',
    decision: 'Content and explicit work states advance in the same SQLite transaction.',
    evidence: 'src/reddit_minerals/services/scrape.py',
    label: 'Scrape service',
  },
  {
    number: '03',
    challenge: 'Late AI responses racing changed data',
    decision: 'Input, configuration, and dependency revisions are checked at commit time.',
    evidence: 'tests/test_database.py',
    label: 'Concurrency tests',
  },
  {
    number: '04',
    challenge: 'Untrusted structured output',
    decision: 'Strict bounded Pydantic schemas reject invalid provider responses.',
    evidence: 'src/reddit_minerals/models.py',
    label: 'Domain models',
  },
  {
    number: '05',
    challenge: 'Environment and packaging drift',
    decision: 'A locked environment, cross-platform CI, artifact smoke tests, and non-root image.',
    evidence: '.github/workflows/ci.yml',
    label: 'CI workflow',
  },
  {
    number: '06',
    challenge: 'A credible demo without credentials',
    decision:
      'Deterministic adapters exercise the real services, database, and export path offline.',
    evidence: 'src/reddit_minerals/demo.py',
    label: 'Offline demo',
  },
] as const;

export function EngineeringPage() {
  return (
    <div className="page engineering-page">
      <PageHeader
        eyebrow="Engineering case study"
        title="Research software with production-grade boundaries."
        description="The system makes collection, model analysis, persistence, failure recovery, and provenance independently inspectable instead of hiding them inside one script."
        actions={
          <a
            className="button primary"
            href="https://github.com/Mohemed-Amine-Chalhy/reddit_minerals_scraper-"
            target="_blank"
            rel="noreferrer"
          >
            Browse the repository <span aria-hidden="true">↗</span>
          </a>
        }
      />

      <section className="context-grid">
        <article className="context-card featured">
          <p className="eyebrow">Research context</p>
          <h2>A software system supporting mineral-discourse research.</h2>
          <p>
            The pipeline supported research undertaken during an internship at{' '}
            <a href="https://mines-nancy.univ-lorraine.fr/en/" target="_blank" rel="noreferrer">
              Mines Nancy
            </a>
            . It provides a reproducible path from bounded public discussions to validated,
            provenance-aware analysis records.
          </p>
          <p>
            According to the project owner, the associated manuscript is currently in advanced
            pre-publication review. No claim of acceptance or peer review is made here.
          </p>
        </article>
        <article className="context-card">
          <p className="eyebrow">Portfolio interface</p>
          <h2>Built on the export boundary.</h2>
          <p>
            MineralLens is a portfolio presentation layer over the pipeline’s versioned export
            contract. It combines a curated public research sample, a typed read-only API, and local
            export inspection in one modern research interface.
          </p>
          <div className="context-tags">
            <span>React + TypeScript</span>
            <span>Schema validated</span>
            <span>Static deployable</span>
          </div>
        </article>
      </section>

      <section className="architecture-panel panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">System architecture</p>
            <h2>Ports at the edge. Transactions at the core.</h2>
          </div>
          <span className="chart-key">Click source evidence below</span>
        </div>
        <div className="architecture-map" aria-label="System component flow">
          <div className="architecture-column">
            <span className="architecture-label">Boundaries</span>
            <article className="architecture-node node-provider">
              <small>01</small>
              <strong>Reddit adapter</strong>
              <span>bounded read client</span>
            </article>
            <article className="architecture-node node-provider">
              <small>02</small>
              <strong>Analysis adapter</strong>
              <span>schema-only responses</span>
            </article>
            <article className="architecture-node node-demo">
              <small>00</small>
              <strong>Synthetic adapters</strong>
              <span>same protocols, offline</span>
            </article>
          </div>
          <span className="architecture-arrow" aria-hidden="true">
            →
          </span>
          <div className="architecture-column core-column">
            <span className="architecture-label">Application</span>
            <article className="architecture-node node-service">
              <small>03</small>
              <strong>Typed services</strong>
              <span>scrape · analyze · migrate</span>
            </article>
            <article className="architecture-node node-storage">
              <small>04</small>
              <strong>Transactional SQLite</strong>
              <span>canonical state · schema v3</span>
            </article>
          </div>
          <span className="architecture-arrow" aria-hidden="true">
            →
          </span>
          <div className="architecture-column">
            <span className="architecture-label">Views</span>
            <article className="architecture-node node-output">
              <small>05</small>
              <strong>Status + deletion</strong>
              <span>observable operations</span>
            </article>
            <article className="architecture-node node-output">
              <small>06</small>
              <strong>JSON / JSONL</strong>
              <span>atomic versioned exports</span>
            </article>
            <article className="architecture-node node-web">
              <small>07</small>
              <strong>MineralLens</strong>
              <span>read-only portfolio view</span>
            </article>
          </div>
        </div>
      </section>

      <section className="quality-strip" aria-label="Verified engineering quality">
        <article>
          <strong>351</strong>
          <span>passing Python + frontend tests</span>
        </article>
        <article>
          <strong>92.40%</strong>
          <span>total Python coverage</span>
        </article>
        <article>
          <strong>2 × 2</strong>
          <span>Python versions × operating systems</span>
        </article>
        <article>
          <strong>strict</strong>
          <span>mypy + typed package boundary</span>
        </article>
      </section>

      <section className="decision-section">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Design decisions</p>
            <h2>Every claim points to implementation evidence.</h2>
          </div>
        </div>
        <div className="decision-grid">
          {decisions.map((decision) => (
            <article key={decision.number}>
              <span className="decision-number">{decision.number}</span>
              <p>{decision.challenge}</p>
              <h3>{decision.decision}</h3>
              <a href={`${repositoryRoot}${decision.evidence}`} target="_blank" rel="noreferrer">
                {decision.label} <span aria-hidden="true">↗</span>
              </a>
            </article>
          ))}
        </div>
      </section>

      <section className="engineering-cta">
        <div>
          <p className="eyebrow">Run it without credentials</p>
          <h2>One command crosses every production boundary.</h2>
          <p>
            The offline CLI demo runs real collection and analysis services, initializes an isolated
            SQLite database, and publishes JSONL before cleaning up.
          </p>
        </div>
        <code>uv run reddit-minerals demo</code>
      </section>
    </div>
  );
}
