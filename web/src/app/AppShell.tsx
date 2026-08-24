import { NavLink, Outlet } from 'react-router-dom';
import { useResearch } from './research';

const navigation = [
  { to: '/', label: 'Overview', end: true },
  { to: '/explorer', label: 'Explorer', end: false },
  { to: '/pipeline', label: 'Pipeline', end: false },
  { to: '/engineering', label: 'Engineering', end: false },
] as const;

export function AppShell() {
  const { snapshot } = useResearch();
  const badge = !snapshot
    ? 'Research dataset'
    : snapshot.provenance.kind === 'live-reddit'
      ? 'Live Reddit snapshot'
      : snapshot.provenance.kind === 'public-research-sample'
        ? 'Public dataset sample'
        : snapshot.provenance.kind === 'local-import'
          ? 'Local browser import'
          : 'Synthetic fixture';

  return (
    <div className="app-shell">
      <a className="skip-link" href="#main-content">
        Skip to content
      </a>
      <div className="ambient-grid" aria-hidden="true" />
      <header className="site-header">
        <NavLink className="brand" to="/" aria-label="MineralLens home">
          <span className="brand-mark" aria-hidden="true">
            <span />
            <span />
            <span />
          </span>
          <span>
            <strong>MineralLens</strong>
            <small>Research systems explorer</small>
          </span>
        </NavLink>
        <nav aria-label="Primary navigation">
          {navigation.map((item) => (
            <NavLink
              key={item.to}
              end={item.end}
              to={item.to}
              className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
        <div className="header-actions">
          <span className="demo-badge">
            <span aria-hidden="true" /> {badge}
          </span>
          <a
            className="icon-link"
            href="https://github.com/Mohemed-Amine-Chalhy/reddit_minerals_scraper-"
            target="_blank"
            rel="noreferrer"
          >
            Source <span aria-hidden="true">↗</span>
          </a>
        </div>
      </header>
      <main id="main-content" tabIndex={-1}>
        <Outlet />
      </main>
      <footer className="site-footer">
        <span>MineralLens · critical-minerals research intelligence</span>
        <span>
          {snapshot?.provenance.kind === 'live-reddit'
            ? 'Live snapshot loaded · ready for exploration'
            : 'Static dataset mode · live collection available with the backend'}
        </span>
      </footer>
    </div>
  );
}
