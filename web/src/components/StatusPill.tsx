import type { AnalysisStatus } from '../domain/schemas';

export function StatusPill({ status }: { readonly status: AnalysisStatus }) {
  return <span className={`status-pill status-${status}`}>{status.replaceAll('_', ' ')}</span>;
}
