interface MetricCardProps {
  readonly label: string;
  readonly value: string | number;
  readonly detail: string;
  readonly tone?: 'gold' | 'teal' | 'blue' | 'neutral';
}

export function MetricCard({ label, value, detail, tone = 'neutral' }: MetricCardProps) {
  return (
    <article className={`metric-card tone-${tone}`}>
      <span className="metric-label">{label}</span>
      <strong>{value}</strong>
      <span className="metric-detail">{detail}</span>
    </article>
  );
}
