import type { DistributionDatum } from '../domain/selectors';

interface DistributionBarsProps {
  readonly title: string;
  readonly description: string;
  readonly data: readonly DistributionDatum[];
  readonly percent?: boolean;
}

export function DistributionBars({
  title,
  description,
  data,
  percent = false,
}: DistributionBarsProps) {
  const max = Math.max(1, ...data.map((item) => item.value));
  return (
    <figure className="panel chart-panel">
      <figcaption>
        <div>
          <h2>{title}</h2>
          <p>{description}</p>
        </div>
        <span className="chart-key">{percent ? 'Average signal' : 'Records'}</span>
      </figcaption>
      {data.length ? (
        <ul className="bar-chart" aria-label={`${title} values`}>
          {data.map((item) => (
            <li key={item.label}>
              <div className="bar-label">
                <span>{item.label}</span>
                <strong>
                  {item.value}
                  {percent ? '%' : ''}
                </strong>
              </div>
              <div className="bar-track" aria-hidden="true">
                <span
                  className={`bar-fill tone-${item.tone}`}
                  style={{ width: `${Math.max(4, (item.value / max) * 100)}%` }}
                />
              </div>
            </li>
          ))}
        </ul>
      ) : (
        <p className="empty-copy">No completed analyses match these controls.</p>
      )}
    </figure>
  );
}
