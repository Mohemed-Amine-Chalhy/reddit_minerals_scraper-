import campusContextImage from '../../assets/um6p-campus-context-v1.webp';

const datasetUrl = 'https://www.kaggle.com/datasets/mohamedaminechalhy/reddit-mining-stance';
const qualityEvidenceUrl =
  'https://github.com/Mohemed-Amine-Chalhy/reddit_minerals_scraper-/actions/workflows/ci.yml';

export function CampusContext() {
  return (
    <section className="campus-context" aria-labelledby="campus-context-title">
      <div className="campus-context-heading">
        <div>
          <p className="eyebrow">Project lineage</p>
          <h2 id="campus-context-title">Engineering depth, carried from research to product.</h2>
        </div>
        <p>
          The work connects an industrial-engineering foundation in Benguerir with a critical-metals
          research internship in Nancy, then turns that research path into an inspectable software
          system.
        </p>
      </div>

      <div className="campus-context-grid">
        <figure className="campus-visual">
          <img
            src={campusContextImage}
            width="1536"
            height="1024"
            loading="lazy"
            decoding="async"
            alt="Original illustration of the UM6P campus context in Benguerir, with terracotta courtyard buildings, solar roofs, palms, and a shaded central walkway."
          />
          <span className="campus-visual-label">Benguerir · Morocco</span>
          <figcaption>
            <span aria-hidden="true">◇</span>
            Original illustrated UM6P campus context · Benguerir, Morocco
          </figcaption>
        </figure>

        <div className="project-route-panel">
          <p className="route-kicker">Research-to-system route</p>
          <ol className="project-route" aria-label="Project journey">
            <li>
              <span className="route-index">01</span>
              <div>
                <small>Engineering foundation</small>
                <strong>
                  <a href="https://www.um6p.ma/" target="_blank" rel="noreferrer">
                    EMINES · UM6P, Benguerir
                  </a>
                </strong>
                <span>Morocco</span>
                <p>Systems thinking, industrial context, and rigorous problem framing.</p>
              </div>
            </li>
            <li>
              <span className="route-index">02</span>
              <div>
                <small>Research internship</small>
                <strong>
                  <a
                    href="https://mines-nancy.univ-lorraine.fr/en/"
                    target="_blank"
                    rel="noreferrer"
                  >
                    Mines Nancy, France
                  </a>
                </strong>
                <span>Nancy</span>
                <p>Critical-metals discourse, reproducible acquisition, and traceable analysis.</p>
              </div>
            </li>
            <li>
              <span className="route-index">03</span>
              <div>
                <small>System outcome</small>
                <strong>MineralLens</strong>
                <span>Typed research intelligence</span>
                <p>One coherent interface across collection, state, evidence, and exploration.</p>
              </div>
            </li>
          </ol>
        </div>
      </div>

      <div className="project-scale" aria-label="Verified project scale">
        <article>
          <strong>1.04M</strong>
          <span>published research records</span>
          <a href={datasetUrl} target="_blank" rel="noreferrer">
            Public dataset <span aria-hidden="true">↗</span>
          </a>
        </article>
        <article>
          <strong>26</strong>
          <span>critical-mineral topics</span>
          <a href={datasetUrl} target="_blank" rel="noreferrer">
            Dataset scope <span aria-hidden="true">↗</span>
          </a>
        </article>
        <article>
          <strong>351</strong>
          <span>passing backend + frontend tests</span>
          <a href={qualityEvidenceUrl} target="_blank" rel="noreferrer">
            CI evidence <span aria-hidden="true">↗</span>
          </a>
        </article>
        <article>
          <strong>92.40%</strong>
          <span>backend test coverage</span>
          <a href={qualityEvidenceUrl} target="_blank" rel="noreferrer">
            Quality workflow <span aria-hidden="true">↗</span>
          </a>
        </article>
      </div>
    </section>
  );
}
