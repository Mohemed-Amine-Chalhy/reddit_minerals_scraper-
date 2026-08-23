import { render } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { App } from '../app/App';
import { ResearchProvider } from '../app/research';
import { PublicSampleResearchRepository, type ResearchRepository } from '../domain/repository';

export function renderApp(
  route = '/',
  repository: ResearchRepository = new PublicSampleResearchRepository(),
) {
  return render(
    <MemoryRouter initialEntries={[route]}>
      <ResearchProvider repository={repository}>
        <App />
      </ResearchProvider>
    </MemoryRouter>,
  );
}
