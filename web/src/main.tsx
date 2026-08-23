import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { App } from './app/App';
import { ResearchProvider } from './app/research';
import './styles/index.css';

const root = document.getElementById('root');
if (!root) throw new Error('Application root element is missing');
const basename = import.meta.env.BASE_URL.replace(/\/$/u, '') || '/';

createRoot(root).render(
  <StrictMode>
    <BrowserRouter basename={basename}>
      <ResearchProvider>
        <App />
      </ResearchProvider>
    </BrowserRouter>
  </StrictMode>,
);
