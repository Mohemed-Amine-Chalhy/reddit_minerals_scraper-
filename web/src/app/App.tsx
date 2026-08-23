import { Navigate, Route, Routes } from 'react-router-dom';
import { AppShell } from './AppShell';
import { EngineeringPage } from '../features/engineering/EngineeringPage';
import { ExplorerPage } from '../features/explorer/ExplorerPage';
import { OverviewPage } from '../features/overview/OverviewPage';
import { PipelinePage } from '../features/pipeline/PipelinePage';

export function App() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<OverviewPage />} />
        <Route path="explorer" element={<ExplorerPage />} />
        <Route path="pipeline" element={<PipelinePage />} />
        <Route path="engineering" element={<EngineeringPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}
