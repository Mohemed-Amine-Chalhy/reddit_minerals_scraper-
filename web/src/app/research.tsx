import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react';
import { ImportValidationError, MAX_IMPORT_BYTES, parseExportText } from '../domain/importer';
import {
  ApiResearchRepository,
  PublicSampleResearchRepository,
  SyntheticFixtureResearchRepository,
  type ResearchRepository,
} from '../domain/repository';
import type { ResearchSnapshot } from '../domain/schemas';

interface ResearchContextValue {
  readonly snapshot: ResearchSnapshot | null;
  readonly loading: boolean;
  readonly error: string | null;
  readonly importFile: (file: File) => Promise<void>;
  readonly resetDataset: () => Promise<void>;
}

const ResearchContext = createContext<ResearchContextValue | null>(null);
const publicSampleRepository = new PublicSampleResearchRepository();
const syntheticFixtureRepository = new SyntheticFixtureResearchRepository();

export function createDefaultRepository(dataMode: string | undefined): ResearchRepository {
  if (dataMode === 'synthetic') return syntheticFixtureRepository;
  return dataMode === 'fixture' || dataMode === 'public-sample'
    ? publicSampleRepository
    : new ApiResearchRepository('/api/v1', publicSampleRepository);
}

const defaultRepository = createDefaultRepository(import.meta.env.VITE_DATA_MODE);

interface ResearchProviderProps {
  readonly children: ReactNode;
  readonly repository?: ResearchRepository;
}

export function ResearchProvider({
  children,
  repository = defaultRepository,
}: ResearchProviderProps) {
  const [snapshot, setSnapshot] = useState<ResearchSnapshot | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setSnapshot(await repository.load());
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Unable to load research data.');
    } finally {
      setLoading(false);
    }
  }, [repository]);

  useEffect(() => {
    void load();
  }, [load]);

  const importFile = useCallback(async (file: File) => {
    if (file.size > MAX_IMPORT_BYTES) {
      throw new ImportValidationError('The selected file is larger than the 10 MB demo limit.');
    }
    const records = parseExportText(await file.text());
    setSnapshot({
      records,
      runs: [],
      delivery: 'local',
      provenance: {
        kind: 'local-import',
        datasetLabel: file.name,
        datasetDescription: 'A schema-compatible export selected explicitly in this browser.',
        synthetic: false,
        publicSample: false,
        sourceUrl: null,
        datasetVersion: null,
        sourceNote: 'Origin and research meaning are not independently verified by MineralLens.',
      },
      notice: 'Local preview: this file stays in your browser and is not uploaded.',
    });
    setError(null);
  }, []);

  const value = useMemo<ResearchContextValue>(
    () => ({ snapshot, loading, error, importFile, resetDataset: load }),
    [snapshot, loading, error, importFile, load],
  );
  return <ResearchContext.Provider value={value}>{children}</ResearchContext.Provider>;
}

export function useResearch(): ResearchContextValue {
  const value = useContext(ResearchContext);
  if (!value) throw new Error('useResearch must be used inside ResearchProvider');
  return value;
}
