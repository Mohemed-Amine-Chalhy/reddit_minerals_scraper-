/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_BASE_PATH?: string;
  readonly VITE_DATA_MODE?: 'api' | 'fixture' | 'public-sample' | 'synthetic';
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
