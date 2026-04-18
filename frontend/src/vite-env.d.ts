/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_PIPELINE_URL: string;
  readonly VITE_QUERY_URL: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
