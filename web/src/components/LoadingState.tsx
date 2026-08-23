export function LoadingState() {
  return (
    <div className="loading-state" role="status">
      <span className="loading-gem" aria-hidden="true" />
      <span>Preparing the research workspace…</span>
    </div>
  );
}

export function ErrorState({ message }: { readonly message: string }) {
  return (
    <div className="error-state" role="alert">
      <strong>Data workspace unavailable</strong>
      <span>{message}</span>
    </div>
  );
}
