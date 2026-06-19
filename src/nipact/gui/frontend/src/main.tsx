import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React, { Suspense, lazy } from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, NavLink, Route, Routes, useLocation } from "react-router-dom";
import { queryRetryDelay, shouldRetryQueryFailure } from "./api/queryRetry";
import { AppErrorBoundary } from "./components/ui/AppErrorBoundary";
import { LoadingPanel } from "./components/ui/LoadingPanel";
import "./styles.css";

const OverviewPage = lazy(async () => ({
  default: (await import("./pages/OverviewPage")).OverviewPage,
}));
const WorkflowsPage = lazy(async () => ({
  default: (await import("./pages/WorkflowsPage")).WorkflowsPage,
}));
const ArtifactsPage = lazy(async () => ({
  default: (await import("./pages/ArtifactsPage")).ArtifactsPage,
}));
const ArtifactDetailPage = lazy(async () => ({
  default: (await import("./pages/ArtifactDetailPage")).ArtifactDetailPage,
}));
const LineagePage = lazy(async () => ({
  default: (await import("./pages/LineagePage")).LineagePage,
}));
const ManifestsPage = lazy(async () => ({
  default: (await import("./pages/ManifestsPage")).ManifestsPage,
}));
const ManifestDetailPage = lazy(async () => ({
  default: (await import("./pages/ManifestDetailPage")).ManifestDetailPage,
}));

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: shouldRetryQueryFailure,
      retryDelay: queryRetryDelay,
      staleTime: 30_000,
    },
  },
});

function AppShell({ children }: { children: React.ReactNode }) {
  const navItems = [
    { to: "/", label: "Overview", end: true },
    { to: "/workflows", label: "Workflows", end: false },
    { to: "/artifacts", label: "Artifacts", end: false },
    { to: "/manifests", label: "Manifests", end: false },
  ] as const;

  return (
    <main className="layout">
      <header className="app-header">
        <NavLink to="/" end className="brand">
          NIPACT
        </NavLink>
        <nav aria-label="Primary" className="nav">
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) =>
                isActive ? "nav-link nav-link--active" : "nav-link"
              }
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
      </header>
      {children}
    </main>
  );
}

function NotFoundPage() {
  return (
    <section className="panel panel--muted">
      <h1>Not Found</h1>
      <p>Unknown GUI route.</p>
    </section>
  );
}

function RoutedContent() {
  const location = useLocation();
  return (
    <Suspense fallback={<LoadingPanel label="Loading page" />}>
      <AppErrorBoundary resetKey={location.pathname}>
        <Routes location={location}>
          <Route path="/" element={<OverviewPage />} />
          <Route path="/workflows" element={<WorkflowsPage />} />
          <Route path="/artifacts" element={<ArtifactsPage />} />
          <Route path="/artifacts/:artifactId" element={<ArtifactDetailPage />} />
          <Route path="/artifacts/:artifactId/lineage" element={<LineagePage />} />
          <Route path="/manifests" element={<ManifestsPage />} />
          <Route path="/manifests/:manifestName" element={<ManifestDetailPage />} />
          <Route path="*" element={<NotFoundPage />} />
        </Routes>
      </AppErrorBoundary>
    </Suspense>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AppShell>
          <RoutedContent />
        </AppShell>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
