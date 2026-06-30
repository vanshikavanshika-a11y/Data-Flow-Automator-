# Data-Flow-Automator-
import { pgTable, text, serial, timestamp, integer, jsonb } from "drizzle-orm/pg-core";
export const pipelinesTable = pgTable("pipelines", {
  id:           serial("id").primaryKey(),
  name:         text("name").notNull(),
  description:  text("description"),
  status:       text("status").notNull().default("draft"),
  schedule:     text("schedule"),
  steps:        jsonb("steps").notNull().default([]),
  successCount: integer("success_count").notNull().default(0),
  failureCount: integer("failure_count").notNull().default(0),
  lastRunAt:    timestamp("last_run_at", { withTimezone: true }),
  createdAt:    timestamp("created_at",  { withTimezone: true }).notNull().defaultNow(),
  updatedAt:    timestamp("updated_at",  { withTimezone: true }).notNull().defaultNow(),
});

lib/db/src/schema/pipelineRuns.ts
import { pgTable, text, serial, timestamp, integer, jsonb } from "drizzle-orm/pg-core";
import { pipelinesTable } from "./pipelines";
export const pipelineRunsTable = pgTable("pipeline_runs", {
  id:            serial("id").primaryKey(),
  pipelineId:    integer("pipeline_id").notNull().references(() => pipelinesTable.id, { onDelete: "cascade" }),
  pipelineName:  text("pipeline_name").notNull(),
  status:        text("status").notNull().default("queued"),
  triggeredBy:   text("triggered_by").notNull().default("manual"),
  startedAt:     timestamp("started_at",  { withTimezone: true }),
  finishedAt:    timestamp("finished_at", { withTimezone: true }),
  durationMs:    integer("duration_ms"),
  rowsProcessed: integer("rows_processed"),
  errorMessage:  text("error_message"),
  stepLogs:      jsonb("step_logs").notNull().default([]),
  createdAt:     timestamp("created_at",  { withTimezone: true }).notNull().defaultNow(),
});

BACKEND — Express API Routes
artifacts/api-server/src/routes/pipelines.ts
import { Router, type IRouter } from "express";
import { eq, desc } from "drizzle-orm";
import { db, pipelinesTable, pipelineRunsTable } from "@workspace/db";
const router: IRouter = Router();
// GET /pipelines — list all
router.get("/pipelines", async (_req, res): Promise<void> => {
  const rows = await db.select().from(pipelinesTable).orderBy(desc(pipelinesTable.createdAt));
  res.json(rows.map(fmt));
});
// POST /pipelines — create
router.post("/pipelines", async (req, res): Promise<void> => {
  const { name, description, status, schedule, steps } = req.body;
  const [p] = await db.insert(pipelinesTable).values({ name, description, status, schedule, steps }).returning();
  res.status(201).json(fmt(p));
});
// GET /pipelines/:id — get one
router.get("/pipelines/:id", async (req, res): Promise<void> => {
  const id = parseInt(req.params.id, 10);
  const [p] = await db.select().from(pipelinesTable).where(eq(pipelinesTable.id, id));
  if (!p) { res.status(404).json({ error: "Not found" }); return; }
  res.json(fmt(p));
});
// PATCH /pipelines/:id — update
router.patch("/pipelines/:id", async (req, res): Promise<void> => {
  const id = parseInt(req.params.id, 10);
  const [p] = await db.update(pipelinesTable).set(req.body).where(eq(pipelinesTable.id, id)).returning();
  if (!p) { res.status(404).json({ error: "Not found" }); return; }
  res.json(fmt(p));
});
// DELETE /pipelines/:id — delete
router.delete("/pipelines/:id", async (req, res): Promise<void> => {
  const id = parseInt(req.params.id, 10);
  await db.delete(pipelinesTable).where(eq(pipelinesTable.id, id));
  res.sendStatus(204);
});
// POST /pipelines/:id/trigger — start a run
router.post("/pipelines/:id/trigger", async (req, res): Promise<void> => {
  const id = parseInt(req.params.id, 10);
  const [pipeline] = await db.select().from(pipelinesTable).where(eq(pipelinesTable.id, id));
  if (!pipeline) { res.status(404).json({ error: "Not found" }); return; }
  const steps = (pipeline.steps as Array<{ name: string; type: string }>) ?? [];
  const stepLogs = steps.map(s => ({
    stepName: s.name, status: "pending",
    startedAt: null, finishedAt: null, message: null,
  }));
  const [run] = await db.insert(pipelineRunsTable).values({
    pipelineId: pipeline.id, pipelineName: pipeline.name,
    status: "running", triggeredBy: "manual",
    startedAt: new Date(), stepLogs,
  }).returning();
  simulatePipelineRun(pipeline.id, run.id, steps).catch(() => {});
  res.status(201).json(fmtRun(run));
});
// GET /pipelines/:id/runs — runs for a pipeline
router.get("/pipelines/:id/runs", async (req, res): Promise<void> => {
  const id = parseInt(req.params.id, 10);
  const runs = await db.select().from(pipelineRunsTable)
    .where(eq(pipelineRunsTable.pipelineId, id))
    .orderBy(desc(pipelineRunsTable.createdAt));
  res.json(runs.map(fmtRun));
});
// Helpers
function fmt(p: typeof pipelinesTable.$inferSelect) {
  return {
    ...p,
    steps: (p.steps as unknown[]) ?? [],
    lastRunAt: p.lastRunAt?.toISOString() ?? null,
    createdAt: p.createdAt.toISOString(),
    updatedAt: p.updatedAt.toISOString(),
  };
}
function fmtRun(r: typeof pipelineRunsTable.$inferSelect) {
  return {
    ...r,
    stepLogs: (r.stepLogs as unknown[]) ?? [],
    startedAt: r.startedAt?.toISOString() ?? null,
    finishedAt: r.finishedAt?.toISOString() ?? null,
    createdAt: r.createdAt.toISOString(),
  };
}
// Simulates step-by-step pipeline execution in the background
async function simulatePipelineRun(
  pipelineId: number, runId: number,
  steps: Array<{ name: string; type: string }>
) {
  const delay = (ms: number) => new Promise(r => setTimeout(r, ms));
  const logs = steps.map(s => ({
    stepName: s.name, status: "pending",
    startedAt: null as string | null,
    finishedAt: null as string | null,
    message: null as string | null,
  }));
  let totalRows = 0, failed = false, errorMessage: string | null = null;
  for (let i = 0; i < steps.length; i++) {
    const d = 800 + Math.random() * 1200;
    await delay(d);
    logs[i].status = "running";
    logs[i].startedAt = new Date().toISOString();
    await db.update(pipelineRunsTable).set({ stepLogs: logs }).where(eq(pipelineRunsTable.id, runId));
    await delay(d);
    if (Math.random() < 0.08) {
      logs[i].status = "failed";
      logs[i].finishedAt = new Date().toISOString();
      logs[i].message = `Unexpected data format in ${steps[i].name}`;
      failed = true;
      errorMessage = `Failed at "${steps[i].name}"`;
      for (let j = i + 1; j < steps.length; j++) logs[j].status = "skipped";
      break;
    } else {
      const rows = Math.floor(Math.random() * 50000) + 1000;
      totalRows += rows;
      logs[i].status = "success";
      logs[i].finishedAt = new Date().toISOString();
      logs[i].message = `Processed ${rows.toLocaleString()} rows`;
    }
  }
  const now = new Date();
  const durationMs = now.getTime() - new Date(logs[0]?.startedAt ?? now).getTime();
  await db.update(pipelineRunsTable).set({
    status: failed ? "failed" : "success",
    finishedAt: now, durationMs,
    rowsProcessed: failed ? null : totalRows,
    errorMessage, stepLogs: logs,
  }).where(eq(pipelineRunsTable.id, runId));
  const [cur] = await db.select({ s: pipelinesTable.successCount, f: pipelinesTable.failureCount })
    .from(pipelinesTable).where(eq(pipelinesTable.id, pipelineId));
  if (cur) {
    await db.update(pipelinesTable).set({
      successCount: failed ? cur.s : cur.s + 1,
      failureCount: failed ? cur.f + 1 : cur.f,
      lastRunAt: now,
    }).where(eq(pipelinesTable.id, pipelineId));
  }
}
export default router;

artifacts/api-server/src/routes/runs.ts
import { Router, type IRouter } from "express";
import { eq, desc } from "drizzle-orm";
import { db, pipelineRunsTable } from "@workspace/db";
const router: IRouter = Router();
function fmt(r: typeof pipelineRunsTable.$inferSelect) {
  return {
    ...r,
    stepLogs: (r.stepLogs as unknown[]) ?? [],
    startedAt:  r.startedAt?.toISOString()  ?? null,
    finishedAt: r.finishedAt?.toISOString() ?? null,
    createdAt:  r.createdAt.toISOString(),
  };
}
// GET /runs
router.get("/runs", async (_req, res): Promise<void> => {
  const runs = await db.select().from(pipelineRunsTable)
    .orderBy(desc(pipelineRunsTable.createdAt)).limit(100);
  res.json(runs.map(fmt));
});
// GET /runs/:id
router.get("/runs/:id", async (req, res): Promise<void> => {
  const id = parseInt(req.params.id, 10);
  const [run] = await db.select().from(pipelineRunsTable).where(eq(pipelineRunsTable.id, id));
  if (!run) { res.status(404).json({ error: "Not found" }); return; }
  res.json(fmt(run));
});
// POST /runs/:id/cancel
router.post("/runs/:id/cancel", async (req, res): Promise<void> => {
  const id = parseInt(req.params.id, 10);
  const [run] = await db.select().from(pipelineRunsTable).where(eq(pipelineRunsTable.id, id));
  if (!run) { res.status(404).json({ error: "Not found" }); return; }
  const now = new Date();
  const durationMs = now.getTime() - (run.startedAt ?? now).getTime();
  const [updated] = await db.update(pipelineRunsTable)
    .set({ status: "cancelled", finishedAt: now, durationMs })
    .where(eq(pipelineRunsTable.id, id)).returning();
  res.json(fmt(updated));
});
export default router;

artifacts/api-server/src/routes/dashboard.ts
import { Router, type IRouter } from "express";
import { desc, sql, gte } from "drizzle-orm";
import { db, pipelinesTable, pipelineRunsTable } from "@workspace/db";
const router: IRouter = Router();
router.get("/dashboard/summary", async (_req, res): Promise<void> => {
  const todayStart = new Date();
  todayStart.setHours(0, 0, 0, 0);
  const [ps] = await db.select({
    total:  sql<number>`count(*)::int`,
    active: sql<number>`count(*) filter (where status = 'active')::int`,
  }).from(pipelinesTable);
  const rs = await db.select({
    status:    pipelineRunsTable.status,
    count:     sql<number>`count(*)::int`,
    totalRows: sql<number>`coalesce(sum(rows_processed), 0)::int`,
  }).from(pipelineRunsTable)
    .where(gte(pipelineRunsTable.createdAt, todayStart))
    .groupBy(pipelineRunsTable.status);
  let total = 0, success = 0, failed = 0, running = 0, rows = 0;
  for (const r of rs) {
    total += r.count;
    if (r.status === "success") { success += r.count; rows += r.totalRows; }
    if (r.status === "failed")  failed  += r.count;
    if (r.status === "running" || r.status === "queued") running += r.count;
  }
  res.json({
    totalPipelines:          ps?.total  ?? 0,
    activePipelines:         ps?.active ?? 0,
    totalRunsToday:          total,
    successRatePercent:      total > 0 ? Math.round((success / total) * 100) : 0,
    runningNow:              running,
    failedToday:             failed,
    totalRowsProcessedToday: rows,
  });
});
router.get("/dashboard/recent-runs", async (_req, res): Promise<void> => {
  const runs = await db.select().from(pipelineRunsTable)
    .orderBy(desc(pipelineRunsTable.createdAt)).limit(20);
  res.json(runs.map(r => ({
    ...r,
    stepLogs:   (r.stepLogs as unknown[]) ?? [],
    startedAt:  r.startedAt?.toISOString()  ?? null,
    finishedAt: r.finishedAt?.toISOString() ?? null,
    createdAt:  r.createdAt.toISOString(),
  })));
});
export default router;

FRONTEND — React Pages & Components
src/components/layout/AppLayout.tsx
import { Sidebar } from "./Sidebar";
import { ReactNode } from "react";
export function AppLayout({ children }: { children: ReactNode }) {
  return (
    <div className="flex h-screen overflow-hidden bg-background">
      <Sidebar />
      <main className="flex-1 overflow-y-auto overflow-x-hidden">{children}</main>
    </div>
  );
}

src/components/layout/Sidebar.tsx
import { Link, useLocation } from "wouter";
import { Activity, LayoutDashboard, Settings, PlaySquare } from "lucide-react";
import { cn } from "@/lib/utils";
export function Sidebar() {
  const [location] = useLocation();
  const links = [
    { href: "/",          label: "Dashboard", icon: LayoutDashboard },
    { href: "/pipelines", label: "Pipelines", icon: Settings },
    { href: "/runs",      label: "Runs",      icon: Activity },
  ];
  return (
    <div className="flex h-full w-64 flex-col bg-sidebar border-r border-sidebar-border">
      <div className="p-4 flex items-center gap-2 border-b border-sidebar-border">
        <div className="bg-primary text-primary-foreground p-1.5 rounded-md">
          <PlaySquare className="w-5 h-5" />
        </div>
        <span className="font-semibold text-sidebar-foreground">FlowForge</span>
      </div>
      <nav className="flex-1 p-4 space-y-1">
        {links.map(({ href, label, icon: Icon }) => {
          const active = location === href || (href !== "/" && location.startsWith(href));
          return (
            <Link key={href} href={href} className={cn(
              "flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-colors",
              active
                ? "bg-sidebar-accent text-sidebar-accent-foreground"
                : "text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground"
            )}>
              <Icon className="w-4 h-4" />{label}
            </Link>
          );
        })}
      </nav>
      <div className="p-4 border-t border-sidebar-border text-xs text-sidebar-foreground/40">v1.0.0</div>
    </div>
  );
}

src/components/ui/status-badge.tsx
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { CheckCircle2, Clock, XCircle, PlayCircle, StopCircle, Edit3 } from "lucide-react";
export function RunStatusBadge({ status, className }: { status: string; className?: string }) {
  const map: Record<string, { cls: string; icon: JSX.Element; label: string }> = {
    success:   { cls: "bg-emerald-500/10 text-emerald-600 border-emerald-500/20",                      icon: <CheckCircle2 className="w-3 h-3"/>, label: "Success"   },
    failed:    { cls: "bg-red-500/10    text-red-600    border-red-500/20",                            icon: <XCircle      className="w-3 h-3"/>, label: "Failed"    },
    running:   { cls: "bg-amber-500/10  text-amber-600  border-amber-500/20  animate-pulse",           icon: <PlayCircle   className="w-3 h-3"/>, label: "Running"   },
    queued:    { cls: "bg-blue-500/10   text-blue-600   border-blue-500/20",                           icon: <Clock        className="w-3 h-3"/>, label: "Queued"    },
    cancelled: { cls: "bg-slate-500/10  text-slate-600  border-slate-500/20",                          icon: <StopCircle   className="w-3 h-3"/>, label: "Cancelled" },
  };
  const s = map[status];
  if (!s) return <Badge variant="outline">{status}</Badge>;
  return <Badge variant="outline" className={cn("gap-1 font-medium", s.cls, className)}>{s.icon} {s.label}</Badge>;
}
export function PipelineStatusBadge({ status, className }: { status: string; className?: string }) {
  const map: Record<string, { cls: string; icon: JSX.Element; label: string }> = {
    active: { cls: "bg-emerald-500/10 text-emerald-600 border-emerald-500/20", icon: <PlayCircle className="w-3 h-3"/>, label: "Active" },
    paused: { cls: "bg-amber-500/10  text-amber-600  border-amber-500/20",     icon: <StopCircle className="w-3 h-3"/>, label: "Paused" },
    draft:  { cls: "bg-slate-500/10  text-slate-600  border-slate-500/20",     icon: <Edit3      className="w-3 h-3"/>, label: "Draft"  },
  };
  const s = map[status];
  if (!s) return <Badge variant="outline">{status}</Badge>;
  return <Badge variant="outline" className={cn("gap-1 font-medium", s.cls, className)}>{s.icon} {s.label}</Badge>;
}

src/pages/Dashboard.tsx
import { useGetDashboardSummary, getGetDashboardSummaryQueryKey, useGetRecentRuns, getGetRecentRunsQueryKey } from "@workspace/api-client-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Activity, CheckCircle2, AlertCircle, Database, PlayCircle, Layers } from "lucide-react";
import { RunStatusBadge } from "@/components/ui/status-badge";
import { Link, useLocation } from "wouter";
export function Dashboard() {
  const [, setLocation] = useLocation();
  const { data: summary } = useGetDashboardSummary({ query: { queryKey: getGetDashboardSummaryQueryKey() } });
  const { data: recentRuns } = useGetRecentRuns({ query: { queryKey: getGetRecentRunsQueryKey() } });
  if (!summary) return <div className="p-8">Loading...</div>;
  return (
    <div className="p-8 space-y-8">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Mission Control</h1>
        <p className="text-muted-foreground mt-1">Live overview of your automated pipelines.</p>
      </div>
      {/* Stat Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-6 gap-4">
        {[
          { title: "Total Pipelines", value: summary.totalPipelines,          icon: Layers,       cls: "" },
          { title: "Active",          value: summary.activePipelines,          icon: PlayCircle,   cls: "text-primary" },
          { title: "Runs Today",      value: summary.totalRunsToday,           icon: Activity,     cls: "" },
          { title: "Success Rate",    value: `${summary.successRatePercent}%`, icon: CheckCircle2, cls: "text-emerald-500" },
          { title: "Running Now",     value: summary.runningNow,               icon: PlayCircle,   cls: "text-amber-500" },
          { title: "Failed Today",    value: summary.failedToday,              icon: AlertCircle,  cls: "text-destructive" },
        ].map(({ title, value, icon: Icon, cls }) => (
          <Card key={title}>
            <CardContent className="p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-sm font-medium text-muted-foreground">{title}</h3>
                <Icon className={`w-4 h-4 text-muted-foreground ${cls}`} />
              </div>
              <div className={`text-2xl font-bold ${cls || "text-foreground"}`}>{value}</div>
            </CardContent>
          </Card>
        ))}
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Recent Runs */}
        <Card className="lg:col-span-2">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="flex items-center gap-2 text-lg">
              <Activity className="w-5 h-5 text-primary" /> Recent Activity
            </CardTitle>
            <Link href="/runs" className="text-sm text-muted-foreground hover:text-foreground">View all runs</Link>
          </CardHeader>
          <CardContent className="p-0">
            <table className="w-full">
              <thead><tr className="bg-muted/30 text-left">
                <th className="px-6 py-3 text-xs font-medium text-muted-foreground">Pipeline</th>
                <th className="py-3 text-xs font-medium text-muted-foreground">Status</th>
                <th className="py-3 text-xs font-medium text-muted-foreground">Duration</th>
                <th className="py-3 text-xs font-medium text-muted-foreground">Time</th>
              </tr></thead>
              <tbody>
                {recentRuns?.slice(0, 5).map(run => (
                  <tr key={run.id} className="border-t border-border hover:bg-muted/30 cursor-pointer"
                      onClick={() => setLocation(`/runs/${run.id}`)}>
                    <td className="px-6 py-3 font-medium text-sm">{run.pipelineName}</td>
                    <td className="py-3"><RunStatusBadge status={run.status} /></td>
                    <td className="py-3 text-sm text-muted-foreground">{run.durationMs ? `${(run.durationMs/1000).toFixed(1)}s` : '-'}</td>
                    <td className="py-3 text-sm text-muted-foreground">{new Date(run.createdAt).toLocaleTimeString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </CardContent>
        </Card>
        {/* Rows Counter */}
        <Card className="flex flex-col justify-center text-center">
          <CardHeader>
            <CardTitle className="flex items-center justify-center gap-2 text-lg">
              <Database className="w-5 h-5 text-primary" /> Data Processed Today
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-6xl font-mono font-medium tracking-tighter mb-2">
              {summary.totalRowsProcessedToday.toLocaleString()}
            </div>
            <p className="text-sm text-muted-foreground">rows extracted, transformed, and loaded.</p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

src/pages/pipelines/pipeline-list.tsx
import { useState } from "react";
import { useListPipelines, getListPipelinesQueryKey, useTriggerPipeline } from "@workspace/api-client-react";
import { useQueryClient } from "@tanstack/react-query";
import { PipelineStatusBadge } from "@/components/ui/status-badge";
import { Link, useLocation } from "wouter";
import { Play, Plus, Search, Calendar } from "lucide-react";
export function PipelineList() {
  const [search, setSearch] = useState("");
  const [, setLocation] = useLocation();
  const { data: pipelines, isLoading } = useListPipelines();
  const trigger = useTriggerPipeline();
  const qc = useQueryClient();
  const filtered = pipelines?.filter(p =>
    p.name.toLowerCase().includes(search.toLowerCase()) ||
    p.description?.toLowerCase().includes(search.toLowerCase())
  );
  return (
    <div className="p-8 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Pipelines</h1>
          <p className="text-muted-foreground mt-1">Manage and monitor your data pipelines.</p>
        </div>
        <Link href="/pipelines/new" className="flex items-center gap-2 bg-primary text-primary-foreground rounded-md px-4 py-2 text-sm font-medium hover:bg-primary/90">
          <Plus className="w-4 h-4" /> New Pipeline
        </Link>
      </div>
      <div className="border border-border rounded-lg overflow-hidden">
        <div className="p-4 border-b border-border flex items-center gap-2 relative">
          <Search className="absolute left-7 text-muted-foreground w-4 h-4" />
          <input
            className="pl-8 pr-4 py-1.5 bg-muted/50 rounded-md text-sm outline-none w-72"
            placeholder="Search pipelines..."
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
        </div>
        {isLoading ? (
          <div className="p-8 text-center text-muted-foreground text-sm">Loading...</div>
        ) : (
          <table className="w-full">
            <thead><tr className="bg-muted/30 text-left">
              <th className="px-6 py-3 text-xs font-medium text-muted-foreground">Name</th>
              <th className="py-3 text-xs font-medium text-muted-foreground">Status</th>
              <th className="py-3 text-xs font-medium text-muted-foreground">Schedule</th>
              <th className="py-3 text-xs font-medium text-muted-foreground">Last Run</th>
              <th className="py-3 pr-6 text-xs font-medium text-muted-foreground text-right">Actions</th>
            </tr></thead>
            <tbody>
              {filtered?.map(p => (
                <tr key={p.id} className="border-t border-border hover:bg-muted/30 cursor-pointer"
                    onClick={() => setLocation(`/pipelines/${p.id}`)}>
                  <td className="px-6 py-4">
                    <div className="font-medium text-sm">{p.name}</div>
                    <div className="text-xs text-muted-foreground">{p.description}</div>
                  </td>
                  <td className="py-4"><PipelineStatusBadge status={p.status} /></td>
                  <td className="py-4 text-sm text-muted-foreground">
                    {p.schedule
                      ? <span className="flex items-center gap-1"><Calendar className="w-3.5 h-3.5" /><code className="bg-muted px-1.5 py-0.5 rounded text-xs">{p.schedule}</code></span>
                      : "Manual"}
                  </td>
                  <td className="py-4 text-sm text-muted-foreground">
                    {p.lastRunAt ? new Date(p.lastRunAt).toLocaleString() : "Never"}
                  </td>
                  <td className="py-4 pr-6 text-right">
                    <button
                      className="inline-flex items-center gap-1 text-xs px-3 py-1.5 rounded border border-border hover:bg-muted transition-colors"
                      onClick={e => { e.stopPropagation(); trigger.mutate({ id: p.id }, { onSuccess: () => qc.invalidateQueries({ queryKey: getListPipelinesQueryKey() }) }); }}
                      disabled={trigger.isPending}
                    >
                      <Play className="w-3 h-3" /> Trigger
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

src/pages/pipelines/pipeline-form.tsx
import { useForm, useFieldArray } from "react-hook-form";
import { z } from "zod";
import { zodResolver } from "@hookform/resolvers/zod";
import { useCreatePipeline } from "@workspace/api-client-react";
import { useLocation } from "wouter";
import { Plus, Trash2, ArrowLeft } from "lucide-react";
import { Link } from "wouter";
const schema = z.object({
  name:        z.string().min(1, "Required"),
  description: z.string().optional(),
  status:      z.enum(["active","paused","draft"]).default("draft"),
  schedule:    z.string().optional(),
  steps: z.array(z.object({
    name:   z.string().min(1, "Required"),
    type:   z.enum(["extract","transform","load","validate","notify"]),
    config: z.string().optional(),
  })).min(1, "At least one step required"),
});
type Form = z.infer<typeof schema>;
export function PipelineForm() {
  const [, setLocation] = useLocation();
  const create = useCreatePipeline();
  const { register, control, handleSubmit, formState: { errors } } = useForm<Form>({
    resolver: zodResolver(schema),
    defaultValues: { name: "", description: "", status: "draft", schedule: "", steps: [{ name: "Extract Data", type: "extract", config: "" }] },
  });
  const { fields, append, remove } = useFieldArray({ control, name: "steps" });
  function onSubmit(data: Form) {
    create.mutate({ data }, {
      onSuccess: (p) => setLocation(`/pipelines/${p.id}`),
    });
  }
  return (
    <div className="p-8 max-w-4xl mx-auto space-y-6">
      <div className="flex items-center gap-4">
        <Link href="/pipelines" className="p-2 rounded-md hover:bg-muted"><ArrowLeft className="w-4 h-4" /></Link>
        <div>
          <h1 className="text-3xl font-bold tracking-tight">New Pipeline</h1>
          <p className="text-muted-foreground mt-1">Configure a new automated data pipeline.</p>
        </div>
      </div>
      <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
        {/* General Settings */}
        <div className="border border-border rounded-lg p-6 space-y-4">
          <h2 className="font-semibold text-lg">General Settings</h2>
          <div>
            <label className="text-sm font-medium">Pipeline Name *</label>
            <input {...register("name")} className="mt-1 w-full border border-border rounded-md px-3 py-2 text-sm bg-background" placeholder="e.g. Daily Sales ETL" />
            {errors.name && <p className="text-destructive text-xs mt-1">{errors.name.message}</p>}
          </div>
          <div>
            <label className="text-sm font-medium">Description</label>
            <textarea {...register("description")} className="mt-1 w-full border border-border rounded-md px-3 py-2 text-sm bg-background" rows={2} placeholder="What does this pipeline do?" />
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="text-sm font-medium">Status</label>
              <select {...register("status")} className="mt-1 w-full border border-border rounded-md px-3 py-2 text-sm bg-background">
                <option value="draft">Draft</option>
                <option value="active">Active</option>
                <option value="paused">Paused</option>
              </select>
            </div>
            <div>
              <label className="text-sm font-medium">Schedule (Cron)</label>
              <input {...register("schedule")} className="mt-1 w-full border border-border rounded-md px-3 py-2 text-sm bg-background" placeholder="0 0 * * * (optional)" />
            </div>
          </div>
        </div>
        {/* Steps */}
        <div className="border border-border rounded-lg p-6 space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="font-semibold text-lg">Pipeline Steps</h2>
            <button type="button" onClick={() => append({ name: "", type: "transform", config: "" })}
              className="flex items-center gap-1 text-sm border border-border rounded-md px-3 py-1.5 hover:bg-muted">
              <Plus className="w-4 h-4" /> Add Step
            </button>
          </div>
          {fields.map((field, i) => (
            <div key={field.id} className="border border-border rounded-lg p-4 bg-muted/20 space-y-3">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="text-sm font-medium">Step Name *</label>
                  <input {...register(`steps.${i}.name`)} className="mt-1 w-full border border-border rounded-md px-3 py-2 text-sm bg-background" placeholder="e.g. Fetch API Data" />
                </div>
                <div>
                  <label className="text-sm font-medium">Type *</label>
                  <select {...register(`steps.${i}.type`)} className="mt-1 w-full border border-border rounded-md px-3 py-2 text-sm bg-background">
                    {["extract","transform","load","validate","notify"].map(t => <option key={t} value={t}>{t.charAt(0).toUpperCase() + t.slice(1)}</option>)}
                  </select>
                </div>
              </div>
              <div className="flex gap-2">
                <div className="flex-1">
                  <label className="text-sm font-medium">Config (JSON/Text)</label>
                  <textarea {...register(`steps.${i}.config`)} className="mt-1 w-full border border-border rounded-md px-3 py-2 text-sm bg-background font-mono" rows={2} placeholder='{"endpoint": "..."}' />
                </div>
                <button type="button" onClick={() => remove(i)} disabled={fields.length === 1}
                  className="mt-6 p-2 text-destructive hover:bg-destructive/10 rounded-md disabled:opacity-30">
                  <Trash2 className="w-4 h-4" />
                </button>
              </div>
            </div>
          ))}
        </div>
        <div className="flex justify-end gap-4">
          <button type="button" onClick={() => setLocation("/pipelines")} className="px-4 py-2 border border-border rounded-md text-sm hover:bg-muted">Cancel</button>
          <button type="submit" disabled={create.isPending} className="px-4 py-2 bg-primary text-primary-foreground rounded-md text-sm font-medium hover:bg-primary/90 disabled:opacity-50">
            {create.isPending ? "Creating..." : "Create Pipeline"}
          </button>
        </div>
      </form>
    </div>
  );
}

src/pages/pipelines/pipeline-detail.tsx
import { useGetPipeline, getGetPipelineQueryKey, useListPipelineRuns, useTriggerPipeline, useDeletePipeline } from "@workspace/api-client-react";
import { useQueryClient } from "@tanstack/react-query";
import { useParams, useLocation, Link } from "wouter";
import { PipelineStatusBadge, RunStatusBadge } from "@/components/ui/status-badge";
import { Play, ArrowLeft, Trash2, Settings } from "lucide-react";
export function PipelineDetail() {
  const { id } = useParams();
  const pipelineId = parseInt(id || "0", 10);
  const [, setLocation] = useLocation();
  const qc = useQueryClient();
  const { data: pipeline } = useGetPipeline(pipelineId, { query: { enabled: !!pipelineId, queryKey: getGetPipelineQueryKey(pipelineId) } });
  const { data: runs } = useListPipelineRuns(pipelineId, { query: { enabled: !!pipelineId } });
  const trigger = useTriggerPipeline();
  const del = useDeletePipeline();
  if (!pipeline) return <div className="p-8 text-center text-muted-foreground">Loading...</div>;
  return (
    <div className="p-8 space-y-6 max-w-5xl mx-auto">
      <div className="flex justify-between items-start">
        <div className="flex items-center gap-4">
          <Link href="/pipelines" className="p-2 rounded-md hover:bg-muted"><ArrowLeft className="w-4 h-4" /></Link>
          <div>
            <div className="flex items-center gap-3">
              <h1 className="text-3xl font-bold tracking-tight">{pipeline.name}</h1>
              <PipelineStatusBadge status={pipeline.status} />
            </div>
            <p className="text-muted-foreground mt-1">{pipeline.description || "No description."}</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Link href={`/pipelines/${pipeline.id}/edit`} className="flex items-center gap-1.5 text-sm px-3 py-1.5 border border-border rounded-md hover:bg-muted">
            <Settings className="w-4 h-4" /> Edit
          </Link>
          <button onClick={() => trigger.mutate({ id: pipelineId }, { onSuccess: () => qc.invalidateQueries() })}
            disabled={trigger.isPending} className="flex items-center gap-1.5 text-sm px-3 py-1.5 border border-border rounded-md hover:bg-muted">
            <Play className="w-4 h-4" /> Trigger Run
          </button>
          <button onClick={() => del.mutate({ id: pipelineId }, { onSuccess: () => setLocation("/pipelines") })}
            className="p-2 text-destructive hover:bg-destructive/10 rounded-md">
            <Trash2 className="w-4 h-4" />
          </button>
        </div>
      </div>
      {/* Steps */}
      <div className="border border-border rounded-lg p-6">
        <h2 className="font-semibold text-lg mb-4">Pipeline Steps</h2>
        <div className="flex gap-4 overflow-x-auto pb-2">
          {pipeline.steps.map((step, i) => (
            <div key={i} className="flex-shrink-0 border border-border rounded-lg p-4 w-48 bg-card">
              <div className="text-xs font-mono text-primary bg-primary/10 px-2 py-0.5 rounded inline-block mb-2">{step.type}</div>
              <div className="font-medium text-sm">{step.name}</div>
              {step.config && <pre className="text-xs text-muted-foreground mt-2 bg-muted p-2 rounded overflow-hidden">{step.config}</pre>}
            </div>
          ))}
        </div>
      </div>
      {/* Stats */}
      <div className="grid grid-cols-3 gap-4">
        <div className="border border-border rounded-lg p-4 text-center">
          <div className="text-2xl font-bold text-emerald-600">{pipeline.successCount}</div>
          <div className="text-sm text-muted-foreground">Successful Runs</div>
        </div>
        <div className="border border-border rounded-lg p-4 text-center">
          <div className="text-2xl font-bold text-destructive">{pipeline.failureCount}</div>
          <div className="text-sm text-muted-foreground">Failed Runs</div>
        </div>
        <div className="border border-border rounded-lg p-4 text-center">
          <div className="text-2xl font-bold">{pipeline.lastRunAt ? new Date(pipeline.lastRunAt).toLocaleDateString() : "—"}</div>
          <div className="text-sm text-muted-foreground">Last Run</div>
        </div>
      </div>
      {/* Run History */}
      <div className="border border-border rounded-lg overflow-hidden">
        <div className="p-4 border-b border-border font-semibold">Run History</div>
        <table className="w-full">
          <thead><tr className="bg-muted/30 text-left">
            <th className="px-6 py-3 text-xs font-medium text-muted-foreground">Status</th>
            <th className="py-3 text-xs font-medium text-muted-foreground">Duration</th>
            <th className="py-3 text-xs font-medium text-muted-foreground">Rows</th>
            <th className="py-3 text-xs font-medium text-muted-foreground">Triggered By</th>
            <th className="py-3 text-xs font-medium text-muted-foreground">Date</th>
          </tr></thead>
          <tbody>
            {runs?.map(run => (
              <tr key={run.id} className="border-t border-border hover:bg-muted/30 cursor-pointer" onClick={() => setLocation(`/runs/${run.id}`)}>
                <td className="px-6 py-3"><RunStatusBadge status={run.status} /></td>
                <td className="py-3 text-sm text-muted-foreground">{run.durationMs ? `${(run.durationMs/1000).toFixed(1)}s` : '—'}</td>
                <td className="py-3 text-sm text-muted-foreground">{run.rowsProcessed?.toLocaleString() || '—'}</td>
                <td className="py-3 text-sm text-muted-foreground capitalize">{run.triggeredBy}</td>
                <td className="py-3 text-sm text-muted-foreground">{new Date(run.createdAt).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

src/pages/runs/run-list.tsx
import { useState } from "react";
import { useListRuns } from "@workspace/api-client-react";
import { RunStatusBadge } from "@/components/ui/status-badge";
import { useLocation } from "wouter";
import { Filter } from "lucide-react";
export function RunList() {
  const [search, setSearch] = useState("");
  const [, setLocation] = useLocation();
  const { data: runs, isLoading } = useListRuns();
  const filtered = runs?.filter(r =>
    r.pipelineName.toLowerCase().includes(search.toLowerCase()) ||
    r.status.toLowerCase().includes(search.toLowerCase())
  );
  return (
    <div className="p-8 space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">All Runs</h1>
        <p className="text-muted-foreground mt-1">Execution history across all pipelines.</p>
      </div>
      <div className="border border-border rounded-lg overflow-hidden">
        <div className="p-4 border-b border-border flex items-center gap-2 relative">
          <Filter className="absolute left-7 text-muted-foreground w-4 h-4" />
          <input className="pl-8 pr-4 py-1.5 bg-muted/50 rounded-md text-sm outline-none w-72"
            placeholder="Filter by pipeline or status..." value={search} onChange={e => setSearch(e.target.value)} />
        </div>
        {isLoading ? (
          <div className="p-8 text-center text-muted-foreground text-sm">Loading...</div>
        ) : (
          <table className="w-full">
            <thead><tr className="bg-muted/30 text-left">
              <th className="px-6 py-3 text-xs font-medium text-muted-foreground">Pipeline</th>
              <th className="py-3 text-xs font-medium text-muted-foreground">Status</th>
              <th className="py-3 text-xs font-medium text-muted-foreground">Duration</th>
              <th className="py-3 text-xs font-medium text-muted-foreground">Rows</th>
              <th className="py-3 text-xs font-medium text-muted-foreground">Started</th>
            </tr></thead>
            <tbody>
              {filtered?.map(run => (
                <tr key={run.id} className="border-t border-border hover:bg-muted/30 cursor-pointer" onClick={() => setLocation(`/runs/${run.id}`)}>
                  <td className="px-6 py-4">
                    <div className="font-medium text-sm">{run.pipelineName}</div>
                    <div className="text-xs text-muted-foreground capitalize">Triggered: {run.triggeredBy}</div>
                  </td>
                  < **...**
