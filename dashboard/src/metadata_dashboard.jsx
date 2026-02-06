import { useState, useMemo, useEffect } from "react";
import MetadataReference from "./bq-metadata-reference.jsx";

// -- Color System --
const C = {
  bg: "#0a0e17",
  surface: "#111827",
  surfaceHover: "#1a2236",
  border: "#1e293b",
  borderAccent: "#334155",
  text: "#e2e8f0",
  textMuted: "#94a3b8",
  textDim: "#64748b",
  raw: "#ef4444",
  staging: "#f59e0b",
  analytics: "#3b82f6",
  reporting: "#8b5cf6",
  pii: "#f43f5e",
  quality: "#10b981",
  qualityWarn: "#f59e0b",
  qualityBad: "#ef4444",
  accent: "#06b6d4",
};

const layerColor = (ds) => {
  if (ds?.includes("raw")) return C.raw;
  if (ds?.includes("staging")) return C.staging;
  if (ds?.includes("analytics")) return C.analytics;
  if (ds?.includes("reporting")) return C.reporting;
  return C.textMuted;
};

const formatBytes = (bytes) => {
  if (!bytes || bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
};

const formatDate = (dateStr) => {
  if (!dateStr) return "-";
  const d = new Date(dateStr);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
};

const formatDateTime = (dateStr) => {
  if (!dateStr) return "-";
  const d = new Date(dateStr);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", hour: "numeric", minute: "2-digit" });
};

const formatDuration = (start, end) => {
  if (!start || !end) return "-";
  const ms = new Date(end) - new Date(start);
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60000).toFixed(1)}m`;
};

const truncate = (str, len) => {
  if (!str) return "-";
  return str.length > len ? str.slice(0, len) + "..." : str;
};

// -- Components --

function StatCard({ label, value, sub, color }) {
  return (
    <div style={{
      background: C.surface,
      border: `1px solid ${C.border}`,
      borderRadius: 12,
      padding: "16px 20px",
      minWidth: 130,
      position: "relative",
      overflow: "hidden",
    }}>
      <div style={{
        position: "absolute", top: 0, left: 0, right: 0, height: 3,
        background: `linear-gradient(90deg, ${color || C.accent}, transparent)`,
      }} />
      <div style={{ color: C.textMuted, fontSize: 11, fontWeight: 500, letterSpacing: "0.05em", textTransform: "uppercase", marginBottom: 6 }}>{label}</div>
      <div style={{ color: C.text, fontSize: 24, fontWeight: 700, fontFamily: "'JetBrains Mono', 'SF Mono', monospace", lineHeight: 1 }}>{value}</div>
      {sub && <div style={{ color: C.textDim, fontSize: 11, marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

function TabButton({ active, onClick, children, color }) {
  return (
    <button
      onClick={onClick}
      style={{
        background: active ? (color || C.accent) + "18" : "transparent",
        color: active ? (color || C.accent) : C.textMuted,
        border: `1px solid ${active ? (color || C.accent) + "40" : "transparent"}`,
        borderRadius: 8,
        padding: "10px 18px",
        fontSize: 13,
        fontWeight: 600,
        cursor: "pointer",
        transition: "all 0.2s",
        fontFamily: "inherit",
        letterSpacing: "0.02em",
      }}
    >
      {children}
    </button>
  );
}

function Badge({ children, color, small }) {
  return (
    <span style={{
      background: color + "20",
      color: color,
      padding: small ? "2px 6px" : "3px 10px",
      borderRadius: 6,
      fontSize: small ? 10 : 11,
      fontWeight: 600,
      letterSpacing: "0.03em",
      whiteSpace: "nowrap",
    }}>{children}</span>
  );
}

function SortableHeader({ label, sortKey, currentSort, onSort, style }) {
  const isActive = currentSort.key === sortKey;
  const arrow = isActive ? (currentSort.dir === "asc" ? " ▲" : " ▼") : "";
  return (
    <span
      onClick={() => onSort(sortKey)}
      style={{ cursor: "pointer", userSelect: "none", ...style }}
    >
      {label}{arrow}
    </span>
  );
}

function SearchBox({ value, onChange, placeholder }) {
  return (
    <input
      type="text"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder || "Search..."}
      style={{
        background: C.bg,
        border: `1px solid ${C.border}`,
        borderRadius: 8,
        padding: "8px 14px",
        fontSize: 13,
        color: C.text,
        width: 220,
        fontFamily: "inherit",
      }}
    />
  );
}

function LayerFilter({ value, onChange }) {
  const layers = [
    { key: null, label: "All" },
    { key: "raw", label: "Raw", color: C.raw },
    { key: "staging", label: "Staging", color: C.staging },
    { key: "analytics", label: "Analytics", color: C.analytics },
    { key: "reporting", label: "Reporting", color: C.reporting },
  ];
  return (
    <div style={{ display: "flex", gap: 4 }}>
      {layers.map(l => (
        <button
          key={l.key || "all"}
          onClick={() => onChange(l.key)}
          style={{
            background: value === l.key ? (l.color || C.accent) + "20" : "transparent",
            color: value === l.key ? (l.color || C.accent) : C.textDim,
            border: `1px solid ${value === l.key ? (l.color || C.accent) + "40" : C.border}`,
            borderRadius: 6,
            padding: "6px 12px",
            fontSize: 11,
            fontWeight: 600,
            cursor: "pointer",
            fontFamily: "inherit",
          }}
        >
          {l.label}
        </button>
      ))}
    </div>
  );
}

function LoadingSpinner() {
  return (
    <div style={{
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
      justifyContent: "center",
      minHeight: "100vh",
      background: C.bg,
      color: C.text,
    }}>
      <div style={{
        width: 48,
        height: 48,
        border: `3px solid ${C.border}`,
        borderTop: `3px solid ${C.accent}`,
        borderRadius: "50%",
        animation: "spin 1s linear infinite",
      }} />
      <style>{`@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }`}</style>
      <div style={{ marginTop: 20, fontSize: 14, color: C.textMuted }}>Loading metadata...</div>
    </div>
  );
}

function ErrorDisplay({ error }) {
  return (
    <div style={{
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
      justifyContent: "center",
      minHeight: "100vh",
      background: C.bg,
      color: C.text,
      padding: 40,
    }}>
      <div style={{ fontSize: 48, marginBottom: 20 }}>!</div>
      <h2 style={{ margin: "0 0 16px", color: C.pii }}>Failed to Load Metadata</h2>
      <p style={{ color: C.textMuted, textAlign: "center", maxWidth: 500, lineHeight: 1.6 }}>
        {error}
      </p>
    </div>
  );
}

// -- Lineage Visualization (SVG) --
function LineageGraph({ highlight, tables, tableLineage }) {
  const layers = ["raw_ecommerce", "staging_ecommerce", "analytics_ecommerce", "reporting_ecommerce"];
  const layerLabels = ["Raw", "Staging", "Analytics", "Reporting"];
  const layerColors = [C.raw, C.staging, C.analytics, C.reporting];

  const tablesByLayer = layers.map(l => tables.filter(t => t.dataset === l));
  const colWidth = 210;
  const nodeH = 36;
  const nodeGap = 12;
  const layerGap = 80;
  const padTop = 50;

  const maxPerLayer = Math.max(...tablesByLayer.map(tl => tl.length));
  const svgH = padTop + 40 + maxPerLayer * (nodeH + nodeGap) + 40;
  const svgW = layers.length * (colWidth + layerGap) - layerGap + 40;

  const pos = {};
  tablesByLayer.forEach((tl, li) => {
    const x = 20 + li * (colWidth + layerGap);
    tl.forEach((t, ti) => {
      const y = padTop + 40 + ti * (nodeH + nodeGap);
      const key = `${t.dataset}.${t.name}`;
      pos[key] = { x, y, w: colWidth, h: nodeH, table: t, layerIdx: li };
    });
  });

  const isHighlighted = (key) => {
    if (!highlight) return true;
    return key === highlight || tableLineage.some(e =>
      (e.up === highlight && e.down === key) || (e.down === highlight && e.up === key)
    );
  };

  return (
    <svg width={svgW} height={svgH} style={{ display: "block" }}>
      <defs>
        <marker id="arrow" viewBox="0 0 10 7" refX="9" refY="3.5" markerWidth="8" markerHeight="6" orient="auto">
          <polygon points="0 0, 10 3.5, 0 7" fill={C.textDim} opacity="0.5" />
        </marker>
        <marker id="arrow-hl" viewBox="0 0 10 7" refX="9" refY="3.5" markerWidth="8" markerHeight="6" orient="auto">
          <polygon points="0 0, 10 3.5, 0 7" fill={C.accent} opacity="0.9" />
        </marker>
      </defs>

      {layers.map((l, i) => (
        <g key={l}>
          <text
            x={20 + i * (colWidth + layerGap) + colWidth / 2}
            y={padTop - 8}
            textAnchor="middle"
            fill={layerColors[i]}
            fontSize={13}
            fontWeight={700}
            fontFamily="'JetBrains Mono', monospace"
          >
            {layerLabels[i].toUpperCase()}
          </text>
          <line
            x1={20 + i * (colWidth + layerGap)}
            x2={20 + i * (colWidth + layerGap) + colWidth}
            y1={padTop + 2}
            y2={padTop + 2}
            stroke={layerColors[i]}
            strokeWidth={2}
            opacity={0.4}
          />
        </g>
      ))}

      {tableLineage.map((edge, i) => {
        const from = pos[edge.up];
        const to = pos[edge.down];
        if (!from || !to) return null;
        const hl = highlight ? (edge.up === highlight || edge.down === highlight) : false;
        const opacity = highlight ? (hl ? 0.8 : 0.08) : 0.25;
        return (
          <path
            key={i}
            d={`M ${from.x + from.w} ${from.y + from.h / 2} C ${from.x + from.w + 40} ${from.y + from.h / 2}, ${to.x - 40} ${to.y + to.h / 2}, ${to.x} ${to.y + to.h / 2}`}
            fill="none"
            stroke={hl ? C.accent : C.textDim}
            strokeWidth={hl ? 2 : 1}
            opacity={opacity}
            markerEnd={hl ? "url(#arrow-hl)" : "url(#arrow)"}
          />
        );
      })}

      {Object.entries(pos).map(([key, p]) => {
        const hl = isHighlighted(key);
        const lc = layerColors[p.layerIdx];
        return (
          <g key={key} opacity={hl ? 1 : 0.2} style={{ cursor: "pointer" }}>
            <rect
              x={p.x} y={p.y} width={p.w} height={p.h}
              rx={8}
              fill={highlight === key ? lc + "20" : C.surface}
              stroke={highlight === key ? lc : C.border}
              strokeWidth={highlight === key ? 2 : 1}
            />
            {p.table.pii && (
              <circle cx={p.x + 14} cy={p.y + p.h / 2} r={4} fill={C.pii} />
            )}
            <text
              x={p.x + (p.table.pii ? 26 : 14)}
              y={p.y + p.h / 2 + 1}
              fill={C.text}
              fontSize={11}
              fontFamily="'JetBrains Mono', monospace"
              dominantBaseline="middle"
            >
              <title>{p.table.name}</title>
              {p.table.name.length > 24 ? p.table.name.slice(0, 22) + "..." : p.table.name}
            </text>
            {p.table.type === "VIEW" && (
              <text
                x={p.x + p.w - 10}
                y={p.y + p.h / 2 + 1}
                fill={C.textDim}
                fontSize={9}
                fontFamily="'JetBrains Mono', monospace"
                dominantBaseline="middle"
                textAnchor="end"
              >
                VIEW
              </text>
            )}
          </g>
        );
      })}
    </svg>
  );
}

// -- Column-Level Lineage Detail --
function ColumnLineagePanel({ columnLineage }) {
  const [selected, setSelected] = useState(null);

  const traceColumn = (col) => {
    const upstream = [];
    let cur = col;
    for (let i = 0; i < 10; i++) {
      const edge = columnLineage.find(e => e.down === cur);
      if (!edge) break;
      upstream.unshift(edge);
      cur = edge.up;
    }
    const downstream = [];
    cur = col;
    for (let i = 0; i < 10; i++) {
      const edge = columnLineage.find(e => e.up === cur);
      if (!edge) break;
      downstream.push(edge);
      cur = edge.down;
    }
    return { upstream, downstream };
  };

  const uniqueCols = [...new Set([...columnLineage.map(e => e.up), ...columnLineage.map(e => e.down)])].sort();
  const trace = selected ? traceColumn(selected) : null;
  const allEdges = trace ? [...trace.upstream, ...trace.downstream] : [];

  const getLayerFromCol = (col) => {
    if (col?.includes("raw_")) return "raw";
    if (col?.includes("staging_")) return "staging";
    if (col?.includes("analytics_")) return "analytics";
    if (col?.includes("reporting_")) return "reporting";
    return "unknown";
  };

  return (
    <div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 20 }}>
        {uniqueCols.map(col => {
          const layer = getLayerFromCol(col);
          const lc = layer === "raw" ? C.raw : layer === "staging" ? C.staging : layer === "analytics" ? C.analytics : C.reporting;
          const shortCol = col.split(".").slice(-2).join(".");
          return (
            <button
              key={col}
              onClick={() => setSelected(selected === col ? null : col)}
              style={{
                background: selected === col ? lc + "20" : C.surface,
                color: selected === col ? lc : C.textMuted,
                border: `1px solid ${selected === col ? lc + "60" : C.border}`,
                borderRadius: 6,
                padding: "4px 10px",
                fontSize: 11,
                fontFamily: "'JetBrains Mono', monospace",
                cursor: "pointer",
              }}
              title={col}
            >
              {shortCol}
            </button>
          );
        })}
      </div>

      {allEdges.length > 0 && (
        <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 10, padding: 16 }}>
          <div style={{ fontSize: 11, color: C.textDim, marginBottom: 12, textTransform: "uppercase", letterSpacing: "0.06em", fontWeight: 600 }}>
            Column Lineage Trace
          </div>
          {allEdges.map((e, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8, fontSize: 12, flexWrap: "wrap" }}>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", color: C.text, fontSize: 10 }}>{e.up}</span>
              <span style={{ color: C.accent }}>-&gt;</span>
              <Badge color={C.accent} small>{e.transform}</Badge>
              <span style={{ color: C.accent }}>-&gt;</span>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", color: C.text, fontSize: 10 }}>{e.down}</span>
            </div>
          ))}
        </div>
      )}

      {!selected && (
        <div style={{ color: C.textDim, fontSize: 13, textAlign: "center", padding: 30 }}>
          Click a column above to trace its lineage across layers
        </div>
      )}
    </div>
  );
}

// -- Overview Panel with full inventory --
function OverviewPanel({ data, tables, datasets }) {
  const [search, setSearch] = useState("");
  const [layerFilter, setLayerFilter] = useState(null);
  const [sort, setSort] = useState({ key: "dataset", dir: "asc" });

  const totalCols = data.columns?.length || 0;
  const totalRows = tables.reduce((s, t) => s + (t.rows || 0), 0);
  const totalBytes = tables.reduce((s, t) => s + (t.bytes || 0), 0);
  const viewCount = tables.filter(t => t.type === "VIEW").length;
  const tableCount = tables.filter(t => t.type !== "VIEW").length;
  const tableLineageCount = data.lineage?.table_lineage?.length || 0;
  const columnLineageCount = data.lineage?.column_lineage?.length || 0;
  const piiCount = data.pii_classification?.length || 0;
  const jobCount = data.jobs?.length || 0;
  const tableOptionsCount = data.table_options?.length || 0;

  const handleSort = (key) => {
    setSort(prev => ({
      key,
      dir: prev.key === key && prev.dir === "asc" ? "desc" : "asc"
    }));
  };

  const filteredTables = useMemo(() => {
    let result = [...tables];
    if (layerFilter) {
      result = result.filter(t => t.dataset.includes(layerFilter));
    }
    if (search) {
      const s = search.toLowerCase();
      result = result.filter(t =>
        t.name.toLowerCase().includes(s) ||
        t.dataset.toLowerCase().includes(s) ||
        t.description?.toLowerCase().includes(s)
      );
    }
    result.sort((a, b) => {
      let aVal = a[sort.key];
      let bVal = b[sort.key];
      if (typeof aVal === "string") aVal = aVal?.toLowerCase() || "";
      if (typeof bVal === "string") bVal = bVal?.toLowerCase() || "";
      if (aVal < bVal) return sort.dir === "asc" ? -1 : 1;
      if (aVal > bVal) return sort.dir === "asc" ? 1 : -1;
      return 0;
    });
    return result;
  }, [tables, search, layerFilter, sort]);

  return (
    <div>
      <h3 style={{ margin: "0 0 4px", fontSize: 16, fontWeight: 600 }}>Data Warehouse Overview</h3>
      <p style={{ margin: "0 0 20px", fontSize: 12, color: C.textDim }}>
        Complete inventory of extracted BigQuery metadata
      </p>

      {/* Stats Grid */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(120px, 1fr))", gap: 10, marginBottom: 24 }}>
        <StatCard label="Datasets" value={datasets.length} color={C.accent} />
        <StatCard label="Tables" value={tableCount} color={C.analytics} />
        <StatCard label="Views" value={viewCount} color={C.reporting} />
        <StatCard label="Columns" value={totalCols} color={C.staging} />
        <StatCard label="Total Rows" value={totalRows.toLocaleString()} color={C.quality} />
        <StatCard label="Total Size" value={formatBytes(totalBytes)} color={C.qualityWarn} />
        <StatCard label="PII Columns" value={piiCount} color={C.pii} />
        <StatCard label="Lineage Edges" value={tableLineageCount + columnLineageCount} color={C.quality} />
        <StatCard label="Jobs" value={jobCount} color={C.textMuted} />
        <StatCard label="Table Options" value={tableOptionsCount} color={C.accent} />
      </div>

      {/* Filters */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12, flexWrap: "wrap", gap: 12 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <h4 style={{ margin: 0, fontSize: 14, fontWeight: 600, color: C.text }}>Table Inventory</h4>
          <LayerFilter value={layerFilter} onChange={setLayerFilter} />
        </div>
        <SearchBox value={search} onChange={setSearch} placeholder="Filter tables..." />
      </div>

      {/* Table Inventory */}
      <div style={{ overflowX: "auto" }}>
        <div style={{
          display: "grid",
          gridTemplateColumns: "minmax(70px, 0.8fr) minmax(120px, 1.5fr) 50px minmax(100px, 2fr) 55px 60px 40px 32px minmax(50px, 0.7fr) minmax(55px, 0.7fr) minmax(50px, 0.7fr) 45px 50px minmax(70px, 1fr) 70px 70px",
          gap: 4,
          padding: "8px 10px",
          fontSize: 9,
          color: C.textDim,
          textTransform: "uppercase",
          letterSpacing: "0.05em",
          fontWeight: 600,
          borderBottom: `1px solid ${C.border}`,
          minWidth: 1100,
        }}>
          <SortableHeader label="Dataset" sortKey="dataset" currentSort={sort} onSort={handleSort} />
          <SortableHeader label="Table Name" sortKey="name" currentSort={sort} onSort={handleSort} />
          <SortableHeader label="Type" sortKey="type" currentSort={sort} onSort={handleSort} />
          <span>Description</span>
          <SortableHeader label="Rows" sortKey="rows" currentSort={sort} onSort={handleSort} />
          <SortableHeader label="Size" sortKey="bytes" currentSort={sort} onSort={handleSort} />
          <SortableHeader label="Cols" sortKey="columns" currentSort={sort} onSort={handleSort} />
          <span>PII</span>
          <span>Owner</span>
          <span>Domain</span>
          <span>Source</span>
          <span>Certified</span>
          <span>Partition</span>
          <span>Cluster</span>
          <SortableHeader label="Created" sortKey="created" currentSort={sort} onSort={handleSort} />
          <SortableHeader label="Modified" sortKey="modified" currentSort={sort} onSort={handleSort} />
        </div>
        <div style={{ maxHeight: 420, overflowY: "auto" }}>
          {filteredTables.map((t, i) => (
            <div key={`${t.dataset}.${t.name}`} style={{
              display: "grid",
              gridTemplateColumns: "minmax(70px, 0.8fr) minmax(120px, 1.5fr) 50px minmax(100px, 2fr) 55px 60px 40px 32px minmax(50px, 0.7fr) minmax(55px, 0.7fr) minmax(50px, 0.7fr) 45px 50px minmax(70px, 1fr) 70px 70px",
              gap: 4,
              padding: "8px 10px",
              background: i % 2 === 0 ? C.surface : "transparent",
              borderRadius: 4,
              alignItems: "center",
              fontSize: 11,
              minWidth: 1100,
            }}>
              <Badge color={layerColor(t.dataset)} small>{t.dataset.replace("_ecommerce", "")}</Badge>
              <span title={t.name} style={{ fontFamily: "'JetBrains Mono', monospace", color: C.text, fontSize: 10, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{t.name}</span>
              <Badge color={t.type === "VIEW" ? C.reporting : C.analytics} small>{t.type === "VIEW" ? "VIEW" : "TABLE"}</Badge>
              <span title={t.description} style={{ color: C.textMuted, fontSize: 10, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{t.description || "-"}</span>
              <span style={{ fontFamily: "monospace", color: C.text, fontSize: 10 }}>{(t.rows || 0).toLocaleString()}</span>
              <span style={{ fontFamily: "monospace", color: C.textMuted, fontSize: 10 }}>{formatBytes(t.bytes || 0)}</span>
              <span style={{ fontFamily: "monospace", color: C.text, fontSize: 10 }}>{t.columns}</span>
              <span>{t.pii ? <Badge color={C.pii} small>Y</Badge> : <span style={{ color: C.textDim }}>-</span>}</span>
              <span title={t.labels?.owner} style={{ fontSize: 9, color: C.textDim, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{t.labels?.owner?.split("-")[0] || "-"}</span>
              <span style={{ fontSize: 9, color: C.textDim }}>{t.labels?.domain || "-"}</span>
              <span style={{ fontSize: 9, color: C.textDim }}>{t.labels?.source || "-"}</span>
              <span>{t.labels?.certified === "true" ? <span style={{ color: "#4ade80" }}>✓</span> : <span style={{ color: C.textDim }}>-</span>}</span>
              <span style={{ fontSize: 9, color: t.partitioning ? C.text : C.textDim }}>
                {t.partitioning?.type || "-"}
              </span>
              <span title={t.clustering?.join(", ")} style={{ fontSize: 9, color: t.clustering?.length ? C.text : C.textDim, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {t.clustering?.length ? t.clustering.slice(0, 2).join(", ") : "-"}
              </span>
              <span style={{ fontSize: 9, color: C.textDim }}>{formatDate(t.created)}</span>
              <span style={{ fontSize: 9, color: C.textDim }}>{formatDate(t.modified)}</span>
            </div>
          ))}
        </div>
      </div>
      <div style={{ marginTop: 8, fontSize: 11, color: C.textDim }}>
        Showing {filteredTables.length} of {tables.length} tables
      </div>
    </div>
  );
}

// -- Enhanced Schema Browser --
function SchemaBrowser({ tables, datasets, views, columns, columnFieldPaths, tableOptions }) {
  const [selectedTable, setSelectedTable] = useState(null);
  const [filterDataset, setFilterDataset] = useState(null);
  const [search, setSearch] = useState("");

  const filteredTables = useMemo(() => {
    let result = filterDataset ? tables.filter(t => t.dataset === filterDataset) : tables;
    if (search) {
      const s = search.toLowerCase();
      result = result.filter(t => t.name.toLowerCase().includes(s));
    }
    return result;
  }, [tables, filterDataset, search]);

  const selectedTableData = tables.find(t => `${t.dataset}.${t.name}` === selectedTable);
  const viewDefinition = views?.find(v => `${v.dataset_id}.${v.view_name}` === selectedTable);

  // Get detailed columns from INFORMATION_SCHEMA
  const detailedColumns = useMemo(() => {
    if (!selectedTable) return [];
    return columns?.filter(c => `${c.dataset_id}.${c.table_name}` === selectedTable) || [];
  }, [columns, selectedTable]);

  // Get field paths (may have collation, policy tags)
  const fieldPaths = useMemo(() => {
    if (!selectedTable) return {};
    const paths = columnFieldPaths?.filter(c => `${c.dataset_id}.${c.table_name}` === selectedTable) || [];
    const map = {};
    paths.forEach(p => { map[p.column_name] = p; });
    return map;
  }, [columnFieldPaths, selectedTable]);

  // Get table options for selected table
  const tableOpts = useMemo(() => {
    if (!selectedTable) return [];
    return tableOptions?.filter(o => `${o.dataset_id}.${o.table_name}` === selectedTable) || [];
  }, [tableOptions, selectedTable]);

  return (
    <div style={{ display: "grid", gridTemplateColumns: "260px 1fr", gap: 16, minHeight: 400 }}>
      <div>
        <div style={{ marginBottom: 10 }}>
          <SearchBox value={search} onChange={setSearch} placeholder="Filter tables..." />
        </div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginBottom: 10 }}>
          <button
            onClick={() => setFilterDataset(null)}
            style={{
              background: !filterDataset ? C.accent + "20" : "transparent",
              color: !filterDataset ? C.accent : C.textDim,
              border: "none", borderRadius: 6, padding: "4px 8px", fontSize: 10, cursor: "pointer", fontFamily: "inherit",
            }}
          >All</button>
          {datasets.map(d => (
            <button
              key={d.id}
              onClick={() => setFilterDataset(d.id)}
              style={{
                background: filterDataset === d.id ? d.color + "20" : "transparent",
                color: filterDataset === d.id ? d.color : C.textDim,
                border: "none", borderRadius: 6, padding: "4px 8px", fontSize: 10, cursor: "pointer", fontFamily: "inherit",
              }}
            >{d.label}</button>
          ))}
        </div>
        <div style={{ display: "grid", gap: 4, maxHeight: 480, overflowY: "auto" }}>
          {filteredTables.map(t => {
            const key = `${t.dataset}.${t.name}`;
            const isActive = selectedTable === key;
            return (
              <button
                key={key}
                onClick={() => setSelectedTable(isActive ? null : key)}
                style={{
                  background: isActive ? layerColor(t.dataset) + "15" : C.surface,
                  border: `1px solid ${isActive ? layerColor(t.dataset) + "40" : C.border}`,
                  borderRadius: 6,
                  padding: "6px 10px",
                  textAlign: "left",
                  cursor: "pointer",
                  fontFamily: "inherit",
                }}
              >
                <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                  <div style={{ width: 6, height: 6, borderRadius: "50%", background: layerColor(t.dataset) }} />
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: C.text }}>{t.name}</span>
                  {t.type === "VIEW" && <Badge color={C.reporting} small>VIEW</Badge>}
                  {t.pii && <Badge color={C.pii} small>PII</Badge>}
                </div>
                <div style={{ fontSize: 9, color: C.textDim, marginTop: 2, marginLeft: 12 }}>
                  {(t.rows || 0).toLocaleString()} rows - {t.columns} cols
                </div>
              </button>
            );
          })}
        </div>
      </div>

      <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 10, padding: 16, overflowY: "auto", maxHeight: 600 }}>
        {selectedTableData ? (
          <>
            {/* Header */}
            <div style={{ marginBottom: 14 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 14, fontWeight: 700, color: C.text }}>
                  {selectedTable}
                </span>
                {selectedTableData.pii && <Badge color={C.pii}>PII</Badge>}
                {selectedTableData.type === "VIEW" && <Badge color={C.reporting}>VIEW</Badge>}
              </div>
              {selectedTableData.friendlyName && (
                <div style={{ fontSize: 12, color: C.textMuted, marginBottom: 4 }}>{selectedTableData.friendlyName}</div>
              )}
              <div style={{ fontSize: 12, color: C.textDim, lineHeight: 1.4 }}>
                {selectedTableData.description || "No description"}
              </div>
            </div>

            {/* Metadata Card */}
            <div style={{ background: C.bg, borderRadius: 8, padding: 12, marginBottom: 14 }}>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(120px, 1fr))", gap: 10 }}>
                <div>
                  <div style={{ fontSize: 9, color: C.textDim, textTransform: "uppercase", marginBottom: 2 }}>Rows</div>
                  <div style={{ fontSize: 13, color: C.text, fontFamily: "'JetBrains Mono', monospace" }}>{(selectedTableData.rows || 0).toLocaleString()}</div>
                </div>
                <div>
                  <div style={{ fontSize: 9, color: C.textDim, textTransform: "uppercase", marginBottom: 2 }}>Size</div>
                  <div style={{ fontSize: 13, color: C.text, fontFamily: "'JetBrains Mono', monospace" }}>{formatBytes(selectedTableData.bytes || 0)}</div>
                </div>
                <div>
                  <div style={{ fontSize: 9, color: C.textDim, textTransform: "uppercase", marginBottom: 2 }}>Created</div>
                  <div style={{ fontSize: 11, color: C.text }}>{formatDate(selectedTableData.created)}</div>
                </div>
                <div>
                  <div style={{ fontSize: 9, color: C.textDim, textTransform: "uppercase", marginBottom: 2 }}>Modified</div>
                  <div style={{ fontSize: 11, color: C.text }}>{formatDate(selectedTableData.modified)}</div>
                </div>
                {selectedTableData.expires && (
                  <div>
                    <div style={{ fontSize: 9, color: C.textDim, textTransform: "uppercase", marginBottom: 2 }}>Expires</div>
                    <div style={{ fontSize: 11, color: C.qualityWarn }}>{formatDate(selectedTableData.expires)}</div>
                  </div>
                )}
                {selectedTableData.partitioning?.type && (
                  <div>
                    <div style={{ fontSize: 9, color: C.textDim, textTransform: "uppercase", marginBottom: 2 }}>Partitioning</div>
                    <div style={{ fontSize: 11, color: C.text }}>{selectedTableData.partitioning.type} on {selectedTableData.partitioning.field || "ingestion"}</div>
                  </div>
                )}
                {selectedTableData.clustering?.length > 0 && (
                  <div>
                    <div style={{ fontSize: 9, color: C.textDim, textTransform: "uppercase", marginBottom: 2 }}>Clustering</div>
                    <div style={{ fontSize: 11, color: C.text }}>{selectedTableData.clustering.join(", ")}</div>
                  </div>
                )}
                {selectedTableData.encryption && (
                  <div>
                    <div style={{ fontSize: 9, color: C.textDim, textTransform: "uppercase", marginBottom: 2 }}>Encryption</div>
                    <div style={{ fontSize: 11, color: C.quality }}>CMEK</div>
                  </div>
                )}
              </div>

              {/* Labels */}
              {Object.keys(selectedTableData.labels || {}).length > 0 && (
                <div style={{ marginTop: 10, display: "flex", flexWrap: "wrap", gap: 4 }}>
                  {Object.entries(selectedTableData.labels).map(([k, v]) => (
                    <span key={k} style={{
                      fontFamily: "'JetBrains Mono', monospace", fontSize: 9, color: C.textDim,
                      background: C.surface, padding: "2px 6px", borderRadius: 4,
                    }}>{k}={v}</span>
                  ))}
                </div>
              )}
            </div>

            {/* View Definition */}
            {viewDefinition && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ fontSize: 10, color: C.textDim, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.05em", fontWeight: 600 }}>
                  View Definition {viewDefinition.use_standard_sql && <Badge color={C.accent} small>Standard SQL</Badge>}
                </div>
                <div style={{
                  background: C.bg,
                  borderRadius: 6,
                  padding: 10,
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: 10,
                  color: C.text,
                  whiteSpace: "pre-wrap",
                  overflowX: "auto",
                  maxHeight: 160,
                  overflowY: "auto",
                }}>
                  {viewDefinition.view_definition}
                </div>
              </div>
            )}

            {/* Table Options */}
            {tableOpts.length > 0 && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ fontSize: 10, color: C.textDim, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.05em", fontWeight: 600 }}>
                  Table Options ({tableOpts.length})
                </div>
                <div style={{ display: "grid", gap: 4 }}>
                  {tableOpts.map((opt, i) => (
                    <div key={i} style={{ background: C.bg, borderRadius: 4, padding: "6px 10px", fontSize: 10 }}>
                      <span style={{ color: C.accent, fontFamily: "'JetBrains Mono', monospace" }}>{opt.option_name}</span>
                      <span style={{ color: C.textDim, marginLeft: 8 }}>{opt.option_type}</span>
                      <div style={{ color: C.textMuted, marginTop: 2, wordBreak: "break-all" }}>{truncate(opt.option_value, 100)}</div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Schema */}
            {selectedTableData.schema && selectedTableData.schema.length > 0 && (
              <div>
                <div style={{ fontSize: 10, color: C.textDim, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.05em", fontWeight: 600 }}>
                  Schema ({selectedTableData.schema.length} columns)
                </div>
                <div style={{
                  display: "grid", gridTemplateColumns: "24px 120px 90px 60px 1fr",
                  padding: "4px 8px", fontSize: 9, color: C.textDim, textTransform: "uppercase",
                  letterSpacing: "0.05em", fontWeight: 600, borderBottom: `1px solid ${C.border}`,
                }}>
                  <span>#</span><span>Column</span><span>Type</span><span>Mode</span><span>Description</span>
                </div>
                <div style={{ maxHeight: 200, overflowY: "auto" }}>
                  {selectedTableData.schema.map((col, i) => {
                    const detail = detailedColumns.find(c => c.column_name === col.name);
                    const fp = fieldPaths[col.name];
                    return (
                      <div key={col.name} style={{
                        display: "grid", gridTemplateColumns: "24px 120px 90px 60px 1fr",
                        padding: "6px 8px", borderRadius: 4,
                        background: i % 2 === 0 ? C.bg : "transparent",
                        alignItems: "center",
                      }}>
                        <span style={{ fontSize: 9, color: C.textDim }}>{detail?.ordinal_position || i + 1}</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: C.text }}>{col.name}</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: C.accent }}>{col.type}</span>
                        <Badge color={col.mode === "REQUIRED" ? C.qualityWarn : C.textDim} small>{col.mode || "NULL"}</Badge>
                        <div>
                          <span style={{ fontSize: 10, color: C.textMuted }}>{col.description || fp?.description || "-"}</span>
                          {col.pii && <Badge color={C.pii} small style={{ marginLeft: 4 }}>PII</Badge>}
                          {fp?.collation_name && fp.collation_name !== "NULL" && (
                            <span style={{ marginLeft: 6, fontSize: 9, color: C.textDim }}>collation: {fp.collation_name}</span>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </>
        ) : (
          <div style={{ color: C.textDim, fontSize: 13, padding: 40, textAlign: "center" }}>
            Select a table to explore its schema and metadata
          </div>
        )}
      </div>
    </div>
  );
}

// -- Enhanced Quality Panel --
function QualityPanel({ qualityData }) {
  const [filter, setFilter] = useState("all");
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState({ key: "nullPct", dir: "desc" });

  const allColumns = useMemo(() => {
    const cols = [];
    qualityData.forEach(table => {
      (table.columns || []).forEach(col => {
        cols.push({
          table: `${table.dataset_id}.${table.table_name}`,
          dataset: table.dataset_id,
          tableName: table.table_name,
          column: col.column_name,
          dataType: col.data_type,
          nullPct: col.null_pct || 0,
          nullCount: col.null_count || 0,
          distinctPct: col.uniqueness_pct || 0,
          distinctCount: col.distinct_count || 0,
          totalRows: col.total_rows || 0,
          min: col.min,
          max: col.max,
          mean: col.mean,
          stddev: col.stddev,
          minLength: col.min_length,
          maxLength: col.max_length,
          avgLength: col.avg_length,
        });
      });
    });
    return cols;
  }, [qualityData]);

  const summary = useMemo(() => {
    const withNulls = allColumns.filter(c => c.nullPct > 0).length;
    const lowDistinct = allColumns.filter(c => c.distinctPct < 50 && c.distinctPct > 0).length;
    const highNull = allColumns.filter(c => c.nullPct > 50).length;
    return { total: allColumns.length, withNulls, lowDistinct, highNull };
  }, [allColumns]);

  const filteredColumns = useMemo(() => {
    let result = [...allColumns];
    if (filter === "issues") {
      result = result.filter(c => c.nullPct > 0 || c.distinctPct < 50);
    } else if (filter === "nulls") {
      result = result.filter(c => c.nullPct > 0);
    }
    if (search) {
      const s = search.toLowerCase();
      result = result.filter(c =>
        c.column.toLowerCase().includes(s) ||
        c.tableName.toLowerCase().includes(s)
      );
    }
    result.sort((a, b) => {
      let aVal = a[sort.key];
      let bVal = b[sort.key];
      if (aVal < bVal) return sort.dir === "asc" ? -1 : 1;
      if (aVal > bVal) return sort.dir === "asc" ? 1 : -1;
      return 0;
    });
    return result;
  }, [allColumns, filter, search, sort]);

  const handleSort = (key) => {
    setSort(prev => ({
      key,
      dir: prev.key === key && prev.dir === "desc" ? "asc" : "desc"
    }));
  };

  return (
    <div>
      {/* Summary */}
      <div style={{ display: "flex", gap: 16, marginBottom: 16, flexWrap: "wrap" }}>
        <div style={{ background: C.surface, borderRadius: 8, padding: "10px 16px", border: `1px solid ${C.border}` }}>
          <span style={{ fontSize: 20, fontWeight: 700, color: C.text, fontFamily: "'JetBrains Mono', monospace" }}>{summary.total}</span>
          <span style={{ fontSize: 11, color: C.textDim, marginLeft: 8 }}>columns profiled</span>
        </div>
        <div style={{ background: C.surface, borderRadius: 8, padding: "10px 16px", border: `1px solid ${C.border}` }}>
          <span style={{ fontSize: 20, fontWeight: 700, color: C.qualityWarn, fontFamily: "'JetBrains Mono', monospace" }}>{summary.withNulls}</span>
          <span style={{ fontSize: 11, color: C.textDim, marginLeft: 8 }}>with nulls &gt; 0%</span>
        </div>
        <div style={{ background: C.surface, borderRadius: 8, padding: "10px 16px", border: `1px solid ${C.border}` }}>
          <span style={{ fontSize: 20, fontWeight: 700, color: C.qualityBad, fontFamily: "'JetBrains Mono', monospace" }}>{summary.lowDistinct}</span>
          <span style={{ fontSize: 11, color: C.textDim, marginLeft: 8 }}>low distinctness (&lt;50%)</span>
        </div>
      </div>

      {/* Filters */}
      <div style={{ display: "flex", gap: 8, marginBottom: 14, flexWrap: "wrap", alignItems: "center" }}>
        <div style={{ display: "flex", gap: 4 }}>
          {[
            { key: "all", label: "All Columns" },
            { key: "issues", label: "With Issues" },
            { key: "nulls", label: "Has Nulls" },
          ].map(f => (
            <button
              key={f.key}
              onClick={() => setFilter(f.key)}
              style={{
                background: filter === f.key ? C.accent + "20" : "transparent",
                color: filter === f.key ? C.accent : C.textDim,
                border: `1px solid ${filter === f.key ? C.accent + "40" : C.border}`,
                borderRadius: 6,
                padding: "5px 10px",
                fontSize: 11,
                cursor: "pointer",
                fontFamily: "inherit",
              }}
            >
              {f.label}
            </button>
          ))}
        </div>
        <SearchBox value={search} onChange={setSearch} placeholder="Filter columns..." />
      </div>

      {/* Data Grid */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "1.6fr 0.8fr 70px 70px 80px 100px 100px 70px",
        gap: 6,
        padding: "6px 10px",
        fontSize: 9,
        color: C.textDim,
        textTransform: "uppercase",
        letterSpacing: "0.05em",
        fontWeight: 600,
        borderBottom: `1px solid ${C.border}`,
      }}>
        <span>Table.Column</span>
        <span>Type</span>
        <SortableHeader label="Null %" sortKey="nullPct" currentSort={sort} onSort={handleSort} />
        <SortableHeader label="Dist %" sortKey="distinctPct" currentSort={sort} onSort={handleSort} />
        <SortableHeader label="Distinct" sortKey="distinctCount" currentSort={sort} onSort={handleSort} />
        <span>Min/Max</span>
        <span>Stats</span>
        <span>Layer</span>
      </div>
      <div style={{ maxHeight: 420, overflowY: "auto" }}>
        {filteredColumns.map((q, i) => {
          const nullColor = q.nullPct === 0 ? C.quality : q.nullPct < 10 ? C.qualityWarn : C.qualityBad;
          const distinctColor = q.distinctPct > 80 ? C.quality : q.distinctPct > 50 ? C.textMuted : C.qualityWarn;
          const isNumeric = ["INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"].includes(q.dataType);
          const isString = q.dataType === "STRING";
          const isDate = ["DATE", "TIMESTAMP", "DATETIME"].includes(q.dataType);

          return (
            <div key={i} style={{
              display: "grid",
              gridTemplateColumns: "1.6fr 0.8fr 70px 70px 80px 100px 100px 70px",
              gap: 6,
              padding: "8px 10px",
              background: i % 2 === 0 ? C.surface : "transparent",
              borderRadius: 4,
              alignItems: "center",
              fontSize: 11,
            }}>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", color: C.text, fontSize: 10 }}>
                {q.tableName}.{q.column}
              </span>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", color: C.accent, fontSize: 9 }}>
                {q.dataType}
              </span>
              <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
                <div style={{ width: 30, height: 5, background: C.border, borderRadius: 2, overflow: "hidden" }}>
                  <div style={{ width: `${Math.min(q.nullPct, 100)}%`, height: "100%", background: nullColor }} />
                </div>
                <span style={{ color: nullColor, fontFamily: "monospace", fontSize: 10 }}>{q.nullPct.toFixed(1)}%</span>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
                <div style={{ width: 30, height: 5, background: C.border, borderRadius: 2, overflow: "hidden" }}>
                  <div style={{ width: `${Math.min(q.distinctPct, 100)}%`, height: "100%", background: distinctColor }} />
                </div>
                <span style={{ color: distinctColor, fontFamily: "monospace", fontSize: 10 }}>{q.distinctPct.toFixed(0)}%</span>
              </div>
              <span style={{ fontFamily: "monospace", color: C.textMuted, fontSize: 10 }}>
                {q.distinctCount.toLocaleString()}
              </span>
              <span style={{ fontFamily: "monospace", color: C.textDim, fontSize: 9 }}>
                {isNumeric && q.min !== undefined ? `${q.min} - ${q.max}` : ""}
                {isString && q.minLength !== undefined ? `len: ${q.minLength}-${q.maxLength}` : ""}
                {isDate && q.min ? `${String(q.min).slice(0, 10)}` : ""}
              </span>
              <span style={{ fontFamily: "monospace", color: C.textDim, fontSize: 9 }}>
                {isNumeric && q.mean !== undefined ? `avg: ${q.mean?.toFixed(1)}, std: ${q.stddev?.toFixed(1) || "-"}` : ""}
                {isString && q.avgLength !== undefined ? `avg len: ${q.avgLength?.toFixed(1)}` : ""}
              </span>
              <Badge color={layerColor(q.dataset)} small>
                {q.dataset.split("_")[0]}
              </Badge>
            </div>
          );
        })}
      </div>
      <div style={{ marginTop: 8, fontSize: 11, color: C.textDim }}>
        Showing {filteredColumns.length} of {allColumns.length} columns
      </div>
    </div>
  );
}

// -- PII Panel --
function PIIPanel({ piiColumns }) {
  const byClassification = {};
  piiColumns.forEach(p => {
    if (!byClassification[p.classification]) byClassification[p.classification] = [];
    byClassification[p.classification].push(p);
  });

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
        {Object.entries(byClassification).map(([cls, items]) => (
          <div key={cls} style={{
            background: C.surface,
            border: `1px solid ${C.pii}30`,
            borderRadius: 8,
            padding: "12px 16px",
            minWidth: 160,
          }}>
            <div style={{ color: C.pii, fontSize: 10, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 6 }}>
              {cls}
            </div>
            <div style={{ fontSize: 22, fontWeight: 700, color: C.text, fontFamily: "'JetBrains Mono', monospace" }}>
              {items.length}
            </div>
            <div style={{ fontSize: 10, color: C.textDim, marginTop: 2 }}>
              across {new Set(items.map(i => i.table)).size} tables
            </div>
          </div>
        ))}
      </div>

      <div style={{
        display: "grid",
        gridTemplateColumns: "1.4fr 1fr 1fr 1.2fr 70px",
        gap: 6,
        padding: "6px 10px",
        fontSize: 9,
        color: C.textDim,
        textTransform: "uppercase",
        letterSpacing: "0.05em",
        fontWeight: 600,
        borderBottom: `1px solid ${C.border}`,
      }}>
        <span>Column</span>
        <span>Table</span>
        <span>Dataset</span>
        <span>Classification</span>
        <span>Confidence</span>
      </div>
      <div style={{ maxHeight: 380, overflowY: "auto" }}>
        {piiColumns.map((p, i) => (
          <div key={i} style={{
            display: "grid",
            gridTemplateColumns: "1.4fr 1fr 1fr 1.2fr 70px",
            gap: 6,
            padding: "7px 10px",
            background: i % 2 === 0 ? C.surface : "transparent",
            borderRadius: 4,
            alignItems: "center",
            fontSize: 11,
          }}>
            <span style={{ fontFamily: "'JetBrains Mono', monospace", color: C.pii }}>{p.column}</span>
            <span style={{ color: C.text }}>{p.table}</span>
            <Badge color={layerColor(p.dataset)} small>{p.dataset.replace("_ecommerce", "")}</Badge>
            <span style={{ color: C.textMuted }}>{p.classification}</span>
            <Badge color={p.confidence === "high" ? C.pii : C.qualityWarn} small>{p.confidence}</Badge>
          </div>
        ))}
      </div>
    </div>
  );
}

// -- Enhanced Access Panel --
function AccessPanel({ datasets, accessData, datasetMetadata }) {
  return (
    <div style={{ display: "grid", gap: 16 }}>
      {/* Dataset Configuration */}
      <div>
        <h4 style={{ margin: "0 0 12px", fontSize: 13, fontWeight: 600, color: C.text }}>Dataset Configuration</h4>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: 10 }}>
          {datasetMetadata.map(ds => {
            const color = layerColor(ds.dataset_id);
            return (
              <div key={ds.dataset_id} style={{
                background: C.surface,
                border: `1px solid ${C.border}`,
                borderRadius: 8,
                padding: 14,
              }}>
                <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 10 }}>
                  <div style={{ width: 8, height: 8, borderRadius: "50%", background: color }} />
                  <span style={{ color: C.text, fontWeight: 600, fontSize: 12 }}>{ds.dataset_id}</span>
                </div>
                <div style={{ display: "grid", gap: 5, fontSize: 11 }}>
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <span style={{ color: C.textDim }}>Location</span>
                    <span style={{ color: C.text, fontFamily: "'JetBrains Mono', monospace" }}>{ds.location || "-"}</span>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <span style={{ color: C.textDim }}>Default Table Expiration</span>
                    <span style={{ color: C.text }}>{ds.default_table_expiration_ms ? `${ds.default_table_expiration_ms / 86400000}d` : "-"}</span>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <span style={{ color: C.textDim }}>Max Time Travel</span>
                    <span style={{ color: C.text }}>{ds.max_time_travel_hours ? `${ds.max_time_travel_hours}h` : "-"}</span>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <span style={{ color: C.textDim }}>Storage Billing</span>
                    <span style={{ color: C.text }}>{ds.storage_billing_model || "LOGICAL"}</span>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <span style={{ color: C.textDim }}>Case Insensitive</span>
                    <span style={{ color: C.text }}>{ds.is_case_insensitive ? "Yes" : "No"}</span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Permissions */}
      <div>
        <h4 style={{ margin: "0 0 12px", fontSize: 13, fontWeight: 600, color: C.text }}>Dataset Permissions</h4>
        {accessData.map(dsAccess => {
          const color = layerColor(dsAccess.dataset_id);
          return (
            <div key={dsAccess.dataset_id} style={{
              background: C.surface,
              border: `1px solid ${C.border}`,
              borderRadius: 8,
              padding: 12,
              marginBottom: 10,
            }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
                <div style={{ width: 8, height: 8, borderRadius: "50%", background: color }} />
                <span style={{ color: C.text, fontWeight: 600, fontSize: 12 }}>{dsAccess.dataset_id}</span>
              </div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                {dsAccess.access_entries.map((e, i) => (
                  <div key={i} style={{
                    display: "flex", alignItems: "center", gap: 6,
                    background: C.bg, borderRadius: 6, padding: "5px 10px",
                  }}>
                    <Badge color={e.role === "OWNER" ? C.pii : e.role === "WRITER" ? C.qualityWarn : C.quality} small>
                      {e.role}
                    </Badge>
                    <span style={{ fontSize: 10, color: C.textMuted }}>{e.entity_type}</span>
                    <span style={{ fontSize: 10, color: C.text, fontFamily: "'JetBrains Mono', monospace" }}>{e.entity_id}</span>
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// -- Enhanced Query History Panel --
function QueryHistoryPanel({ jobs }) {
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState({ key: "creation_time", dir: "desc" });

  const summary = useMemo(() => {
    const totalBytes = jobs.reduce((s, j) => s + (j.bytes_processed || 0), 0);
    const totalBilled = jobs.reduce((s, j) => s + (j.bytes_billed || 0), 0);
    const uniqueUsers = new Set(jobs.map(j => j.user_email)).size;
    const successful = jobs.filter(j => j.state === "DONE").length;
    const cached = jobs.filter(j => j.cache_hit).length;
    const cacheRate = jobs.length > 0 ? ((cached / jobs.length) * 100).toFixed(0) : 0;
    return { totalBytes, totalBilled, uniqueUsers, total: jobs.length, successful, cached, cacheRate };
  }, [jobs]);

  const filteredJobs = useMemo(() => {
    let result = [...jobs];
    if (search) {
      const s = search.toLowerCase();
      result = result.filter(j =>
        j.query_preview?.toLowerCase().includes(s) ||
        j.user_email?.toLowerCase().includes(s) ||
        j.statement_type?.toLowerCase().includes(s)
      );
    }
    return result;
  }, [jobs, search]);

  const handleSort = (key) => {
    setSort(prev => ({
      key,
      dir: prev.key === key && prev.dir === "desc" ? "asc" : "desc"
    }));
  };

  return (
    <div>
      {/* Summary */}
      <div style={{ display: "flex", gap: 10, marginBottom: 16, flexWrap: "wrap" }}>
        <StatCard label="Total Queries" value={summary.total} color={C.accent} />
        <StatCard label="Unique Users" value={summary.uniqueUsers} color={C.reporting} />
        <StatCard label="Bytes Processed" value={formatBytes(summary.totalBytes)} color={C.analytics} />
        <StatCard label="Bytes Billed" value={formatBytes(summary.totalBilled)} color={C.qualityWarn} />
        <StatCard label="Cache Hit Rate" value={`${summary.cacheRate}%`} sub={`${summary.cached} cached`} color={C.quality} />
      </div>

      <div style={{ marginBottom: 12 }}>
        <SearchBox value={search} onChange={setSearch} placeholder="Search queries or users..." />
      </div>

      {/* Header */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "130px 100px 70px 50px 80px 80px 60px 50px 70px 1fr",
        gap: 6,
        padding: "6px 10px",
        fontSize: 9,
        color: C.textDim,
        textTransform: "uppercase",
        letterSpacing: "0.05em",
        fontWeight: 600,
        borderBottom: `1px solid ${C.border}`,
      }}>
        <SortableHeader label="Timestamp" sortKey="creation_time" currentSort={sort} onSort={handleSort} />
        <span>User</span>
        <span>Type</span>
        <span>State</span>
        <SortableHeader label="Processed" sortKey="bytes_processed" currentSort={sort} onSort={handleSort} />
        <SortableHeader label="Billed" sortKey="bytes_billed" currentSort={sort} onSort={handleSort} />
        <span>Duration</span>
        <span>Cache</span>
        <span>Sources</span>
        <span>Query Preview</span>
      </div>

      <div style={{ maxHeight: 400, overflowY: "auto" }}>
        {filteredJobs.map((job, i) => (
          <div key={job.job_id} style={{
            display: "grid",
            gridTemplateColumns: "130px 100px 70px 50px 80px 80px 60px 50px 70px 1fr",
            gap: 6,
            padding: "8px 10px",
            background: i % 2 === 0 ? C.surface : "transparent",
            borderRadius: 4,
            alignItems: "center",
            fontSize: 10,
          }}>
            <span style={{ color: C.textMuted, fontSize: 9 }}>{formatDateTime(job.creation_time)}</span>
            <span style={{ color: C.text, fontSize: 9 }}>{job.user_email?.split("@")[0] || "-"}</span>
            <Badge color={C.accent} small>{job.statement_type || job.job_type}</Badge>
            <Badge color={job.state === "DONE" ? C.quality : C.qualityWarn} small>{job.state}</Badge>
            <span style={{ fontFamily: "monospace", color: C.text, fontSize: 9 }}>{formatBytes(job.bytes_processed || 0)}</span>
            <span style={{ fontFamily: "monospace", color: C.qualityWarn, fontSize: 9 }}>{formatBytes(job.bytes_billed || 0)}</span>
            <span style={{ fontFamily: "monospace", color: C.textMuted, fontSize: 9 }}>{formatDuration(job.creation_time, job.end_time)}</span>
            <span>{job.cache_hit ? <Badge color={C.quality} small>Y</Badge> : <span style={{ color: C.textDim }}>-</span>}</span>
            <span style={{ color: C.textMuted, fontSize: 9 }}>{job.referenced_tables?.length || 0} tables</span>
            <span style={{ color: C.textDim, fontSize: 9, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {truncate(job.query_preview?.trim(), 80)}
            </span>
          </div>
        ))}
      </div>
      <div style={{ marginTop: 8, fontSize: 11, color: C.textDim }}>
        Showing {filteredJobs.length} of {jobs.length} jobs
      </div>
    </div>
  );
}

// -- Table Options Panel (NEW) --
function TableOptionsPanel({ tableOptions }) {
  const [search, setSearch] = useState("");

  // Group by table
  const grouped = useMemo(() => {
    const map = {};
    tableOptions.forEach(opt => {
      const key = `${opt.dataset_id}.${opt.table_name}`;
      if (!map[key]) map[key] = { dataset: opt.dataset_id, table: opt.table_name, options: [] };
      map[key].options.push(opt);
    });
    return Object.values(map);
  }, [tableOptions]);

  const filtered = useMemo(() => {
    if (!search) return grouped;
    const s = search.toLowerCase();
    return grouped.filter(g =>
      g.table.toLowerCase().includes(s) ||
      g.dataset.toLowerCase().includes(s) ||
      g.options.some(o => o.option_name.toLowerCase().includes(s))
    );
  }, [grouped, search]);

  return (
    <div>
      <div style={{ marginBottom: 14 }}>
        <SearchBox value={search} onChange={setSearch} placeholder="Filter by table or option..." />
      </div>
      <div style={{ display: "grid", gap: 12, maxHeight: 500, overflowY: "auto" }}>
        {filtered.map(g => (
          <div key={`${g.dataset}.${g.table}`} style={{
            background: C.surface,
            border: `1px solid ${C.border}`,
            borderRadius: 8,
            padding: 12,
          }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
              <Badge color={layerColor(g.dataset)} small>{g.dataset.replace("_ecommerce", "")}</Badge>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: C.text }}>{g.table}</span>
              <span style={{ fontSize: 10, color: C.textDim }}>({g.options.length} options)</span>
            </div>
            <div style={{ display: "grid", gap: 6 }}>
              {g.options.map((opt, i) => (
                <div key={i} style={{ background: C.bg, borderRadius: 4, padding: "6px 10px" }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <span style={{ color: C.accent, fontFamily: "'JetBrains Mono', monospace", fontSize: 11 }}>{opt.option_name}</span>
                    <span style={{ color: C.textDim, fontSize: 9 }}>{opt.option_type}</span>
                  </div>
                  <div style={{ color: C.textMuted, fontSize: 10, marginTop: 4, wordBreak: "break-all" }}>
                    {truncate(opt.option_value, 150)}
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
      <div style={{ marginTop: 8, fontSize: 11, color: C.textDim }}>
        Showing {filtered.length} tables with {tableOptions.length} total options
      </div>
    </div>
  );
}

// -- Coverage Panel (NEW) --
function CoveragePanel({ data }) {
  const metadataTypes = [
    { key: "datasets", label: "Datasets", desc: "Dataset-level metadata" },
    { key: "tables", label: "Tables", desc: "Table-level metadata" },
    { key: "columns", label: "Columns", desc: "Column metadata from INFORMATION_SCHEMA" },
    { key: "column_field_paths", label: "Column Field Paths", desc: "Nested field paths with policy tags" },
    { key: "views", label: "View Definitions", desc: "SQL definitions for views" },
    { key: "table_options", label: "Table Options", desc: "Options like expiration, labels" },
    { key: "lineage.table_lineage", label: "Table Lineage", desc: "Table-to-table data flow" },
    { key: "lineage.column_lineage", label: "Column Lineage", desc: "Column-to-column data flow" },
    { key: "quality", label: "Data Quality", desc: "Column profiling stats" },
    { key: "pii_classification", label: "PII Classification", desc: "Auto-detected PII columns" },
    { key: "access.dataset_permissions", label: "Dataset Permissions", desc: "Access control entries" },
    { key: "jobs", label: "Query History", desc: "Recent query jobs" },
    { key: "routines", label: "Routines", desc: "Stored procedures and UDFs", empty: "No routines defined" },
    { key: "routine_parameters", label: "Routine Parameters", desc: "Parameters for routines", empty: "No routines defined" },
    { key: "scheduled_queries", label: "Scheduled Queries", desc: "BigQuery Data Transfer schedules", empty: "No scheduled queries" },
    { key: "storage", label: "Storage Metadata", desc: "TABLE_STORAGE stats", empty: "Requires specific region setup" },
    { key: "table_constraints", label: "Table Constraints", desc: "Primary/foreign keys", empty: "No constraints defined" },
    { key: "key_column_usage", label: "Key Column Usage", desc: "Constraint column mappings", empty: "No constraints defined" },
    { key: "row_level_security", label: "Row-Level Security", desc: "RLS policies", empty: "No RLS policies" },
    { key: "snapshots", label: "Table Snapshots", desc: "Snapshot metadata", empty: "No snapshots exist" },
    { key: "partitions", label: "Partitions", desc: "Partition-level stats", empty: "No partitioned tables" },
    { key: "streaming_buffers", label: "Streaming Buffers", desc: "Active streaming data", empty: "No active streaming" },
    { key: "search_indexes", label: "Search Indexes", desc: "Full-text search indexes", empty: "No search indexes" },
    { key: "vector_indexes", label: "Vector Indexes", desc: "Vector similarity indexes", empty: "No vector indexes" },
    { key: "materialized_views", label: "Materialized Views", desc: "Materialized view metadata", empty: "No materialized views" },
    { key: "bi_engine", label: "BI Engine", desc: "BI Engine capacity", empty: "BI Engine not configured" },
  ];

  const getCount = (key) => {
    const parts = key.split(".");
    let val = data;
    for (const p of parts) {
      val = val?.[p];
    }
    if (Array.isArray(val)) return val.length;
    if (typeof val === "object" && val) return Object.keys(val).length;
    return 0;
  };

  const populated = metadataTypes.filter(m => getCount(m.key) > 0).length;

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 14, fontWeight: 600, color: C.text, marginBottom: 4 }}>Metadata Coverage</div>
        <div style={{ fontSize: 12, color: C.textDim }}>
          {populated} of {metadataTypes.length} metadata types have data
        </div>
      </div>

      <div style={{ display: "grid", gap: 8 }}>
        {metadataTypes.map(m => {
          const count = getCount(m.key);
          const hasData = count > 0;
          return (
            <div key={m.key} style={{
              display: "flex",
              alignItems: "center",
              gap: 12,
              background: hasData ? C.surface : "transparent",
              border: `1px solid ${hasData ? C.border : C.border + "60"}`,
              borderRadius: 6,
              padding: "10px 14px",
            }}>
              <span style={{ fontSize: 16, width: 24 }}>{hasData ? "✓" : "○"}</span>
              <div style={{ flex: 1 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{ fontSize: 12, fontWeight: 600, color: hasData ? C.text : C.textDim }}>{m.label}</span>
                  {hasData && (
                    <Badge color={C.quality} small>{count} items</Badge>
                  )}
                </div>
                <div style={{ fontSize: 10, color: C.textDim, marginTop: 2 }}>
                  {hasData ? m.desc : (m.empty || "No data available")}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// -- Main App --
export default function MetadataDashboard() {
  const [activeTab, setActiveTab] = useState("overview");
  const [lineageHighlight, setLineageHighlight] = useState(null);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetch("/bigquery_metadata.json")
      .then(res => {
        if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
        return res.json();
      })
      .then(json => {
        setData(json);
        setLoading(false);
      })
      .catch(err => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  const {
    datasets, tables, tableLineage, columnLineage, piiColumns,
    qualityData, accessData, jobs, views, columns, columnFieldPaths,
    tableOptions, datasetMetadata
  } = useMemo(() => {
    if (!data) return {
      datasets: [], tables: [], tableLineage: [], columnLineage: [],
      piiColumns: [], qualityData: [], accessData: [], jobs: [], views: [],
      columns: [], columnFieldPaths: [], tableOptions: [], datasetMetadata: []
    };

    const datasets = (data.datasets || []).map(d => ({
      id: d.dataset_id,
      label: d.dataset_id.replace("_ecommerce", "").charAt(0).toUpperCase() + d.dataset_id.replace("_ecommerce", "").slice(1),
      color: layerColor(d.dataset_id),
    }));

    const piiSet = new Set((data.pii_classification || []).map(p => `${p.dataset_id}.${p.table_name}.${p.column_name}`));

    const tables = (data.tables || []).map(t => {
      const hasPii = (t.schema_fields || []).some(col => piiSet.has(`${t.dataset_id}.${t.table_name}.${col.name}`));
      return {
        dataset: t.dataset_id,
        name: t.table_name,
        type: t.table_type === "VIEW" ? "VIEW" : "TABLE",
        rows: t.num_rows || 0,
        bytes: t.num_bytes || 0,
        columns: (t.schema_fields || []).length,
        pii: hasPii || (t.labels?.pii === "true"),
        labels: t.labels || {},
        description: t.description,
        friendlyName: t.friendly_name,
        created: t.created,
        modified: t.modified,
        expires: t.expires,
        partitioning: t.partitioning,
        clustering: t.clustering_fields,
        streamingBuffer: t.streaming_buffer,
        encryption: t.encryption_configuration,
        schema: (t.schema_fields || []).map(col => ({
          name: col.name,
          type: col.field_type,
          mode: col.mode,
          description: col.description,
          pii: piiSet.has(`${t.dataset_id}.${t.table_name}.${col.name}`),
        })),
      };
    });

    const tableLineage = (data.lineage?.table_lineage || []).map(e => ({
      up: e.upstream,
      down: e.downstream,
      transform: e.transform,
    }));

    const columnLineage = (data.lineage?.column_lineage || []).map(e => ({
      up: e.upstream_col,
      down: e.downstream_col,
      transform: e.transform,
    }));

    const piiColumns = (data.pii_classification || []).map(p => ({
      dataset: p.dataset_id,
      table: p.table_name,
      column: p.column_name,
      classification: p.pii_classification,
      confidence: p.confidence,
    }));

    const qualityData = data.quality || [];
    const accessData = (data.access?.dataset_permissions || []).map(d => ({
      dataset_id: d.dataset_id,
      access_entries: d.access_entries || [],
    }));
    const jobs = data.jobs || [];
    const views = data.views || [];
    const columns = data.columns || [];
    const columnFieldPaths = data.column_field_paths || [];
    const tableOptions = data.table_options || [];
    const datasetMetadata = data.datasets || [];

    return {
      datasets, tables, tableLineage, columnLineage, piiColumns,
      qualityData, accessData, jobs, views, columns, columnFieldPaths,
      tableOptions, datasetMetadata
    };
  }, [data]);

  const hasTableOptions = tableOptions && tableOptions.length > 0;

  if (loading) return <LoadingSpinner />;
  if (error) return <ErrorDisplay error={error} />;

  return (
    <div style={{
      fontFamily: "'Inter', 'Segoe UI', system-ui, sans-serif",
      background: C.bg,
      color: C.text,
      minHeight: "100vh",
      padding: "20px 28px",
    }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 6 }}>
          <div style={{
            width: 32, height: 32, borderRadius: 8,
            background: `linear-gradient(135deg, ${C.accent}, ${C.reporting})`,
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 16, fontWeight: 800, color: "#fff",
          }}>M</div>
          <div>
            <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, letterSpacing: "-0.02em" }}>
              BigQuery Metadata Explorer
            </h1>
            <div style={{ fontSize: 11, color: C.textDim, marginTop: 1 }}>
              {data?.extraction_metadata?.project_id} - v{data?.extraction_metadata?.extractor_version} - Extracted {formatDateTime(data?.extraction_metadata?.extracted_at)}
            </div>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 5, marginBottom: 20, flexWrap: "wrap" }}>
        <TabButton active={activeTab === "overview"} onClick={() => setActiveTab("overview")} color={C.accent}>
          Overview
        </TabButton>
        <TabButton active={activeTab === "schema"} onClick={() => setActiveTab("schema")} color={C.analytics}>
          Schema Browser
        </TabButton>
        <TabButton active={activeTab === "lineage"} onClick={() => setActiveTab("lineage")} color={C.accent}>
          Table Lineage
        </TabButton>
        <TabButton active={activeTab === "collineage"} onClick={() => setActiveTab("collineage")} color={C.quality}>
          Column Lineage
        </TabButton>
        <TabButton active={activeTab === "quality"} onClick={() => setActiveTab("quality")} color={C.quality}>
          Data Quality
        </TabButton>
        <TabButton active={activeTab === "pii"} onClick={() => setActiveTab("pii")} color={C.pii}>
          PII Classification
        </TabButton>
        <TabButton active={activeTab === "access"} onClick={() => setActiveTab("access")} color={C.reporting}>
          Access & Permissions
        </TabButton>
        <TabButton active={activeTab === "jobs"} onClick={() => setActiveTab("jobs")} color={C.textMuted}>
          Query History
        </TabButton>
        {hasTableOptions && (
          <TabButton active={activeTab === "options"} onClick={() => setActiveTab("options")} color={C.staging}>
            Table Options
          </TabButton>
        )}
        <TabButton active={activeTab === "coverage"} onClick={() => setActiveTab("coverage")} color={C.quality}>
          Coverage
        </TabButton>
        <TabButton active={activeTab === "reference"} onClick={() => setActiveTab("reference")} color={C.textMuted}>
          Reference
        </TabButton>
      </div>

      {/* Content */}
      <div style={{
        background: C.surface + "80",
        border: `1px solid ${C.border}`,
        borderRadius: 12,
        padding: 20,
      }}>
        {activeTab === "overview" && (
          <OverviewPanel data={data} tables={tables} datasets={datasets} />
        )}

        {activeTab === "lineage" && (
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
              <div>
                <h3 style={{ margin: 0, fontSize: 15, fontWeight: 600 }}>Table Lineage Graph</h3>
                <p style={{ margin: "3px 0 0", fontSize: 11, color: C.textDim }}>
                  Click a table node to highlight its dependencies
                </p>
              </div>
              {lineageHighlight && (
                <button
                  onClick={() => setLineageHighlight(null)}
                  style={{
                    background: C.bg, border: `1px solid ${C.border}`, borderRadius: 6,
                    padding: "4px 10px", color: C.textMuted, fontSize: 10, cursor: "pointer", fontFamily: "inherit",
                  }}
                >Clear filter</button>
              )}
            </div>
            <div style={{ overflowX: "auto" }}>
              <div onClick={(e) => {
                const t = e.target.closest("g[opacity]");
                if (!t) return;
                const text = t.querySelector("text");
                if (!text) return;
                const name = text.textContent;
                const table = tables.find(tb => tb.name === name);
                if (table) {
                  const key = `${table.dataset}.${table.name}`;
                  setLineageHighlight(lineageHighlight === key ? null : key);
                }
              }}>
                <LineageGraph highlight={lineageHighlight} tables={tables} tableLineage={tableLineage} />
              </div>
            </div>
          </div>
        )}

        {activeTab === "collineage" && (
          <div>
            <h3 style={{ margin: "0 0 3px", fontSize: 15, fontWeight: 600 }}>Column-Level Lineage</h3>
            <p style={{ margin: "0 0 14px", fontSize: 11, color: C.textDim }}>
              Trace how columns flow across the data stack ({columnLineage.length} edges)
            </p>
            <ColumnLineagePanel columnLineage={columnLineage} />
          </div>
        )}

        {activeTab === "schema" && (
          <div>
            <h3 style={{ margin: "0 0 3px", fontSize: 15, fontWeight: 600 }}>Schema Browser</h3>
            <p style={{ margin: "0 0 14px", fontSize: 11, color: C.textDim }}>
              Explore tables, columns, and metadata across {tables.length} assets
            </p>
            <SchemaBrowser
              tables={tables}
              datasets={datasets}
              views={views}
              columns={columns}
              columnFieldPaths={columnFieldPaths}
              tableOptions={tableOptions}
            />
          </div>
        )}

        {activeTab === "quality" && (
          <div>
            <h3 style={{ margin: "0 0 3px", fontSize: 15, fontWeight: 600 }}>Data Quality Signals</h3>
            <p style={{ margin: "0 0 14px", fontSize: 11, color: C.textDim }}>
              Column-level profiling with null rates, distinctness, and type-specific statistics
            </p>
            <QualityPanel qualityData={qualityData} />
          </div>
        )}

        {activeTab === "pii" && (
          <div>
            <h3 style={{ margin: "0 0 3px", fontSize: 15, fontWeight: 600 }}>PII Classification</h3>
            <p style={{ margin: "0 0 14px", fontSize: 11, color: C.textDim }}>
              Auto-detected personally identifiable information ({piiColumns.length} columns)
            </p>
            <PIIPanel piiColumns={piiColumns} />
          </div>
        )}

        {activeTab === "access" && (
          <div>
            <h3 style={{ margin: "0 0 3px", fontSize: 15, fontWeight: 600 }}>Access & Permissions</h3>
            <p style={{ margin: "0 0 14px", fontSize: 11, color: C.textDim }}>
              Dataset configuration and access control
            </p>
            <AccessPanel datasets={datasets} accessData={accessData} datasetMetadata={datasetMetadata} />
          </div>
        )}

        {activeTab === "jobs" && (
          <div>
            <h3 style={{ margin: "0 0 3px", fontSize: 15, fontWeight: 600 }}>Query History</h3>
            <p style={{ margin: "0 0 14px", fontSize: 11, color: C.textDim }}>
              Recent query jobs ({jobs.length} queries)
            </p>
            <QueryHistoryPanel jobs={jobs} />
          </div>
        )}

        {activeTab === "options" && hasTableOptions && (
          <div>
            <h3 style={{ margin: "0 0 3px", fontSize: 15, fontWeight: 600 }}>Table Options</h3>
            <p style={{ margin: "0 0 14px", fontSize: 11, color: C.textDim }}>
              BigQuery table options ({tableOptions.length} options across tables)
            </p>
            <TableOptionsPanel tableOptions={tableOptions} />
          </div>
        )}

        {activeTab === "coverage" && (
          <div>
            <h3 style={{ margin: "0 0 3px", fontSize: 15, fontWeight: 600 }}>Metadata Coverage</h3>
            <p style={{ margin: "0 0 14px", fontSize: 11, color: C.textDim }}>
              All 26 metadata types extracted from BigQuery
            </p>
            <CoveragePanel data={data} />
          </div>
        )}

        {activeTab === "reference" && (
          <MetadataReference />
        )}
      </div>

      {/* Footer */}
      <div style={{
        marginTop: 20, padding: "12px 0",
        borderTop: `1px solid ${C.border}`,
        display: "flex", justifyContent: "space-between",
        fontSize: 10, color: C.textDim,
      }}>
        <span>BigQuery Metadata Spike</span>
        <span>{datasets.length} datasets - {tables.length} tables - {tableLineage.length + columnLineage.length} lineage edges - {piiColumns.length} PII columns</span>
      </div>
    </div>
  );
}
