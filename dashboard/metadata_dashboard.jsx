import { useState, useMemo } from "react";
import * as d3 from "d3";

// ── Embedded metadata (mirrors what extract_metadata.py would produce) ──
const DATASETS = [
  { id: "raw_ecommerce", label: "Raw", layer: "raw", color: "#ef4444", description: "Raw ingested e-commerce data from source systems", tables: 6 },
  { id: "staging_ecommerce", label: "Staging", layer: "staging", color: "#f59e0b", description: "Cleaned and typed e-commerce data", tables: 6 },
  { id: "analytics_ecommerce", label: "Analytics", layer: "analytics", color: "#3b82f6", description: "Business logic layer — aggregations, derived metrics", tables: 4 },
  { id: "reporting_ecommerce", label: "Reporting", layer: "reporting", color: "#8b5cf6", description: "BI-ready views for dashboards", tables: 4 },
];

const TABLES = [
  { dataset: "raw_ecommerce", name: "customers", type: "TABLE", rows: 10, sizeMB: 0.002, columns: 16, pii: true, labels: { source: "crm", pii: "true" }, description: "Raw customer records ingested from CRM. Contains PII fields." },
  { dataset: "raw_ecommerce", name: "products", type: "TABLE", rows: 12, sizeMB: 0.001, columns: 11, pii: false, labels: { source: "catalog" }, description: "Raw product catalog data. Prices as strings." },
  { dataset: "raw_ecommerce", name: "orders", type: "TABLE", rows: 15, sizeMB: 0.002, columns: 14, pii: false, labels: { source: "oms" }, description: "Raw order records from OMS. Amounts as strings." },
  { dataset: "raw_ecommerce", name: "order_items", type: "TABLE", rows: 23, sizeMB: 0.001, columns: 7, pii: false, labels: { source: "oms" }, description: "Raw order line items." },
  { dataset: "raw_ecommerce", name: "payments", type: "TABLE", rows: 15, sizeMB: 0.002, columns: 11, pii: true, labels: { source: "stripe", pii: "true" }, description: "Raw payment transactions from Stripe. Contains partial card info." },
  { dataset: "raw_ecommerce", name: "shipping_events", type: "TABLE", rows: 12, sizeMB: 0.001, columns: 9, pii: false, labels: { source: "logistics" }, description: "Raw shipping/logistics events from carrier APIs." },

  { dataset: "staging_ecommerce", name: "stg_customers", type: "TABLE", rows: 9, sizeMB: 0.002, columns: 16, pii: true, labels: { certified: "true", pii: "true" }, description: "Cleaned customer records. Deduplicated, dates parsed." },
  { dataset: "staging_ecommerce", name: "stg_products", type: "TABLE", rows: 12, sizeMB: 0.001, columns: 10, pii: false, labels: { certified: "true" }, description: "Cleaned product catalog. Prices as NUMERIC." },
  { dataset: "staging_ecommerce", name: "stg_orders", type: "TABLE", rows: 15, sizeMB: 0.002, columns: 13, pii: false, labels: { certified: "true" }, description: "Cleaned orders. Amounts cast to NUMERIC." },
  { dataset: "staging_ecommerce", name: "stg_order_items", type: "TABLE", rows: 23, sizeMB: 0.001, columns: 6, pii: false, labels: { certified: "true" }, description: "Cleaned order line items." },
  { dataset: "staging_ecommerce", name: "stg_payments", type: "TABLE", rows: 15, sizeMB: 0.002, columns: 11, pii: true, labels: { certified: "true", pii: "true" }, description: "Cleaned payment transactions." },
  { dataset: "staging_ecommerce", name: "stg_shipping_events", type: "TABLE", rows: 12, sizeMB: 0.001, columns: 8, pii: false, labels: {}, description: "Cleaned shipping events." },

  { dataset: "analytics_ecommerce", name: "customer_360", type: "TABLE", rows: 9, sizeMB: 0.003, columns: 17, pii: true, labels: { domain: "customer", pii: "true" }, description: "Unified customer profile with order behavior metrics." },
  { dataset: "analytics_ecommerce", name: "product_performance", type: "TABLE", rows: 12, sizeMB: 0.002, columns: 14, pii: false, labels: { domain: "product" }, description: "Product-level performance: revenue, units, margin." },
  { dataset: "analytics_ecommerce", name: "revenue_daily", type: "TABLE", rows: 11, sizeMB: 0.001, columns: 10, pii: false, labels: { domain: "finance" }, description: "Daily revenue aggregation. Source of truth for revenue." },
  { dataset: "analytics_ecommerce", name: "order_fulfillment", type: "TABLE", rows: 15, sizeMB: 0.002, columns: 13, pii: false, labels: { domain: "operations" }, description: "Order-level fulfillment tracking." },

  { dataset: "reporting_ecommerce", name: "v_executive_kpis", type: "VIEW", rows: 4, sizeMB: 0, columns: 3, pii: false, labels: { dashboard: "executive", certified: "true" }, description: "Executive-level KPIs for C-suite dashboard." },
  { dataset: "reporting_ecommerce", name: "v_customer_segments", type: "VIEW", rows: 4, sizeMB: 0, columns: 6, pii: false, labels: { dashboard: "marketing", certified: "true" }, description: "Customer segmentation for marketing dashboard." },
  { dataset: "reporting_ecommerce", name: "v_category_performance", type: "VIEW", rows: 6, sizeMB: 0, columns: 7, pii: false, labels: { dashboard: "merchandising" }, description: "Category performance for merchandising." },
  { dataset: "reporting_ecommerce", name: "v_fulfillment_sla", type: "VIEW", rows: 5, sizeMB: 0, columns: 6, pii: false, labels: { dashboard: "operations" }, description: "Fulfillment SLA metrics for operations." },
];

const TABLE_LINEAGE = [
  { up: "raw_ecommerce.customers", down: "staging_ecommerce.stg_customers" },
  { up: "raw_ecommerce.products", down: "staging_ecommerce.stg_products" },
  { up: "raw_ecommerce.orders", down: "staging_ecommerce.stg_orders" },
  { up: "raw_ecommerce.order_items", down: "staging_ecommerce.stg_order_items" },
  { up: "raw_ecommerce.payments", down: "staging_ecommerce.stg_payments" },
  { up: "raw_ecommerce.shipping_events", down: "staging_ecommerce.stg_shipping_events" },
  { up: "staging_ecommerce.stg_customers", down: "analytics_ecommerce.customer_360" },
  { up: "staging_ecommerce.stg_orders", down: "analytics_ecommerce.customer_360" },
  { up: "staging_ecommerce.stg_products", down: "analytics_ecommerce.product_performance" },
  { up: "staging_ecommerce.stg_order_items", down: "analytics_ecommerce.product_performance" },
  { up: "staging_ecommerce.stg_orders", down: "analytics_ecommerce.product_performance" },
  { up: "staging_ecommerce.stg_orders", down: "analytics_ecommerce.revenue_daily" },
  { up: "staging_ecommerce.stg_orders", down: "analytics_ecommerce.order_fulfillment" },
  { up: "staging_ecommerce.stg_payments", down: "analytics_ecommerce.order_fulfillment" },
  { up: "staging_ecommerce.stg_shipping_events", down: "analytics_ecommerce.order_fulfillment" },
  { up: "analytics_ecommerce.revenue_daily", down: "reporting_ecommerce.v_executive_kpis" },
  { up: "analytics_ecommerce.customer_360", down: "reporting_ecommerce.v_executive_kpis" },
  { up: "analytics_ecommerce.customer_360", down: "reporting_ecommerce.v_customer_segments" },
  { up: "analytics_ecommerce.product_performance", down: "reporting_ecommerce.v_category_performance" },
  { up: "analytics_ecommerce.order_fulfillment", down: "reporting_ecommerce.v_fulfillment_sla" },
];

const COLUMN_LINEAGE = [
  { up: "raw.customers.email", down: "stg.stg_customers.email", transform: "LOWER()" },
  { up: "stg.stg_customers.email", down: "analytics.customer_360.email", transform: "pass-through" },
  { up: "raw.customers.customer_id", down: "stg.stg_customers.customer_id", transform: "dedup" },
  { up: "stg.stg_customers.customer_id", down: "analytics.customer_360.customer_id", transform: "pass-through" },
  { up: "raw.orders.total_amount", down: "stg.stg_orders.total_amount", transform: "CAST → NUMERIC" },
  { up: "stg.stg_orders.total_amount", down: "analytics.customer_360.lifetime_revenue", transform: "SUM()" },
  { up: "stg.stg_orders.total_amount", down: "analytics.revenue_daily.gross_revenue", transform: "SUM()" },
  { up: "stg.stg_orders.order_id", down: "analytics.customer_360.customer_segment", transform: "COUNT → CASE" },
  { up: "analytics.revenue_daily.net_revenue", down: "reporting.v_executive_kpis.kpi_value", transform: "SUM → STRING" },
  { up: "analytics.customer_360.customer_segment", down: "reporting.v_customer_segments.customer_segment", transform: "GROUP BY" },
];

const PII_COLUMNS = [
  { dataset: "raw_ecommerce", table: "customers", column: "email", classification: "Email Address", confidence: "high" },
  { dataset: "raw_ecommerce", table: "customers", column: "phone", classification: "Phone Number", confidence: "high" },
  { dataset: "raw_ecommerce", table: "customers", column: "date_of_birth", classification: "Date of Birth", confidence: "high" },
  { dataset: "raw_ecommerce", table: "customers", column: "address_line1", classification: "Physical Address", confidence: "medium" },
  { dataset: "raw_ecommerce", table: "customers", column: "postal_code", classification: "Postal Code", confidence: "high" },
  { dataset: "raw_ecommerce", table: "customers", column: "first_name", classification: "Person Name", confidence: "medium" },
  { dataset: "raw_ecommerce", table: "customers", column: "last_name", classification: "Person Name", confidence: "medium" },
  { dataset: "raw_ecommerce", table: "payments", column: "card_last_four", classification: "Partial Card Number", confidence: "high" },
  { dataset: "staging_ecommerce", table: "stg_customers", column: "email", classification: "Email Address", confidence: "high" },
  { dataset: "staging_ecommerce", table: "stg_customers", column: "phone", classification: "Phone Number", confidence: "high" },
  { dataset: "staging_ecommerce", table: "stg_customers", column: "date_of_birth", classification: "Date of Birth", confidence: "high" },
  { dataset: "staging_ecommerce", table: "stg_customers", column: "first_name", classification: "Person Name", confidence: "medium" },
  { dataset: "staging_ecommerce", table: "stg_customers", column: "last_name", classification: "Person Name", confidence: "medium" },
  { dataset: "staging_ecommerce", table: "stg_payments", column: "card_last_four", classification: "Partial Card Number", confidence: "high" },
  { dataset: "analytics_ecommerce", table: "customer_360", column: "email", classification: "Email Address", confidence: "high" },
  { dataset: "analytics_ecommerce", table: "customer_360", column: "first_name", classification: "Person Name", confidence: "medium" },
  { dataset: "analytics_ecommerce", table: "customer_360", column: "last_name", classification: "Person Name", confidence: "medium" },
];

const QUALITY_PROFILES = [
  { table: "raw_ecommerce.customers", column: "email", nullPct: 0, distinctPct: 90, issue: null },
  { table: "raw_ecommerce.customers", column: "phone", nullPct: 0, distinctPct: 90, issue: null },
  { table: "raw_ecommerce.customers", column: "date_of_birth", nullPct: 0, distinctPct: 90, issue: null },
  { table: "raw_ecommerce.customers", column: "customer_id", nullPct: 0, distinctPct: 90, issue: "Duplicate rows (C002 appears twice)" },
  { table: "raw_ecommerce.orders", column: "total_amount", nullPct: 0, distinctPct: 87, issue: "Stored as STRING, needs casting" },
  { table: "raw_ecommerce.orders", column: "order_status", nullPct: 0, distinctPct: 33, issue: null },
  { table: "raw_ecommerce.payments", column: "card_last_four", nullPct: 13.3, distinctPct: 73, issue: "NULL for PayPal payments" },
  { table: "raw_ecommerce.products", column: "is_active", nullPct: 0, distinctPct: 17, issue: "Stored as STRING 'true'/'false'" },
  { table: "staging_ecommerce.stg_customers", column: "customer_id", nullPct: 0, distinctPct: 100, issue: null },
  { table: "staging_ecommerce.stg_orders", column: "total_amount", nullPct: 0, distinctPct: 87, issue: null },
  { table: "analytics_ecommerce.customer_360", column: "lifetime_revenue", nullPct: 0, distinctPct: 100, issue: null },
  { table: "analytics_ecommerce.customer_360", column: "customer_segment", nullPct: 0, distinctPct: 33, issue: null },
  { table: "analytics_ecommerce.product_performance", column: "margin_pct", nullPct: 8.3, distinctPct: 92, issue: "NULL for products with zero sales" },
];

const ACCESS_ENTRIES = [
  { dataset: "raw_ecommerce", role: "WRITER", entity: "projectWriters", type: "Project Writers" },
  { dataset: "raw_ecommerce", role: "OWNER", entity: "projectOwners", type: "Project Owners" },
  { dataset: "raw_ecommerce", role: "READER", entity: "projectReaders", type: "Project Readers" },
  { dataset: "staging_ecommerce", role: "WRITER", entity: "projectWriters", type: "Project Writers" },
  { dataset: "staging_ecommerce", role: "OWNER", entity: "projectOwners", type: "Project Owners" },
  { dataset: "staging_ecommerce", role: "READER", entity: "projectReaders", type: "Project Readers" },
  { dataset: "analytics_ecommerce", role: "WRITER", entity: "projectWriters", type: "Project Writers" },
  { dataset: "analytics_ecommerce", role: "OWNER", entity: "projectOwners", type: "Project Owners" },
  { dataset: "analytics_ecommerce", role: "READER", entity: "allAuthenticatedUsers", type: "All Authenticated" },
  { dataset: "reporting_ecommerce", role: "READER", entity: "allAuthenticatedUsers", type: "All Authenticated" },
  { dataset: "reporting_ecommerce", role: "OWNER", entity: "projectOwners", type: "Project Owners" },
];

// ── Color System ──
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
  if (ds.includes("raw")) return C.raw;
  if (ds.includes("staging")) return C.staging;
  if (ds.includes("analytics")) return C.analytics;
  if (ds.includes("reporting")) return C.reporting;
  return C.textMuted;
};

// ── Components ──

function StatCard({ label, value, sub, color }) {
  return (
    <div style={{
      background: C.surface,
      border: `1px solid ${C.border}`,
      borderRadius: 12,
      padding: "20px 24px",
      minWidth: 160,
      position: "relative",
      overflow: "hidden",
    }}>
      <div style={{
        position: "absolute", top: 0, left: 0, right: 0, height: 3,
        background: `linear-gradient(90deg, ${color || C.accent}, transparent)`,
      }} />
      <div style={{ color: C.textMuted, fontSize: 12, fontWeight: 500, letterSpacing: "0.05em", textTransform: "uppercase", marginBottom: 8 }}>{label}</div>
      <div style={{ color: C.text, fontSize: 28, fontWeight: 700, fontFamily: "'JetBrains Mono', 'SF Mono', monospace", lineHeight: 1 }}>{value}</div>
      {sub && <div style={{ color: C.textDim, fontSize: 12, marginTop: 6 }}>{sub}</div>}
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
        padding: "10px 20px",
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

// ── Lineage Visualization (SVG) ──
function LineageGraph({ highlight }) {
  const layers = ["raw_ecommerce", "staging_ecommerce", "analytics_ecommerce", "reporting_ecommerce"];
  const layerLabels = ["Raw", "Staging", "Analytics", "Reporting"];
  const layerColors = [C.raw, C.staging, C.analytics, C.reporting];

  const tablesByLayer = layers.map(l => TABLES.filter(t => t.dataset === l));
  const colWidth = 210;
  const nodeH = 36;
  const nodeGap = 12;
  const layerGap = 80;
  const headerH = 40;
  const padTop = 50;

  const maxPerLayer = Math.max(...tablesByLayer.map(tl => tl.length));
  const svgH = padTop + headerH + maxPerLayer * (nodeH + nodeGap) + 40;
  const svgW = layers.length * (colWidth + layerGap) - layerGap + 40;

  const pos = {};
  tablesByLayer.forEach((tl, li) => {
    const x = 20 + li * (colWidth + layerGap);
    tl.forEach((t, ti) => {
      const y = padTop + headerH + ti * (nodeH + nodeGap);
      const key = `${t.dataset}.${t.name}`;
      pos[key] = { x, y, w: colWidth, h: nodeH, table: t, layerIdx: li };
    });
  });

  const isHighlighted = (key) => {
    if (!highlight) return true;
    return key === highlight || TABLE_LINEAGE.some(e =>
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
        <filter id="glow">
          <feGaussianBlur stdDeviation="3" result="blur" />
          <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
        </filter>
      </defs>

      {/* Layer headers */}
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
            letterSpacing="0.05em"
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

      {/* Edges */}
      {TABLE_LINEAGE.map((edge, i) => {
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
            filter={hl ? "url(#glow)" : undefined}
          />
        );
      })}

      {/* Nodes */}
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
              {p.table.name}
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

// ── Column-Level Lineage Detail ──
function ColumnLineagePanel() {
  const [selected, setSelected] = useState(null);

  const traceColumn = (col) => {
    const chain = [col];
    let current = col;
    // trace upstream
    const upstream = [];
    let cur = current;
    for (let i = 0; i < 10; i++) {
      const edge = COLUMN_LINEAGE.find(e => e.down === cur);
      if (!edge) break;
      upstream.unshift(edge);
      cur = edge.up;
    }
    // trace downstream
    const downstream = [];
    cur = current;
    for (let i = 0; i < 10; i++) {
      const edge = COLUMN_LINEAGE.find(e => e.up === cur);
      if (!edge) break;
      downstream.push(edge);
      cur = edge.down;
    }
    return { upstream, downstream };
  };

  const uniqueCols = [...new Set([...COLUMN_LINEAGE.map(e => e.up), ...COLUMN_LINEAGE.map(e => e.down)])].sort();
  const trace = selected ? traceColumn(selected) : null;
  const allEdges = trace ? [...trace.upstream, ...trace.downstream] : [];

  return (
    <div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 20 }}>
        {uniqueCols.map(col => {
          const parts = col.split(".");
          const colName = parts[parts.length - 1];
          const layer = parts[0];
          const lc = layer === "raw" ? C.raw : layer === "stg" ? C.staging : layer === "analytics" ? C.analytics : C.reporting;
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
                transition: "all 0.15s",
              }}
            >
              {col}
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
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8, fontSize: 12 }}>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", color: C.text }}>{e.up}</span>
              <span style={{ color: C.accent }}>→</span>
              <Badge color={C.accent} small>{e.transform}</Badge>
              <span style={{ color: C.accent }}>→</span>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", color: C.text }}>{e.down}</span>
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

// ── Quality Panel ──
function QualityPanel() {
  return (
    <div style={{ display: "grid", gap: 8 }}>
      <div style={{
        display: "grid",
        gridTemplateColumns: "2fr 1.5fr 80px 80px 2fr",
        gap: 8,
        padding: "8px 12px",
        fontSize: 10,
        color: C.textDim,
        textTransform: "uppercase",
        letterSpacing: "0.06em",
        fontWeight: 600,
        borderBottom: `1px solid ${C.border}`,
      }}>
        <span>Table.Column</span>
        <span>Type Check</span>
        <span>Null %</span>
        <span>Distinct %</span>
        <span>Issues</span>
      </div>
      {QUALITY_PROFILES.map((q, i) => {
        const nullColor = q.nullPct === 0 ? C.quality : q.nullPct < 10 ? C.qualityWarn : C.qualityBad;
        const distinctColor = q.distinctPct > 80 ? C.quality : q.distinctPct > 50 ? C.qualityWarn : C.qualityBad;
        return (
          <div key={i} style={{
            display: "grid",
            gridTemplateColumns: "2fr 1.5fr 80px 80px 2fr",
            gap: 8,
            padding: "10px 12px",
            background: i % 2 === 0 ? C.surface : "transparent",
            borderRadius: 6,
            alignItems: "center",
            fontSize: 12,
          }}>
            <span style={{ fontFamily: "'JetBrains Mono', monospace", color: C.text, fontSize: 11 }}>
              {q.table.split(".")[1]}.{q.column}
            </span>
            <Badge color={layerColor(q.table)} small>
              {q.table.split(".")[0].replace("_ecommerce", "")}
            </Badge>
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <div style={{
                width: 40, height: 6, background: C.border, borderRadius: 3, overflow: "hidden",
              }}>
                <div style={{ width: `${q.nullPct}%`, height: "100%", background: nullColor, borderRadius: 3 }} />
              </div>
              <span style={{ color: nullColor, fontFamily: "monospace", fontSize: 11 }}>{q.nullPct}%</span>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <div style={{
                width: 40, height: 6, background: C.border, borderRadius: 3, overflow: "hidden",
              }}>
                <div style={{ width: `${q.distinctPct}%`, height: "100%", background: distinctColor, borderRadius: 3 }} />
              </div>
              <span style={{ color: distinctColor, fontFamily: "monospace", fontSize: 11 }}>{q.distinctPct}%</span>
            </div>
            <span style={{ color: q.issue ? C.qualityWarn : C.textDim, fontSize: 11 }}>
              {q.issue || "—"}
            </span>
          </div>
        );
      })}
    </div>
  );
}

// ── PII Panel ──
function PIIPanel() {
  const byClassification = {};
  PII_COLUMNS.forEach(p => {
    if (!byClassification[p.classification]) byClassification[p.classification] = [];
    byClassification[p.classification].push(p);
  });

  return (
    <div style={{ display: "grid", gap: 20 }}>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
        {Object.entries(byClassification).map(([cls, items]) => (
          <div key={cls} style={{
            background: C.surface,
            border: `1px solid ${C.pii}30`,
            borderRadius: 10,
            padding: "14px 18px",
            minWidth: 180,
          }}>
            <div style={{ color: C.pii, fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 8 }}>
              {cls}
            </div>
            <div style={{ fontSize: 24, fontWeight: 700, color: C.text, fontFamily: "'JetBrains Mono', monospace" }}>
              {items.length}
            </div>
            <div style={{ fontSize: 11, color: C.textDim, marginTop: 4 }}>
              across {new Set(items.map(i => i.table)).size} tables
            </div>
          </div>
        ))}
      </div>

      <div style={{ display: "grid", gap: 4 }}>
        <div style={{
          display: "grid",
          gridTemplateColumns: "1.5fr 1fr 1fr 1.2fr 80px",
          gap: 8,
          padding: "8px 12px",
          fontSize: 10,
          color: C.textDim,
          textTransform: "uppercase",
          letterSpacing: "0.06em",
          fontWeight: 600,
          borderBottom: `1px solid ${C.border}`,
        }}>
          <span>Column</span>
          <span>Table</span>
          <span>Dataset</span>
          <span>Classification</span>
          <span>Confidence</span>
        </div>
        {PII_COLUMNS.map((p, i) => (
          <div key={i} style={{
            display: "grid",
            gridTemplateColumns: "1.5fr 1fr 1fr 1.2fr 80px",
            gap: 8,
            padding: "8px 12px",
            background: i % 2 === 0 ? C.surface : "transparent",
            borderRadius: 6,
            alignItems: "center",
            fontSize: 12,
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

// ── Access Panel ──
function AccessPanel() {
  return (
    <div style={{ display: "grid", gap: 16 }}>
      {DATASETS.map(ds => {
        const entries = ACCESS_ENTRIES.filter(a => a.dataset === ds.id);
        return (
          <div key={ds.id} style={{
            background: C.surface,
            border: `1px solid ${C.border}`,
            borderRadius: 10,
            padding: 16,
          }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12 }}>
              <div style={{ width: 10, height: 10, borderRadius: "50%", background: ds.color }} />
              <span style={{ color: C.text, fontWeight: 600, fontSize: 14 }}>{ds.label}</span>
              <span style={{ color: C.textDim, fontSize: 12, fontFamily: "'JetBrains Mono', monospace" }}>{ds.id}</span>
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
              {entries.map((e, i) => (
                <div key={i} style={{
                  display: "flex", alignItems: "center", gap: 8,
                  background: C.bg, borderRadius: 8, padding: "6px 12px",
                }}>
                  <Badge color={e.role === "OWNER" ? C.pii : e.role === "WRITER" ? C.qualityWarn : C.quality} small>
                    {e.role}
                  </Badge>
                  <span style={{ fontSize: 12, color: C.text }}>{e.type}</span>
                </div>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ── Schema Browser ──
function SchemaBrowser() {
  const [selectedTable, setSelectedTable] = useState(null);
  const [filterDataset, setFilterDataset] = useState(null);

  const filteredTables = filterDataset ? TABLES.filter(t => t.dataset === filterDataset) : TABLES;

  const SCHEMAS = {
    "raw_ecommerce.customers": [
      { name: "customer_id", type: "STRING", desc: "Unique customer identifier from CRM" },
      { name: "first_name", type: "STRING", desc: "Customer first name" },
      { name: "last_name", type: "STRING", desc: "Customer last name" },
      { name: "email", type: "STRING", desc: "Customer email address — PII", pii: true },
      { name: "phone", type: "STRING", desc: "Customer phone number — PII", pii: true },
      { name: "date_of_birth", type: "STRING", desc: "Date of birth as string — PII", pii: true },
      { name: "gender", type: "STRING", desc: "Customer gender" },
      { name: "city", type: "STRING", desc: "City" },
      { name: "state", type: "STRING", desc: "State/province code" },
      { name: "country", type: "STRING", desc: "ISO country code" },
      { name: "postal_code", type: "STRING", desc: "Postal/ZIP code — PII", pii: true },
      { name: "created_at", type: "STRING", desc: "Account creation timestamp" },
      { name: "_ingested_at", type: "TIMESTAMP", desc: "Ingestion timestamp" },
    ],
    "analytics_ecommerce.customer_360": [
      { name: "customer_id", type: "STRING", desc: "Unique customer identifier" },
      { name: "first_name", type: "STRING", desc: "Customer first name", pii: true },
      { name: "last_name", type: "STRING", desc: "Customer last name", pii: true },
      { name: "email", type: "STRING", desc: "Customer email", pii: true },
      { name: "city", type: "STRING", desc: "City" },
      { name: "state", type: "STRING", desc: "State" },
      { name: "account_age_days", type: "INT64", desc: "Days since account creation" },
      { name: "total_orders", type: "INT64", desc: "Total number of orders" },
      { name: "lifetime_revenue", type: "NUMERIC", desc: "Total revenue from customer" },
      { name: "avg_order_value", type: "NUMERIC", desc: "Average order value" },
      { name: "completed_orders", type: "INT64", desc: "Number of completed orders" },
      { name: "returned_orders", type: "INT64", desc: "Number of returned orders" },
      { name: "customer_segment", type: "STRING", desc: "VIP / Regular / Occasional / Prospect" },
    ],
    "analytics_ecommerce.product_performance": [
      { name: "product_id", type: "STRING", desc: "Product SKU" },
      { name: "product_name", type: "STRING", desc: "Display name" },
      { name: "category", type: "STRING", desc: "Product category" },
      { name: "brand", type: "STRING", desc: "Brand name" },
      { name: "current_price", type: "NUMERIC", desc: "Current unit price" },
      { name: "total_units_sold", type: "INT64", desc: "Total units sold" },
      { name: "total_revenue", type: "NUMERIC", desc: "Total revenue generated" },
      { name: "gross_margin", type: "NUMERIC", desc: "Revenue minus cost" },
      { name: "margin_pct", type: "FLOAT64", desc: "Margin as percentage" },
      { name: "unique_buyers", type: "INT64", desc: "Distinct customers who purchased" },
    ],
  };

  return (
    <div style={{ display: "grid", gridTemplateColumns: "280px 1fr", gap: 16, minHeight: 400 }}>
      <div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginBottom: 12 }}>
          <button
            onClick={() => setFilterDataset(null)}
            style={{
              background: !filterDataset ? C.accent + "20" : "transparent",
              color: !filterDataset ? C.accent : C.textDim,
              border: "none", borderRadius: 6, padding: "4px 10px", fontSize: 11, cursor: "pointer", fontFamily: "inherit",
            }}
          >All</button>
          {DATASETS.map(d => (
            <button
              key={d.id}
              onClick={() => setFilterDataset(d.id)}
              style={{
                background: filterDataset === d.id ? d.color + "20" : "transparent",
                color: filterDataset === d.id ? d.color : C.textDim,
                border: "none", borderRadius: 6, padding: "4px 10px", fontSize: 11, cursor: "pointer", fontFamily: "inherit",
              }}
            >{d.label}</button>
          ))}
        </div>
        <div style={{ display: "grid", gap: 4, maxHeight: 500, overflowY: "auto" }}>
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
                  borderRadius: 8,
                  padding: "8px 12px",
                  textAlign: "left",
                  cursor: "pointer",
                  transition: "all 0.15s",
                  fontFamily: "inherit",
                }}
              >
                <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                  <div style={{ width: 6, height: 6, borderRadius: "50%", background: layerColor(t.dataset) }} />
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: C.text }}>{t.name}</span>
                  {t.type === "VIEW" && <Badge color={C.reporting} small>VIEW</Badge>}
                  {t.pii && <Badge color={C.pii} small>PII</Badge>}
                </div>
                <div style={{ fontSize: 10, color: C.textDim, marginTop: 4, marginLeft: 12 }}>
                  {t.rows} rows · {t.columns} cols
                </div>
              </button>
            );
          })}
        </div>
      </div>

      <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
        {selectedTable ? (
          <>
            <div style={{ marginBottom: 16 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
                <span style={{
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: 16,
                  fontWeight: 700,
                  color: C.text,
                }}>{selectedTable}</span>
                {TABLES.find(t => `${t.dataset}.${t.name}` === selectedTable)?.pii && (
                  <Badge color={C.pii}>PII</Badge>
                )}
              </div>
              <div style={{ fontSize: 13, color: C.textMuted, lineHeight: 1.5 }}>
                {TABLES.find(t => `${t.dataset}.${t.name}` === selectedTable)?.description}
              </div>
              <div style={{ display: "flex", gap: 12, marginTop: 10 }}>
                {Object.entries(TABLES.find(t => `${t.dataset}.${t.name}` === selectedTable)?.labels || {}).map(([k, v]) => (
                  <span key={k} style={{
                    fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: C.textDim,
                    background: C.bg, padding: "3px 8px", borderRadius: 4,
                  }}>{k}={v}</span>
                ))}
              </div>
            </div>

            {SCHEMAS[selectedTable] ? (
              <div style={{ display: "grid", gap: 2 }}>
                <div style={{
                  display: "grid", gridTemplateColumns: "140px 100px 1fr 50px",
                  padding: "6px 10px", fontSize: 10, color: C.textDim, textTransform: "uppercase",
                  letterSpacing: "0.06em", fontWeight: 600, borderBottom: `1px solid ${C.border}`,
                }}>
                  <span>Column</span><span>Type</span><span>Description</span><span>PII</span>
                </div>
                {SCHEMAS[selectedTable].map((col, i) => (
                  <div key={col.name} style={{
                    display: "grid", gridTemplateColumns: "140px 100px 1fr 50px",
                    padding: "8px 10px", borderRadius: 4,
                    background: i % 2 === 0 ? C.bg : "transparent",
                    alignItems: "center",
                  }}>
                    <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: C.text }}>{col.name}</span>
                    <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: C.accent }}>{col.type}</span>
                    <span style={{ fontSize: 12, color: C.textMuted }}>{col.desc}</span>
                    <span>{col.pii ? <Badge color={C.pii} small>PII</Badge> : ""}</span>
                  </div>
                ))}
              </div>
            ) : (
              <div style={{ color: C.textDim, fontSize: 13, padding: 20, textAlign: "center" }}>
                Schema details available for key tables (customers, customer_360, product_performance)
              </div>
            )}
          </>
        ) : (
          <div style={{ color: C.textDim, fontSize: 14, padding: 40, textAlign: "center" }}>
            ← Select a table to explore its schema and column details
          </div>
        )}
      </div>
    </div>
  );
}

// ── Main App ──
export default function MetadataDashboard() {
  const [activeTab, setActiveTab] = useState("lineage");
  const [lineageHighlight, setLineageHighlight] = useState(null);

  const totalCols = TABLES.reduce((s, t) => s + t.columns, 0);
  const totalRows = TABLES.reduce((s, t) => s + t.rows, 0);

  return (
    <div style={{
      fontFamily: "'Inter', 'Segoe UI', system-ui, sans-serif",
      background: C.bg,
      color: C.text,
      minHeight: "100vh",
      padding: "24px 32px",
    }}>
      {/* Header */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 8 }}>
          <div style={{
            width: 36, height: 36, borderRadius: 10,
            background: `linear-gradient(135deg, ${C.accent}, ${C.reporting})`,
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 18, fontWeight: 800, color: "#fff",
          }}>M</div>
          <div>
            <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700, letterSpacing: "-0.02em" }}>
              BigQuery Metadata Explorer
            </h1>
            <div style={{ fontSize: 12, color: C.textDim, marginTop: 2 }}>
              bq-metadata-spike · E-Commerce Data Stack · Extracted {new Date().toLocaleDateString()}
            </div>
          </div>
        </div>
      </div>

      {/* Stats */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 12, marginBottom: 28 }}>
        <StatCard label="Datasets" value="4" sub="raw → staging → analytics → reporting" color={C.accent} />
        <StatCard label="Tables & Views" value={TABLES.length} sub={`${TABLES.filter(t => t.type === "VIEW").length} views, ${TABLES.filter(t => t.type === "TABLE").length} tables`} color={C.analytics} />
        <StatCard label="Columns" value={totalCols} sub="across all layers" color={C.staging} />
        <StatCard label="Lineage Edges" value={TABLE_LINEAGE.length} sub={`+ ${COLUMN_LINEAGE.length} column-level`} color={C.quality} />
        <StatCard label="PII Columns" value={PII_COLUMNS.length} sub={`${new Set(PII_COLUMNS.map(p => p.classification)).size} classification types`} color={C.pii} />
        <StatCard label="Access Rules" value={ACCESS_ENTRIES.length} sub="across 4 datasets" color={C.reporting} />
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 6, marginBottom: 24, flexWrap: "wrap" }}>
        <TabButton active={activeTab === "lineage"} onClick={() => setActiveTab("lineage")} color={C.accent}>
          ◈ Table Lineage
        </TabButton>
        <TabButton active={activeTab === "collineage"} onClick={() => setActiveTab("collineage")} color={C.quality}>
          ⟡ Column Lineage
        </TabButton>
        <TabButton active={activeTab === "schema"} onClick={() => setActiveTab("schema")} color={C.analytics}>
          ▤ Schema Browser
        </TabButton>
        <TabButton active={activeTab === "quality"} onClick={() => setActiveTab("quality")} color={C.quality}>
          ◉ Data Quality
        </TabButton>
        <TabButton active={activeTab === "pii"} onClick={() => setActiveTab("pii")} color={C.pii}>
          ◐ PII Classification
        </TabButton>
        <TabButton active={activeTab === "access"} onClick={() => setActiveTab("access")} color={C.reporting}>
          ◈ Access & Permissions
        </TabButton>
      </div>

      {/* Content */}
      <div style={{
        background: C.surface + "80",
        border: `1px solid ${C.border}`,
        borderRadius: 14,
        padding: 24,
      }}>
        {activeTab === "lineage" && (
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
              <div>
                <h3 style={{ margin: 0, fontSize: 16, fontWeight: 600 }}>Table Lineage Graph</h3>
                <p style={{ margin: "4px 0 0", fontSize: 12, color: C.textDim }}>
                  Click a table node to highlight its upstream and downstream dependencies
                </p>
              </div>
              {lineageHighlight && (
                <button
                  onClick={() => setLineageHighlight(null)}
                  style={{
                    background: C.bg, border: `1px solid ${C.border}`, borderRadius: 6,
                    padding: "4px 12px", color: C.textMuted, fontSize: 11, cursor: "pointer", fontFamily: "inherit",
                  }}
                >Clear filter</button>
              )}
            </div>
            <div style={{ overflowX: "auto", paddingBottom: 8 }}>
              <div onClick={(e) => {
                const t = e.target.closest("g[opacity]");
                if (!t) return;
                const text = t.querySelector("text");
                if (!text) return;
                const name = text.textContent;
                const table = TABLES.find(tb => tb.name === name);
                if (table) {
                  const key = `${table.dataset}.${table.name}`;
                  setLineageHighlight(lineageHighlight === key ? null : key);
                }
              }}>
                <LineageGraph highlight={lineageHighlight} />
              </div>
            </div>
          </div>
        )}

        {activeTab === "collineage" && (
          <div>
            <h3 style={{ margin: "0 0 4px", fontSize: 16, fontWeight: 600 }}>Column-Level Lineage</h3>
            <p style={{ margin: "0 0 16px", fontSize: 12, color: C.textDim }}>
              Trace how individual columns flow and transform across the data stack
            </p>
            <ColumnLineagePanel />
          </div>
        )}

        {activeTab === "schema" && (
          <div>
            <h3 style={{ margin: "0 0 4px", fontSize: 16, fontWeight: 600 }}>Schema Browser</h3>
            <p style={{ margin: "0 0 16px", fontSize: 12, color: C.textDim }}>
              Explore tables, columns, types, descriptions, and labels across all datasets
            </p>
            <SchemaBrowser />
          </div>
        )}

        {activeTab === "quality" && (
          <div>
            <h3 style={{ margin: "0 0 4px", fontSize: 16, fontWeight: 600 }}>Data Quality Signals</h3>
            <p style={{ margin: "0 0 16px", fontSize: 12, color: C.textDim }}>
              Column-level profiling: null rates, distinct value percentages, and detected issues
            </p>
            <QualityPanel />
          </div>
        )}

        {activeTab === "pii" && (
          <div>
            <h3 style={{ margin: "0 0 4px", fontSize: 16, fontWeight: 600 }}>PII Classification</h3>
            <p style={{ margin: "0 0 16px", fontSize: 12, color: C.textDim }}>
              Auto-detected personally identifiable information across the data stack
            </p>
            <PIIPanel />
          </div>
        )}

        {activeTab === "access" && (
          <div>
            <h3 style={{ margin: "0 0 4px", fontSize: 16, fontWeight: 600 }}>Access & Permissions</h3>
            <p style={{ margin: "0 0 16px", fontSize: 12, color: C.textDim }}>
              Dataset-level access control entries from BigQuery IAM
            </p>
            <AccessPanel />
          </div>
        )}
      </div>

      {/* Footer */}
      <div style={{
        marginTop: 24, padding: "16px 0",
        borderTop: `1px solid ${C.border}`,
        display: "flex", justifyContent: "space-between",
        fontSize: 11, color: C.textDim,
      }}>
        <span>BigQuery Metadata Spike · Prototype</span>
        <span>4 datasets · {TABLES.length} assets · {TABLE_LINEAGE.length + COLUMN_LINEAGE.length} lineage edges · {PII_COLUMNS.length} PII columns</span>
      </div>
    </div>
  );
}
