
import { useState, useEffect } from "react";
import axios from "axios";
import {
  BarChart, Bar, XAxis, YAxis,
  CartesianGrid, Tooltip, ResponsiveContainer
} from "recharts";
import {
  Database, Activity, Shield, Play, Search,
  AlertTriangle, CheckCircle, XCircle, Upload,
  RefreshCw, Zap, TrendingUp, ChevronRight, Table, Cpu
} from "lucide-react";

const API = "https://dataforge-ai-production-d2c7.up.railway.app";

const theme = {
  bg: "#050810",
  surface: "#0D1117",
  card: "#111827",
  border: "#1F2937",
  accent: "#6366F1",
  accent2: "#8B5CF6",
  green: "#10B981",
  red: "#EF4444",
  yellow: "#F59E0B",
  blue: "#3B82F6",
  text: "#F9FAFB",
  muted: "#6B7280",
  subtle: "#374151",
};

const styles = `
  @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');
  * { margin:0; padding:0; box-sizing:border-box; }
  body { background:${theme.bg}; color:${theme.text}; font-family:'Syne',sans-serif; overflow-x:hidden; }
  ::-webkit-scrollbar { width:4px; }
  ::-webkit-scrollbar-track { background:${theme.bg}; }
  ::-webkit-scrollbar-thumb { background:${theme.accent}; border-radius:2px; }
  .mono { font-family:'JetBrains Mono',monospace; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
  @keyframes slideIn { from{opacity:0;transform:translateY(20px)} to{opacity:1;transform:translateY(0)} }
  @keyframes glow { 0%,100%{box-shadow:0 0 20px rgba(99,102,241,0.3)} 50%{box-shadow:0 0 40px rgba(99,102,241,0.6)} }
  @keyframes spin { from{transform:rotate(0deg)} to{transform:rotate(360deg)} }
  .animate-in { animation:slideIn 0.4s ease forwards; }
  .glow { animation:glow 3s ease infinite; }
  .spinning { animation:spin 1s linear infinite; }
  .grid-bg {
    background-image: linear-gradient(rgba(99,102,241,0.03) 1px, transparent 1px),
    linear-gradient(90deg, rgba(99,102,241,0.03) 1px, transparent 1px);
    background-size: 40px 40px;
  }
`;

function HealthRing({ score, size = 80 }) {
  const r = size / 2 - 8;
  const circ = 2 * Math.PI * r;
  const dash = (score / 100) * circ;
  const color = score >= 90 ? theme.green : score >= 70 ? theme.yellow : theme.red;
  return (
    <svg width={size} height={size} style={{ transform: "rotate(-90deg)" }}>
      <circle cx={size/2} cy={size/2} r={r} fill="none" stroke={theme.border} strokeWidth="6"/>
      <circle cx={size/2} cy={size/2} r={r} fill="none" stroke={color} strokeWidth="6"
        strokeDasharray={`${dash} ${circ}`} strokeLinecap="round"
        style={{ transition: "stroke-dasharray 1s ease" }}/>
      <text x={size/2} y={size/2+1} textAnchor="middle" dominantBaseline="middle"
        fill={color} fontSize="14" fontWeight="700" fontFamily="Syne"
        style={{ transform: `rotate(90deg) translate(0px, -${size}px)` }}>
        {score}
      </text>
    </svg>
  );
}

function MetricCard({ icon: Icon, label, value, sub, color, delay = 0 }) {
  return (
    <div className="animate-in" style={{
      animationDelay: `${delay}ms`,
      background: `linear-gradient(135deg, ${theme.card}, ${theme.surface})`,
      border: `1px solid ${theme.border}`,
      borderRadius: 16, padding: "24px",
      position: "relative", overflow: "hidden"
    }}>
      <div style={{
        position: "absolute", top: 0, right: 0,
        width: 80, height: 80,
        background: `radial-gradient(circle, ${color}22, transparent)`,
        borderRadius: "0 16px 0 80px"
      }}/>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12 }}>
        <div style={{ background: `${color}22`, borderRadius: 10, padding: 8 }}>
          <Icon size={18} color={color}/>
        </div>
        <span style={{ color: theme.muted, fontSize: 12, letterSpacing: 1, textTransform: "uppercase" }}>{label}</span>
      </div>
      <div style={{ fontSize: 32, fontWeight: 800, color: theme.text }}>{value}</div>
      {sub && <div style={{ fontSize: 12, color: theme.muted, marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

function Badge({ type }) {
  const configs = {
    HEALTHY: { color: theme.green, bg: "#10B98122", label: "HEALTHY" },
    WARNING: { color: theme.yellow, bg: "#F59E0B22", label: "WARNING" },
    CRITICAL: { color: theme.red, bg: "#EF444422", label: "CRITICAL" },
    SUCCESS: { color: theme.green, bg: "#10B98122", label: "SUCCESS" },
    FAILED: { color: theme.red, bg: "#EF444422", label: "FAILED" },
    HIGH: { color: theme.red, bg: "#EF444422", label: "HIGH" },
    MEDIUM: { color: theme.yellow, bg: "#F59E0B22", label: "MEDIUM" },
    LOW: { color: theme.blue, bg: "#3B82F622", label: "LOW" },
  };
  const c = configs[type] || configs.LOW;
  return (
    <span style={{
      background: c.bg, color: c.color,
      padding: "3px 10px", borderRadius: 20,
      fontSize: 10, fontWeight: 700, letterSpacing: 1,
      border: `1px solid ${c.color}44`
    }}>{c.label}</span>
  );
}

function Sidebar({ active, setActive }) {
  const nav = [
    { id: "dashboard", icon: Activity, label: "Dashboard" },
    { id: "sources", icon: Database, label: "Sources" },
    { id: "quality", icon: Shield, label: "Quality" },
    { id: "pipeline", icon: Cpu, label: "Pipeline" },
    { id: "transform", icon: Zap, label: "Transform" },
    { id: "query", icon: Search, label: "AI Query" },
  ];
  return (
    <div style={{
      width: 220, minHeight: "100vh",
      background: theme.surface,
      borderRight: `1px solid ${theme.border}`,
      padding: "24px 16px",
      position: "fixed", left: 0, top: 0,
      display: "flex", flexDirection: "column",
      zIndex: 100
    }}>
      <div style={{ marginBottom: 32, padding: "0 8px" }}>
        <div style={{
          background: `linear-gradient(135deg, ${theme.accent}, ${theme.accent2})`,
          borderRadius: 12, padding: "10px 14px", marginBottom: 8
        }}>
          <div style={{ fontSize: 16, fontWeight: 800, letterSpacing: 1 }}>DataForge</div>
          <div style={{ fontSize: 10, color: "rgba(255,255,255,0.7)", letterSpacing: 2 }}>AI · v2.0</div>
        </div>
        <div style={{ fontSize: 10, color: theme.muted, textAlign: "center", letterSpacing: 1 }}>E.L.F Beauty Pipeline</div>
      </div>
      <nav style={{ flex: 1, display: "flex", flexDirection: "column", gap: 4 }}>
        {nav.map(item => (
          <button key={item.id} onClick={() => setActive(item.id)} style={{
            display: "flex", alignItems: "center", gap: 12,
            padding: "12px 14px", borderRadius: 12, border: "none",
            cursor: "pointer", textAlign: "left", width: "100%",
            background: active === item.id
              ? `linear-gradient(135deg, ${theme.accent}22, ${theme.accent2}22)`
              : "transparent",
            color: active === item.id ? theme.accent : theme.muted,
            borderLeft: active === item.id ? `3px solid ${theme.accent}` : "3px solid transparent",
            transition: "all 0.2s",
            fontSize: 13, fontWeight: active === item.id ? 700 : 400,
            fontFamily: "Syne, sans-serif"
          }}>
            <item.icon size={16}/>
            {item.label}
            {active === item.id && <ChevronRight size={12} style={{ marginLeft: "auto" }}/>}
          </button>
        ))}
      </nav>
      <div style={{
        padding: "12px 14px", background: `${theme.green}11`,
        borderRadius: 12, border: `1px solid ${theme.green}33`
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <div style={{
            width: 8, height: 8, borderRadius: "50%",
            background: theme.green, animation: "pulse 2s infinite"
          }}/>
          <span style={{ fontSize: 11, color: theme.green, fontWeight: 600 }}>LIVE</span>
        </div>
        <div style={{ fontSize: 10, color: theme.muted, marginTop: 4 }}>Pipeline Active</div>
      </div>
    </div>
  );
}

function Dashboard() {
  const [status, setStatus] = useState(null);
  const [runs, setRuns] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      axios.get(`${API}/status`),
      axios.get(`${API}/pipeline/runs`),
    ]).then(([s, r]) => {
      setStatus(s.data);
      setRuns(r.data.pipeline_runs || []);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  const chartData = runs.slice(0, 10).reverse().map((r, i) => ({
    name: `Run ${i+1}`,
    loaded: r.records_loaded || 0,
    quarantined: r.records_quarantined || 0,
  }));

  const successRate = runs.length > 0
    ? Math.round(runs.filter(r => r.status === "SUCCESS").length / runs.length * 100)
    : 0;

  if (loading) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "60vh" }}>
      <div style={{ textAlign: "center" }}>
        <RefreshCw size={32} color={theme.accent} className="spinning"/>
        <div style={{ color: theme.muted, marginTop: 12, fontSize: 13 }}>Loading pipeline data...</div>
      </div>
    </div>
  );

  return (
    <div style={{ animation: "slideIn 0.4s ease" }}>
      <div style={{ marginBottom: 32 }}>
        <h1 style={{ fontSize: 28, fontWeight: 800, marginBottom: 6 }}>Pipeline Dashboard</h1>
        <p style={{ color: theme.muted, fontSize: 13 }}>Real-time monitoring for E.L.F Beauty data pipeline</p>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16, marginBottom: 24 }}>
        <MetricCard icon={Play} label="Total Runs" value={status?.total_pipeline_runs || 0} sub="All time" color={theme.accent} delay={0}/>
        <MetricCard icon={CheckCircle} label="Successful" value={status?.successful_runs || 0} sub={`${successRate}% success rate`} color={theme.green} delay={100}/>
        <MetricCard icon={XCircle} label="Failed" value={status?.failed_runs || 0} sub="Needs attention" color={theme.red} delay={200}/>
        <MetricCard icon={TrendingUp} label="Records Loaded" value={(status?.total_records_loaded || 0).toLocaleString()} sub="To Snowflake Bronze" color={theme.blue} delay={300}/>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: 16, marginBottom: 24 }}>
        <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24 }}>
          <h3 style={{ marginBottom: 20, fontSize: 14, fontWeight: 700, color: theme.muted, letterSpacing: 1, textTransform: "uppercase" }}>Records Loaded Per Run</h3>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={theme.border}/>
              <XAxis dataKey="name" stroke={theme.muted} fontSize={11}/>
              <YAxis stroke={theme.muted} fontSize={11}/>
              <Tooltip contentStyle={{ background: theme.card, border: `1px solid ${theme.border}`, borderRadius: 8, fontSize: 12 }}/>
              <Bar dataKey="loaded" fill={theme.accent} radius={[4,4,0,0]}/>
              <Bar dataKey="quarantined" fill={theme.red} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24 }}>
          <h3 style={{ marginBottom: 20, fontSize: 14, fontWeight: 700, color: theme.muted, letterSpacing: 1, textTransform: "uppercase" }}>Pipeline Health</h3>
          <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            {[
              { label: "Success Rate", value: successRate, color: theme.green },
              { label: "Data Quality", value: 75, color: theme.yellow },
              { label: "Uptime", value: 99, color: theme.green },
            ].map(item => (
              <div key={item.label}>
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
                  <span style={{ fontSize: 12, color: theme.muted }}>{item.label}</span>
                  <span style={{ fontSize: 12, fontWeight: 700, color: item.color }}>{item.value}%</span>
                </div>
                <div style={{ background: theme.border, borderRadius: 4, height: 6 }}>
                  <div style={{
                    background: `linear-gradient(90deg, ${item.color}, ${item.color}88)`,
                    width: `${item.value}%`, height: "100%",
                    borderRadius: 4, transition: "width 1s ease"
                  }}/>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
      <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24 }}>
        <h3 style={{ marginBottom: 20, fontSize: 14, fontWeight: 700, color: theme.muted, letterSpacing: 1, textTransform: "uppercase" }}>Recent Pipeline Runs</h3>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: `1px solid ${theme.border}` }}>
              {["Run ID", "Source", "Status", "Fetched", "Loaded", "Duration", "Time"].map(h => (
                <th key={h} style={{ padding: "8px 12px", textAlign: "left", fontSize: 11, color: theme.muted, fontWeight: 600, letterSpacing: 1, textTransform: "uppercase" }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {runs.slice(0, 8).map((run, i) => (
              <tr key={i} style={{ borderBottom: `1px solid ${theme.border}22`, transition: "background 0.2s" }}
                onMouseEnter={e => e.currentTarget.style.background = theme.subtle + "44"}
                onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
                <td style={{ padding: "12px", fontSize: 12 }} className="mono">{run.run_id}</td>
                <td style={{ padding: "12px", fontSize: 12, color: theme.muted }}>{run.source_name}</td>
                <td style={{ padding: "12px" }}><Badge type={run.status}/></td>
                <td style={{ padding: "12px", fontSize: 12 }}>{run.records_fetched || 0}</td>
                <td style={{ padding: "12px", fontSize: 12, color: theme.green }}>{run.records_loaded || 0}</td>
                <td style={{ padding: "12px", fontSize: 12, color: theme.muted }}>{run.duration_seconds}s</td>
                <td style={{ padding: "12px", fontSize: 11, color: theme.muted }}>{run.start_time?.slice(0, 16)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function Sources() {
  const [mysqlTables, setMysqlTables] = useState([]);
  const [bronzeTables, setBronzeTables] = useState([]);
  const [syncing, setSyncing] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [message, setMessage] = useState("");
  const [file, setFile] = useState(null);
  const [tableName, setTableName] = useState("ELF_PRODUCTS");

  useEffect(() => {
    axios.get(`${API}/mysql/tables`).then(r => setMysqlTables(r.data.mysql_tables || []));
    axios.get(`${API}/bronze/tables`).then(r => setBronzeTables(r.data.bronze_tables || []));
  }, []);

  const syncAll = () => {
    setSyncing(true);
    setMessage("Syncing all MySQL tables to Snowflake Bronze...");
    axios.post(`${API}/mysql/sync-all`).then(r => {
      setMessage(`✅ Synced ${r.data.tables_synced} tables successfully!`);
      axios.get(`${API}/bronze/tables`).then(r => setBronzeTables(r.data.bronze_tables || []));
      setSyncing(false);
    }).catch(() => { setMessage("❌ Sync failed"); setSyncing(false); });
  };

  const uploadCSV = () => {
    if (!file) return;
    setUploading(true);
    setMessage("Uploading and loading to Bronze...");
    const formData = new FormData();
    formData.append("file", file);
    axios.post(`${API}/upload/csv?table_name=${tableName}`, formData)
      .then(r => {
        setMessage(`✅ Loaded ${r.data.records_loaded} records to ${r.data.table_created}. Health: ${r.data.health_score}/100`);
        setUploading(false);
      }).catch(() => { setMessage("❌ Upload failed"); setUploading(false); });
  };

  return (
    <div style={{ animation: "slideIn 0.4s ease" }}>
      <div style={{ marginBottom: 32 }}>
        <h1 style={{ fontSize: 28, fontWeight: 800, marginBottom: 6 }}>Data Sources</h1>
        <p style={{ color: theme.muted, fontSize: 13 }}>Manage all pipeline connectors and data sources</p>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 24 }}>
        <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24 }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 20 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              <div style={{ background: `${theme.blue}22`, padding: 10, borderRadius: 12 }}>
                <Database size={20} color={theme.blue}/>
              </div>
              <div>
                <div style={{ fontWeight: 700 }}>MySQL Connector</div>
                <div style={{ fontSize: 12, color: theme.muted }}>Railway Cloud Database</div>
              </div>
            </div>
            <Badge type="HEALTHY"/>
          </div>
          <div style={{ marginBottom: 16 }}>
            {mysqlTables.map(t => (
              <div key={t} style={{
                display: "flex", alignItems: "center", justifyContent: "space-between",
                padding: "8px 12px", background: theme.surface, borderRadius: 8, marginBottom: 6
              }}>
                <span style={{ fontSize: 12 }} className="mono">{t}</span>
                <CheckCircle size={14} color={theme.green}/>
              </div>
            ))}
          </div>
          <button onClick={syncAll} disabled={syncing} style={{
            width: "100%", padding: "12px", borderRadius: 10, border: "none",
            background: syncing ? theme.subtle : `linear-gradient(135deg, ${theme.accent}, ${theme.accent2})`,
            color: theme.text, cursor: syncing ? "not-allowed" : "pointer",
            fontWeight: 700, fontSize: 13, fontFamily: "Syne",
            display: "flex", alignItems: "center", justifyContent: "center", gap: 8
          }}>
            <RefreshCw size={14} className={syncing ? "spinning" : ""}/>
            {syncing ? "Syncing..." : "Sync All Tables"}
          </button>
        </div>
        <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20 }}>
            <div style={{ background: `${theme.green}22`, padding: 10, borderRadius: 12 }}>
              <Upload size={20} color={theme.green}/>
            </div>
            <div>
              <div style={{ fontWeight: 700 }}>CSV / Excel Upload</div>
              <div style={{ fontSize: 12, color: theme.muted }}>Upload files to Bronze layer</div>
            </div>
          </div>
          <input type="text" value={tableName} onChange={e => setTableName(e.target.value)}
            placeholder="Table name" style={{
              width: "100%", padding: "10px 14px",
              background: theme.surface, border: `1px solid ${theme.border}`,
              borderRadius: 8, color: theme.text, marginBottom: 10,
              fontSize: 13, fontFamily: "Syne"
            }}/>
          <div style={{
            border: `2px dashed ${theme.border}`, borderRadius: 10,
            padding: "20px", textAlign: "center", marginBottom: 12, cursor: "pointer"
          }} onDragOver={e => e.preventDefault()}
            onDrop={e => { e.preventDefault(); setFile(e.dataTransfer.files[0]); }}>
            <input type="file" accept=".csv,.xlsx,.xls" style={{ display: "none" }}
              id="fileInput" onChange={e => setFile(e.target.files[0])}/>
            <label htmlFor="fileInput" style={{ cursor: "pointer" }}>
              <Upload size={24} color={theme.muted} style={{ margin: "0 auto 8px" }}/>
              <div style={{ fontSize: 12, color: theme.muted }}>
                {file ? file.name : "Drop CSV/Excel or click to browse"}
              </div>
            </label>
          </div>
          <button onClick={uploadCSV} disabled={!file || uploading} style={{
            width: "100%", padding: "12px", borderRadius: 10, border: "none",
            background: !file || uploading ? theme.subtle : `linear-gradient(135deg, ${theme.green}, #059669)`,
            color: theme.text, cursor: !file || uploading ? "not-allowed" : "pointer",
            fontWeight: 700, fontSize: 13, fontFamily: "Syne"
          }}>
            {uploading ? "Uploading..." : "Upload & Load to Bronze"}
          </button>
        </div>
      </div>
      {message && (
        <div style={{
          background: theme.card, border: `1px solid ${theme.border}`,
          borderRadius: 12, padding: "14px 20px", marginBottom: 24,
          fontSize: 13, color: theme.green
        }}>{message}</div>
      )}
      <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24 }}>
        <h3 style={{ marginBottom: 20, fontSize: 14, fontWeight: 700, color: theme.muted, letterSpacing: 1, textTransform: "uppercase" }}>Bronze Tables in Snowflake</h3>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
          {bronzeTables.map(t => (
            <div key={t.table_name} style={{
              background: theme.surface, borderRadius: 12,
              border: `1px solid ${theme.border}`, padding: 16
            }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                <Table size={14} color={theme.accent}/>
                <span style={{ fontSize: 11, fontWeight: 700 }} className="mono">{t.table_name}</span>
              </div>
              <div style={{ fontSize: 24, fontWeight: 800, color: theme.text }}>{(t.row_count || 0).toLocaleString()}</div>
              <div style={{ fontSize: 11, color: theme.muted }}>records</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function Quality() {
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [alerts, setAlerts] = useState([]);

  useEffect(() => {
    axios.get(`${API}/alerts`).then(r => setAlerts(r.data.alerts || []));
  }, []);

  const runChecks = () => {
    setLoading(true);
    axios.get(`${API}/quality/check-all`).then(r => {
      setResults(r.data.results || []);
      setLoading(false);
      axios.get(`${API}/alerts`).then(r => setAlerts(r.data.alerts || []));
    }).catch(() => setLoading(false));
  };

  return (
    <div style={{ animation: "slideIn 0.4s ease" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 32 }}>
        <div>
          <h1 style={{ fontSize: 28, fontWeight: 800, marginBottom: 6 }}>Quality Guardian</h1>
          <p style={{ color: theme.muted, fontSize: 13 }}>AI-powered data quality monitoring</p>
        </div>
        <button onClick={runChecks} disabled={loading} style={{
          padding: "12px 24px", borderRadius: 12, border: "none",
          background: `linear-gradient(135deg, ${theme.accent}, ${theme.accent2})`,
          color: theme.text, cursor: "pointer", fontWeight: 700,
          fontSize: 13, fontFamily: "Syne",
          display: "flex", alignItems: "center", gap: 8
        }}>
          <Shield size={16} className={loading ? "spinning" : ""}/>
          {loading ? "Running Checks..." : "Run Quality Checks"}
        </button>
      </div>
      {results.length > 0 && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 16, marginBottom: 24 }}>
          {results.map((r, i) => r.table && (
            <div key={i} style={{
              background: theme.card, borderRadius: 16,
              border: `1px solid ${r.health_score >= 90 ? theme.green + "44" : r.health_score >= 70 ? theme.yellow + "44" : theme.red + "44"}`,
              padding: 24
            }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
                <div>
                  <div style={{ fontSize: 11, color: theme.muted, marginBottom: 4 }} className="mono">{r.table}</div>
                  <div style={{ fontWeight: 700 }}>{r.total_records} records checked</div>
                </div>
                <HealthRing score={r.health_score || 0}/>
              </div>
              <Badge type={r.quality_status}/>
              {r.issues && r.issues.length > 0 && (
                <div style={{ marginTop: 16 }}>
                  {r.issues.slice(0, 4).map((issue, j) => (
                    <div key={j} style={{
                      display: "flex", alignItems: "center", justifyContent: "space-between",
                      padding: "6px 0", borderBottom: `1px solid ${theme.border}22`
                    }}>
                      <div>
                        <span style={{ fontSize: 11, color: theme.muted }}>{issue.issue_type}</span>
                        <span style={{ fontSize: 11, color: theme.muted }}> · {issue.column}</span>
                      </div>
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <span style={{ fontSize: 11, fontWeight: 700 }}>{issue.count}</span>
                        <Badge type={issue.severity}/>
                      </div>
                    </div>
                  ))}
                </div>
              )}
              {r.ai_diagnosis && (
                <div style={{
                  marginTop: 16, padding: 12,
                  background: `${theme.accent}11`,
                  borderRadius: 8, borderLeft: `3px solid ${theme.accent}`
                }}>
                  <div style={{ fontSize: 11, color: theme.muted, marginBottom: 4, fontWeight: 700 }}>AI DIAGNOSIS</div>
                  <div style={{ fontSize: 11, color: theme.text, lineHeight: 1.6 }}>{r.ai_diagnosis}</div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
      <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24 }}>
        <h3 style={{ marginBottom: 20, fontSize: 14, fontWeight: 700, color: theme.muted, letterSpacing: 1, textTransform: "uppercase" }}>Alert Log</h3>
        {alerts.length === 0 ? (
          <div style={{ textAlign: "center", padding: "40px", color: theme.muted, fontSize: 13 }}>
            No alerts yet. Run quality checks to detect issues.
          </div>
        ) : (
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${theme.border}` }}>
                {["Type", "Message", "Severity", "Time"].map(h => (
                  <th key={h} style={{ padding: "8px 12px", textAlign: "left", fontSize: 11, color: theme.muted, fontWeight: 600, letterSpacing: 1, textTransform: "uppercase" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {alerts.slice(0, 10).map((a, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${theme.border}22` }}>
                  <td style={{ padding: "10px 12px", fontSize: 11, fontWeight: 700 }} className="mono">{a.alert_type}</td>
                  <td style={{ padding: "10px 12px", fontSize: 11, color: theme.muted, maxWidth: 400 }}>{a.message}</td>
                  <td style={{ padding: "10px 12px" }}><Badge type={a.severity}/></td>
                  <td style={{ padding: "10px 12px", fontSize: 11, color: theme.muted }}>{a.created_at?.slice(0, 16)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

function Pipeline() {
  const [runs, setRuns] = useState([]);
  const [quarantine, setQuarantine] = useState([]);

  useEffect(() => {
    axios.get(`${API}/pipeline/runs`).then(r => setRuns(r.data.pipeline_runs || []));
    axios.get(`${API}/quarantine`).then(r => setQuarantine(r.data.quarantine_records || []));
  }, []);

  return (
    <div style={{ animation: "slideIn 0.4s ease" }}>
      <div style={{ marginBottom: 32 }}>
        <h1 style={{ fontSize: 28, fontWeight: 800, marginBottom: 6 }}>Pipeline Runs</h1>
        <p style={{ color: theme.muted, fontSize: 13 }}>Complete audit trail of all pipeline executions</p>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 16, marginBottom: 24 }}>
        <MetricCard icon={Play} label="Total Runs" value={runs.length} color={theme.accent}/>
        <MetricCard icon={CheckCircle} label="Successful" value={runs.filter(r => r.status === "SUCCESS").length} color={theme.green}/>
        <MetricCard icon={AlertTriangle} label="Quarantined" value={quarantine.length} color={theme.yellow}/>
      </div>
      <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24, marginBottom: 24 }}>
        <h3 style={{ marginBottom: 20, fontSize: 14, fontWeight: 700, color: theme.muted, letterSpacing: 1, textTransform: "uppercase" }}>Pipeline Timeline</h3>
        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          {runs.slice(0, 10).map((run, i) => (
            <div key={i} style={{
              display: "flex", alignItems: "center", gap: 16,
              padding: "14px 16px", borderRadius: 10,
              background: i % 2 === 0 ? theme.surface : "transparent"
            }}>
              <div style={{
                width: 32, height: 32, borderRadius: "50%",
                background: run.status === "SUCCESS" ? `${theme.green}22` : `${theme.red}22`,
                display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0
              }}>
                {run.status === "SUCCESS"
                  ? <CheckCircle size={16} color={theme.green}/>
                  : <XCircle size={16} color={theme.red}/>}
              </div>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 2 }}>
                  <span style={{ fontSize: 13, fontWeight: 600 }}>{run.source_name}</span>
                  <Badge type={run.status}/>
                </div>
                <div style={{ fontSize: 11, color: theme.muted }}>
                  {run.records_loaded} records · {run.duration_seconds}s · {run.start_time?.slice(0, 16)}
                </div>
              </div>
              <span style={{ fontSize: 11, color: theme.muted, fontFamily: "JetBrains Mono" }}>{run.run_id}</span>
            </div>
          ))}
        </div>
      </div>
      {quarantine.length > 0 && (
        <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.yellow}44`, padding: 24 }}>
          <h3 style={{ marginBottom: 20, fontSize: 14, fontWeight: 700, color: theme.yellow, letterSpacing: 1, textTransform: "uppercase" }}>⚠️ Quarantine Records</h3>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${theme.border}` }}>
                {["ID", "Source", "Reason", "Time"].map(h => (
                  <th key={h} style={{ padding: "8px 12px", textAlign: "left", fontSize: 11, color: theme.muted, fontWeight: 600 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {quarantine.slice(0, 5).map((q, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${theme.border}22` }}>
                  <td style={{ padding: "10px 12px", fontSize: 11 }} className="mono">{q.quarantine_id}</td>
                  <td style={{ padding: "10px 12px", fontSize: 11, color: theme.muted }}>{q.source_name}</td>
                  <td style={{ padding: "10px 12px", fontSize: 11, color: theme.yellow }}>{q.rejection_reason}</td>
                  <td style={{ padding: "10px 12px", fontSize: 11, color: theme.muted }}>{q.quarantined_at?.slice(0, 16)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function Transform() {
  const [status, setStatus] = useState(null);
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState(null);

  useEffect(() => {
    axios.get(`${API}/transform/status`)
      .then(r => setStatus(r.data));
  }, []);

  const runTransform = () => {
    setRunning(true);
    setResult(null);
    axios.post(`${API}/transform/run`)
      .then(r => {
        setResult(r.data);
        setRunning(false);
        axios.get(`${API}/transform/status`)
          .then(r => setStatus(r.data));
      })
      .catch(() => setRunning(false));
  };

  const layerColor = { SILVER: theme.blue, GOLD: theme.yellow };

  return (
    <div style={{ animation: "slideIn 0.4s ease" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 32 }}>
        <div>
          <h1 style={{ fontSize: 28, fontWeight: 800, marginBottom: 6 }}>Transform Pipeline</h1>
          <p style={{ color: theme.muted, fontSize: 13 }}>Bronze → Silver → Gold — AI builds all models automatically</p>
        </div>
        <button onClick={runTransform} disabled={running} style={{
          padding: "12px 24px", borderRadius: 12, border: "none",
          background: running ? theme.subtle : `linear-gradient(135deg, ${theme.yellow}, #D97706)`,
          color: running ? theme.muted : "#000",
          cursor: running ? "not-allowed" : "pointer",
          fontWeight: 700, fontSize: 13, fontFamily: "Syne",
          display: "flex", alignItems: "center", gap: 8
        }}>
          <Zap size={16} className={running ? "spinning" : ""}/>
          {running ? "Running..." : "Run Transformations"}
        </button>
      </div>

      {/* Pipeline flow */}
      <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24, marginBottom: 24 }}>
        <h3 style={{ marginBottom: 20, fontSize: 12, fontWeight: 700, color: theme.muted, letterSpacing: 1, textTransform: "uppercase" }}>Pipeline Architecture</h3>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 8, flexWrap: "wrap" }}>
          {[
            { label: "MySQL", sub: "Source", color: theme.blue },
            { label: "→", sub: "", color: theme.muted },
            { label: "BRONZE", sub: "Raw data", color: "#CD7F32" },
            { label: "→", sub: "", color: theme.muted },
            { label: "QUALITY", sub: "AI checks", color: theme.red },
            { label: "→", sub: "", color: theme.muted },
            { label: "SILVER", sub: "Cleaned", color: theme.blue },
            { label: "→", sub: "", color: theme.muted },
            { label: "GOLD", sub: "Business", color: theme.yellow },
            { label: "→", sub: "", color: theme.muted },
            { label: "CEO", sub: "Dashboard", color: theme.green },
          ].map((item, i) => (
            item.label === "→" ? (
              <span key={i} style={{ fontSize: 20, color: theme.muted }}>→</span>
            ) : (
              <div key={i} style={{
                background: `${item.color}22`,
                border: `1px solid ${item.color}44`,
                borderRadius: 10, padding: "10px 16px",
                textAlign: "center", minWidth: 80
              }}>
                <div style={{ fontSize: 12, fontWeight: 800, color: item.color }}>{item.label}</div>
                <div style={{ fontSize: 10, color: theme.muted, marginTop: 2 }}>{item.sub}</div>
              </div>
            )
          ))}
        </div>
      </div>

      {/* Table status */}
      {status?.tables && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 16, marginBottom: 24 }}>
          {["SILVER", "GOLD"].map(layer => (
            <div key={layer} style={{
              background: theme.card, borderRadius: 16,
              border: `1px solid ${layerColor[layer]}44`, padding: 24
            }}>
              <h3 style={{ marginBottom: 16, fontSize: 14, fontWeight: 700, color: layerColor[layer] }}>
                {layer === "SILVER" ? "🥈" : "🥇"} {layer} LAYER
              </h3>
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {status.tables.filter(t => t.layer === layer).map((t, i) => (
                  <div key={i} style={{
                    display: "flex", alignItems: "center", justifyContent: "space-between",
                    padding: "10px 14px", background: theme.surface, borderRadius: 10
                  }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                      {t.exists
                        ? <CheckCircle size={14} color={theme.green}/>
                        : <XCircle size={14} color={theme.red}/>}
                      <span style={{ fontSize: 11, fontFamily: "JetBrains Mono" }}>{t.table}</span>
                    </div>
                    <span style={{ fontSize: 12, fontWeight: 700, color: t.exists ? theme.green : theme.red }}>
                      {t.exists ? `${t.record_count} rows` : "NOT BUILT"}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* AI Summary */}
      {result && (
        <div style={{
          background: theme.card, borderRadius: 16,
          border: `1px solid ${theme.accent}44`,
          padding: 24, animation: "slideIn 0.4s ease"
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 16 }}>
            <Zap size={16} color={theme.accent}/>
            <span style={{ fontSize: 12, fontWeight: 700, color: theme.accent, letterSpacing: 1 }}>AI PIPELINE SUMMARY</span>
            <span style={{ marginLeft: "auto", fontSize: 11, color: theme.green }}>
              {result.models_succeeded} succeeded · {result.models_failed} failed
            </span>
          </div>
          <p style={{ fontSize: 13, lineHeight: 1.7, color: theme.text, marginBottom: 16 }}>
            {result.ai_summary}
          </p>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
            {[...(result.silver_tables || []), ...(result.gold_tables || [])].map((t, i) => (
              <div key={i} style={{
                background: theme.surface, borderRadius: 10,
                padding: "10px 14px",
                borderLeft: `3px solid ${t.layer === "SILVER" ? theme.blue : theme.yellow}`
              }}>
                <div style={{ fontSize: 10, color: theme.muted, marginBottom: 4, fontFamily: "JetBrains Mono" }}>{t.table}</div>
                <div style={{ fontSize: 18, fontWeight: 800, color: theme.text }}>{t.records}</div>
                <div style={{ fontSize: 10, color: theme.muted }}>records</div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function Query() {
  const [question, setQuestion] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [history, setHistory] = useState([]);

  useEffect(() => {
    axios.get(`${API}/query/history`).then(r => setHistory(r.data.queries || []));
  }, []);

  const suggestions = [
    "Which store has highest revenue?",
    "Show me products with low stock",
    "What is daily revenue trend?",
    "Show me store rankings by revenue",
    "Which stores need inventory reorder?",
    "What is total revenue across all stores?",
    "Show me top 3 products by units sold",
    "Which products have negative margin?",
  ];

  const ask = () => {
    if (!question.trim()) return;
    setLoading(true);
    setResult(null);
    axios.post(`${API}/query/ask`, { question })
      .then(r => {
        setResult(r.data);
        setLoading(false);
        axios.get(`${API}/query/history`).then(r => setHistory(r.data.queries || []));
      })
      .catch(e => {
        setResult({ status: "error", message: e.message });
        setLoading(false);
      });
  };

  const buildChartData = (result) => {
    if (!result?.results?.length) return [];
    return result.results.slice(0, 10).map((row) => {
      const values = Object.values(row);
      return {
        name: String(values[0]).slice(0, 15),
        value: parseFloat(values[1]) || 0,
      };
    });
  };

  return (
    <div style={{ animation: "slideIn 0.4s ease" }}>
      <div style={{ marginBottom: 32 }}>
        <h1 style={{ fontSize: 28, fontWeight: 800, marginBottom: 6 }}>AI Query Engine</h1>
        <p style={{ color: theme.muted, fontSize: 13 }}>Ask any business question — AI writes SQL, runs it, explains results</p>
      </div>
      <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24, marginBottom: 16 }}>
        <div style={{ display: "flex", gap: 12, marginBottom: 16 }}>
          <input value={question} onChange={e => setQuestion(e.target.value)}
            onKeyDown={e => e.key === "Enter" && ask()}
            placeholder="Ask anything... e.g. Which store had highest revenue?"
            style={{
              flex: 1, padding: "14px 18px",
              background: theme.surface, border: `1px solid ${theme.border}`,
              borderRadius: 12, color: theme.text,
              fontSize: 14, fontFamily: "Syne", outline: "none"
            }}
            onFocus={e => e.target.style.borderColor = theme.accent}
            onBlur={e => e.target.style.borderColor = theme.border}
          />
          <button onClick={ask} disabled={loading || !question.trim()} style={{
            padding: "14px 28px", borderRadius: 12, border: "none",
            background: loading ? theme.subtle : `linear-gradient(135deg, ${theme.accent}, ${theme.accent2})`,
            color: theme.text, cursor: loading ? "not-allowed" : "pointer",
            fontWeight: 700, fontSize: 13, fontFamily: "Syne",
            display: "flex", alignItems: "center", gap: 8, minWidth: 120
          }}>
            <Zap size={16} className={loading ? "spinning" : ""}/>
            {loading ? "Thinking..." : "Ask AI"}
          </button>
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          {suggestions.map((s, i) => (
            <button key={i} onClick={() => setQuestion(s)} style={{
              padding: "6px 14px", borderRadius: 20,
              background: `${theme.accent}11`, border: `1px solid ${theme.accent}33`,
              color: theme.muted, cursor: "pointer", fontSize: 11, fontFamily: "Syne"
            }}
            onMouseEnter={e => { e.target.style.background = `${theme.accent}22`; e.target.style.color = theme.text; }}
            onMouseLeave={e => { e.target.style.background = `${theme.accent}11`; e.target.style.color = theme.muted; }}>
              {s}
            </button>
          ))}
        </div>
      </div>

      {loading && (
        <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.accent}44`, padding: 32, textAlign: "center", marginBottom: 16 }}>
          <Zap size={32} color={theme.accent} className="spinning" style={{ margin: "0 auto 12px" }}/>
          <div style={{ fontWeight: 700, marginBottom: 4 }}>DataForge AI is thinking...</div>
          <div style={{ fontSize: 12, color: theme.muted }}>Reading question → Writing SQL → Running on Snowflake → Explaining results</div>
        </div>
      )}

      {result && !loading && (
        <div style={{ animation: "slideIn 0.4s ease" }}>
          {result.status === "error" ? (
            <div style={{ background: `${theme.red}11`, border: `1px solid ${theme.red}44`, borderRadius: 16, padding: 24, marginBottom: 16 }}>
              <div style={{ color: theme.red, fontWeight: 700, marginBottom: 8 }}>❌ Query Error</div>
              <div style={{ fontSize: 13, color: theme.muted }}>{result.message}</div>
            </div>
          ) : (
            <>
              <div style={{
                background: `linear-gradient(135deg, ${theme.accent}15, ${theme.accent2}15)`,
                border: `1px solid ${theme.accent}44`,
                borderRadius: 16, padding: 24, marginBottom: 16
              }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
                  <div style={{ background: `${theme.accent}22`, borderRadius: 8, padding: 6 }}>
                    <Zap size={16} color={theme.accent}/>
                  </div>
                  <span style={{ fontSize: 11, fontWeight: 700, color: theme.accent, letterSpacing: 1 }}>AI ANSWER</span>
                  <span style={{ fontSize: 10, color: theme.muted, marginLeft: "auto" }}>
                    {result.total_rows} rows · {result.execution_time_ms}ms
                  </span>
                </div>
                <div style={{ fontSize: 15, lineHeight: 1.7, color: theme.text, fontWeight: 500 }}>{result.explanation}</div>
              </div>

              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 }}>
                <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24 }}>
                  <h3 style={{ marginBottom: 16, fontSize: 12, fontWeight: 700, color: theme.muted, letterSpacing: 1, textTransform: "uppercase" }}>Visualisation</h3>
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={buildChartData(result)}>
                      <CartesianGrid strokeDasharray="3 3" stroke={theme.border}/>
                      <XAxis dataKey="name" stroke={theme.muted} fontSize={10} angle={-20}/>
                      <YAxis stroke={theme.muted} fontSize={10}/>
                      <Tooltip contentStyle={{ background: theme.card, border: `1px solid ${theme.border}`, borderRadius: 8, fontSize: 12 }}/>
                      <Bar dataKey="value" radius={[4,4,0,0]} fill={theme.accent}/>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24 }}>
                  <h3 style={{ marginBottom: 12, fontSize: 12, fontWeight: 700, color: theme.muted, letterSpacing: 1, textTransform: "uppercase" }}>Generated SQL</h3>
                  <pre style={{ fontSize: 11, color: theme.accent, fontFamily: "JetBrains Mono", whiteSpace: "pre-wrap", lineHeight: 1.6, overflow: "auto", maxHeight: 200 }}>
                    {result.generated_sql}
                  </pre>
                </div>
              </div>

              {result.results?.length > 0 && (
                <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24, marginBottom: 16 }}>
                  <h3 style={{ marginBottom: 16, fontSize: 12, fontWeight: 700, color: theme.muted, letterSpacing: 1, textTransform: "uppercase" }}>Results — {result.total_rows} rows</h3>
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse" }}>
                      <thead>
                        <tr style={{ borderBottom: `1px solid ${theme.border}` }}>
                          {result.columns?.map(col => (
                            <th key={col} style={{ padding: "8px 12px", textAlign: "left", fontSize: 11, color: theme.muted, fontWeight: 600, letterSpacing: 1, textTransform: "uppercase", whiteSpace: "nowrap" }}>{col}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {result.results.slice(0, 20).map((row, i) => (
                          <tr key={i} style={{ borderBottom: `1px solid ${theme.border}22` }}>
                            {Object.values(row).map((val, j) => (
                              <td key={j} style={{
                                padding: "10px 12px", fontSize: 12,
                                color: String(val).startsWith("-") ? theme.red : theme.text,
                                fontFamily: !isNaN(val) ? "JetBrains Mono" : "Syne"
                              }}>{val}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      )}

      {history.length > 0 && (
        <div style={{ background: theme.card, borderRadius: 16, border: `1px solid ${theme.border}`, padding: 24 }}>
          <h3 style={{ marginBottom: 16, fontSize: 12, fontWeight: 700, color: theme.muted, letterSpacing: 1, textTransform: "uppercase" }}>Query History</h3>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {history.slice(0, 5).map((h, i) => (
              <div key={i} onClick={() => setQuestion(h.question)} style={{
                display: "flex", alignItems: "center", justifyContent: "space-between",
                padding: "10px 14px", background: theme.surface, borderRadius: 10, cursor: "pointer"
              }}
              onMouseEnter={e => e.currentTarget.style.background = theme.subtle}
              onMouseLeave={e => e.currentTarget.style.background = theme.surface}>
                <div>
                  <div style={{ fontSize: 13 }}>{h.question}</div>
                  <div style={{ fontSize: 11, color: theme.muted, marginTop: 2 }}>
                    {h.rows_returned} rows · {h.execution_time_ms}ms · {h.asked_at?.slice(0, 16)}
                  </div>
                </div>
                <ChevronRight size={14} color={theme.muted}/>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default function App() {
  const [active, setActive] = useState("dashboard");

  const pages = {
    dashboard: <Dashboard/>,
    sources: <Sources/>,
    quality: <Quality/>,
    pipeline: <Pipeline/>,
    transform: <Transform/>,
    query: <Query/>,
  };

  return (
    <>
      <style>{styles}</style>
      <div className="grid-bg" style={{ minHeight: "100vh" }}>
        <Sidebar active={active} setActive={setActive}/>
        <main style={{ marginLeft: 220, padding: "32px 40px", minHeight: "100vh" }}>
          {pages[active]}
        </main>
      </div>
    </>
  );
}