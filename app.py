"""
Polymarket weather arbitrage dashboard.

Local:   python app.py  →  http://localhost:8888
Railway: deploys automatically, auto-scans every SCAN_INTERVAL_HOURS hours
"""

import json
import os
import sys
import subprocess
import threading
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template_string, request, send_file
import io
import csv

app = Flask(__name__)

# DATA_DIR: use env var on Railway (mount a volume there), else local ./data
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
LATEST   = os.path.join(DATA_DIR, "latest_scan.json")
os.makedirs(DATA_DIR, exist_ok=True)

# Auto-scan config (set SCAN_INTERVAL_HOURS=0 to disable)
SCAN_INTERVAL_HOURS = int(os.environ.get("SCAN_INTERVAL_HOURS", "1"))
SCAN_CAPITAL        = int(os.environ.get("SCAN_CAPITAL", "400"))

_scan_running = False
_scan_log     = []


def load_scan():
    if not os.path.exists(LATEST):
        return None
    with open(LATEST) as f:
        return json.load(f)


def list_scans():
    """Return list of past scan files (newest first)."""
    if not os.path.exists(DATA_DIR):
        return []
    files = [f for f in os.listdir(DATA_DIR) if f.startswith("scan_") and f.endswith(".json")]
    files.sort(reverse=True)
    return files[:20]


def run_scan_bg(cities=None, capital=None, days=1):
    global _scan_running, _scan_log
    if _scan_running:
        return
    _scan_running = True
    _scan_log     = []
    capital = capital or SCAN_CAPITAL
    # Use sys.executable so Railway uses the correct venv Python
    cmd = [sys.executable, "scan.py", "--days", str(days), "--capital", str(capital)]
    if cities:
        cmd += ["--cities"] + cities
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for line in proc.stdout:
            _scan_log.append(line.rstrip())
        proc.wait()
    except Exception as e:
        _scan_log.append(f"ERROR: {e}")
    finally:
        _scan_running = False


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Polymarket Weather Scanner</title>
<style>
/* ─── Reset & Base ─────────────────────────────────────────────────── */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
:root {
  --bg:       #0b0d14;
  --surface:  #131520;
  --surface2: #1a1d2e;
  --border:   #252840;
  --border2:  #1e2235;
  --text:     #e2e8f0;
  --text-dim: #94a3b8;
  --text-muted: #4b5563;
  --purple:   #7c3aed;
  --purple-light: #a78bfa;
  --green:    #34d399;
  --green-dim: #064e3b;
  --red:      #f87171;
  --red-dim:  #450a0a;
  --yellow:   #fbbf24;
  --yellow-dim: #451a03;
  --blue:     #38bdf8;
  --blue-dim: #0c4a6e;
  --radius:   8px;
}
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Inter', sans-serif;
  background: var(--bg);
  color: var(--text);
  min-height: 100vh;
  font-size: 13px;
}

/* ─── Header ────────────────────────────────────────────────────────── */
.header {
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  padding: 14px 24px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  position: sticky;
  top: 0;
  z-index: 200;
  backdrop-filter: blur(10px);
}
.header h1 {
  font-size: 1rem;
  font-weight: 700;
  color: var(--purple-light);
  letter-spacing: .3px;
  display: flex;
  align-items: center;
  gap: 8px;
}
.header h1 .dot {
  width: 7px; height: 7px;
  border-radius: 50%;
  background: var(--green);
  box-shadow: 0 0 6px var(--green);
  animation: pulse 2s ease-in-out infinite;
}
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }
.header-right { display: flex; align-items: center; gap: 16px; }
.header-meta { font-size: .72rem; color: var(--text-muted); }
.header-meta span { color: var(--text-dim); }

/* ─── Layout ─────────────────────────────────────────────────────────── */
.layout { display: flex; min-height: calc(100vh - 49px); }
.sidebar {
  width: 220px;
  min-width: 220px;
  background: var(--surface);
  border-right: 1px solid var(--border);
  padding: 16px 12px;
  display: flex;
  flex-direction: column;
  gap: 4px;
  position: sticky;
  top: 49px;
  height: calc(100vh - 49px);
  overflow-y: auto;
}
.sidebar-label {
  font-size: .62rem;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text-muted);
  padding: 10px 8px 4px;
  margin-top: 6px;
}
.nav-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  border-radius: var(--radius);
  border: none;
  background: transparent;
  color: var(--text-dim);
  font-size: .8rem;
  cursor: pointer;
  text-align: left;
  width: 100%;
  transition: background .15s, color .15s;
}
.nav-btn:hover { background: var(--surface2); color: var(--text); }
.nav-btn.active { background: var(--surface2); color: var(--purple-light); }
.nav-btn .badge {
  margin-left: auto;
  font-size: .65rem;
  font-weight: 700;
  padding: 1px 6px;
  border-radius: 10px;
}
.badge-green { background: var(--green-dim); color: var(--green); }
.badge-red   { background: var(--red-dim); color: var(--red); }
.badge-blue  { background: var(--blue-dim); color: var(--blue); }
.main { flex: 1; min-width: 0; padding: 20px 24px; overflow-x: hidden; }

/* ─── Scan bar ─────────────────────────────────────────────────────── */
.scanbar {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 12px 16px;
  margin-bottom: 20px;
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}
.form-group { display: flex; align-items: center; gap: 6px; }
.form-label { font-size: .72rem; color: var(--text-muted); }
input[type=number], select {
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 6px;
  color: var(--text);
  padding: 5px 10px;
  font-size: .8rem;
  outline: none;
  transition: border-color .15s;
}
input[type=number]:focus, select:focus { border-color: var(--purple); }
input[type=number] { width: 80px; }
.btn {
  padding: 6px 14px;
  border-radius: 6px;
  border: none;
  font-size: .8rem;
  font-weight: 600;
  cursor: pointer;
  transition: opacity .15s, transform .1s;
  white-space: nowrap;
}
.btn:hover { opacity: .88; }
.btn:active { transform: scale(.97); }
.btn-primary { background: var(--purple); color: #fff; }
.btn-secondary { background: var(--surface2); color: var(--text-dim); border: 1px solid var(--border); }
.btn-danger { background: var(--red-dim); color: var(--red); border: 1px solid #7f1d1d; }
.scanner-status {
  font-size: .72rem;
  color: var(--text-muted);
  display: flex;
  align-items: center;
  gap: 6px;
}
.spinner {
  display: none;
  width: 14px; height: 14px;
  border: 2px solid var(--purple);
  border-top-color: transparent;
  border-radius: 50%;
  animation: spin .7s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
.scanning .spinner { display: inline-block; }
.scanning .btn-primary { opacity: .4; pointer-events: none; }
.divider { flex: 1; }

/* ─── Log ──────────────────────────────────────────────────────────── */
#log-wrap {
  display: none;
  background: #060810;
  border: 1px solid var(--border2);
  border-radius: var(--radius);
  padding: 10px 12px;
  margin-bottom: 16px;
  font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
  font-size: .7rem;
  color: #64748b;
  max-height: 140px;
  overflow-y: auto;
  line-height: 1.5;
}

/* ─── Summary cards ─────────────────────────────────────────────────── */
.cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 10px;
  margin-bottom: 20px;
}
.card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 14px 16px;
  cursor: default;
}
.card .label {
  font-size: .63rem;
  text-transform: uppercase;
  letter-spacing: .8px;
  color: var(--text-muted);
  margin-bottom: 6px;
}
.card .value { font-size: 1.5rem; font-weight: 700; }
.card.yes .value  { color: var(--green); }
.card.no .value   { color: var(--red); }
.card.dep .value  { color: var(--yellow); }
.card.scan .value { font-size: .8rem; color: var(--text-dim); }
.card .sub { font-size: .65rem; color: var(--text-muted); margin-top: 3px; }

/* ─── Filters ───────────────────────────────────────────────────────── */
.filters {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 10px 14px;
  margin-bottom: 16px;
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}
.filter-label { font-size: .7rem; color: var(--text-muted); }
.filter-badge {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 3px 10px;
  border-radius: 20px;
  border: 1px solid var(--border);
  font-size: .72rem;
  color: var(--text-dim);
  cursor: pointer;
  transition: border-color .15s, color .15s, background .15s;
  user-select: none;
}
.filter-badge:hover { border-color: var(--purple); color: var(--purple-light); }
.filter-badge.active { border-color: var(--purple); background: rgba(124,58,237,.12); color: var(--purple-light); }

/* ─── Section ───────────────────────────────────────────────────────── */
.section { margin-bottom: 28px; }
.section-header {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 10px;
  padding-bottom: 8px;
  border-bottom: 1px solid var(--border2);
}
.section-title {
  font-size: .78rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: .8px;
}
.section-title.yes-title { color: var(--green); }
.section-title.no-title  { color: var(--red); }
.section-title.fc-title  { color: var(--blue); }
.section-count {
  font-size: .68rem;
  font-weight: 700;
  padding: 2px 7px;
  border-radius: 10px;
}
.section-right { margin-left: auto; display: flex; gap: 8px; align-items: center; }

/* ─── Table ─────────────────────────────────────────────────────────── */
.tbl-wrap { overflow-x: auto; border-radius: var(--radius); border: 1px solid var(--border2); }
table {
  width: 100%;
  border-collapse: collapse;
  font-size: .78rem;
}
th {
  text-align: left;
  padding: 9px 10px;
  color: var(--text-muted);
  font-size: .65rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: .6px;
  border-bottom: 1px solid var(--border);
  white-space: nowrap;
  background: var(--surface);
  cursor: pointer;
  user-select: none;
  position: relative;
}
th:hover { color: var(--text-dim); }
th .sort-arrow { display: inline-block; margin-left: 4px; opacity: .4; }
th.sorted-asc  .sort-arrow::after { content: ' ↑'; opacity: 1; color: var(--purple-light); }
th.sorted-desc .sort-arrow::after { content: ' ↓'; opacity: 1; color: var(--purple-light); }
td {
  padding: 9px 10px;
  border-bottom: 1px solid var(--border2);
  vertical-align: middle;
}
tr:last-child td { border-bottom: none; }
tr:hover td { background: rgba(26,29,46,.6); }
tr.pinned td { background: rgba(124,58,237,.06); border-left: 2px solid var(--purple); }

/* ─── Cell styles ───────────────────────────────────────────────────── */
.city-cell { font-weight: 600; color: var(--text); }
.date-cell { color: var(--text-muted); font-size: .72rem; font-variant-numeric: tabular-nums; }
.brackets-cell { color: #c4b5fd; font-size: .72rem; max-width: 260px; word-break: break-word; }
.price-cell { font-variant-numeric: tabular-nums; }
.ret-cell   { font-weight: 700; font-variant-numeric: tabular-nums; }
.ret-high   { color: var(--green); }
.ret-med    { color: var(--yellow); }
.ret-low    { color: var(--text-dim); }
.winrange-cell { color: #93c5fd; font-size: .72rem; font-variant-numeric: tabular-nums; }
.forecast-cell { color: var(--text-dim); font-variant-numeric: tabular-nums; }
.size-cell  { color: var(--yellow); font-weight: 600; font-variant-numeric: tabular-nums; }
.liq-ok   { color: var(--text-dim); font-variant-numeric: tabular-nums; }
.liq-warn { color: var(--yellow); font-variant-numeric: tabular-nums; }
.liq-low  { color: var(--red); font-variant-numeric: tabular-nums; }
.link-cell a {
  color: var(--purple-light);
  text-decoration: none;
  font-size: .7rem;
  opacity: .8;
}
.link-cell a:hover { opacity: 1; text-decoration: underline; }
.copy-btn {
  border: none;
  background: none;
  color: var(--text-muted);
  cursor: pointer;
  font-size: .65rem;
  padding: 2px 4px;
  border-radius: 3px;
  transition: color .15s;
}
.copy-btn:hover { color: var(--text); }

/* ─── Confidence badges ─────────────────────────────────────────────── */
.conf {
  display: inline-flex;
  align-items: center;
  gap: 3px;
  padding: 2px 7px;
  border-radius: 4px;
  font-size: .64rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: .4px;
  white-space: nowrap;
}
.conf-high { background: rgba(52,211,153,.12); color: var(--green); }
.conf-med  { background: rgba(251,191,36,.12); color: var(--yellow); }
.conf-low  { background: rgba(100,116,139,.1); color: var(--text-muted); }

/* ─── City badge ────────────────────────────────────────────────────── */
.city-badge {
  display: inline-block;
  font-size: .6rem;
  font-weight: 700;
  padding: 1px 5px;
  border-radius: 3px;
  margin-left: 4px;
  vertical-align: middle;
  text-transform: uppercase;
}
.badge-us   { background: rgba(56,189,248,.12); color: #7dd3fc; }
.badge-intl { background: rgba(167,139,250,.1); color: var(--purple-light); }

/* ─── Empty state ───────────────────────────────────────────────────── */
.empty {
  text-align: center;
  padding: 40px 0;
  color: var(--text-muted);
  font-size: .82rem;
}
.empty .empty-icon { font-size: 1.8rem; margin-bottom: 8px; }

/* ─── Tab panels ────────────────────────────────────────────────────── */
.tab-panel { display: none; }
.tab-panel.active { display: block; }

/* ─── History panel ─────────────────────────────────────────────────── */
.history-list { display: flex; flex-direction: column; gap: 6px; }
.history-item {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 10px 14px;
  display: flex;
  align-items: center;
  gap: 10px;
  cursor: pointer;
  transition: border-color .15s;
}
.history-item:hover { border-color: var(--purple); }
.history-item.active { border-color: var(--purple); background: rgba(124,58,237,.06); }
.history-time { font-size: .72rem; color: var(--text-dim); font-variant-numeric: tabular-nums; }
.history-stats { font-size: .68rem; color: var(--text-muted); }
.history-stats span { color: var(--text-dim); }

/* ─── Bracket details expand ─────────────────────────────────────────── */
.bracket-detail {
  display: none;
  background: rgba(12,14,26,.6);
  border-top: 1px solid var(--border2);
  padding: 8px 12px;
  font-size: .72rem;
}
.bracket-detail.open { display: table-row; }
.bracket-row {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 4px 0;
  border-bottom: 1px solid rgba(37,40,64,.4);
}
.bracket-row:last-child { border-bottom: none; }
.forecast-dot {
  width: 6px; height: 6px;
  border-radius: 50%;
  background: var(--yellow);
  flex-shrink: 0;
}

/* ─── Scrollbar ─────────────────────────────────────────────────────── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

/* ─── Responsive ────────────────────────────────────────────────────── */
@media (max-width: 768px) {
  .sidebar { display: none; }
  .main { padding: 12px; }
}

/* ─── Keyboard hint ─────────────────────────────────────────────────── */
.kbd {
  display: inline-block;
  background: var(--surface2);
  border: 1px solid var(--border);
  border-radius: 3px;
  padding: 1px 5px;
  font-family: monospace;
  font-size: .65rem;
  color: var(--text-dim);
}

/* ─── Toast ─────────────────────────────────────────────────────────── */
#toast {
  position: fixed;
  bottom: 20px;
  right: 20px;
  background: var(--surface2);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 10px 16px;
  font-size: .78rem;
  color: var(--text);
  opacity: 0;
  transform: translateY(8px);
  transition: opacity .25s, transform .25s;
  z-index: 999;
  pointer-events: none;
  max-width: 260px;
}
#toast.show { opacity: 1; transform: translateY(0); }
</style>
</head>
<body>

<!-- ─── Header ─────────────────────────────────────────────────────── -->
<div class="header">
  <h1>
    <span class="dot" id="live-dot"></span>
    Polymarket Weather Scanner
  </h1>
  <div class="header-right">
    <div class="header-meta" id="header-meta">No scan data</div>
  </div>
</div>

<div class="layout">

<!-- ─── Sidebar ────────────────────────────────────────────────────── -->
<nav class="sidebar">
  <span class="sidebar-label">Views</span>
  <button class="nav-btn active" onclick="showTab('main')" id="tab-main">
    📊 Dashboard
  </button>
  <button class="nav-btn" onclick="showTab('history')" id="tab-history">
    🕐 History
    <span class="badge badge-blue" id="history-badge">0</span>
  </button>

  <span class="sidebar-label">Filters</span>
  <div style="padding: 6px 8px; display:flex; flex-direction:column; gap:8px;">
    <div class="form-group" style="flex-direction:column; align-items:flex-start; gap:4px;">
      <label class="form-label">Min Return %</label>
      <div style="display:flex; align-items:center; gap:6px; width:100%;">
        <input type="range" id="min-return-filter" min="0" max="50" step="1" value="0"
          style="flex:1; accent-color:var(--purple);"
          oninput="document.getElementById('min-return-val').textContent=this.value+'%'; applyFilters()">
        <span id="min-return-val" style="font-size:.72rem; color:var(--text-dim); width:32px;">0%</span>
      </div>
    </div>
    <div class="form-group" style="flex-direction:column; align-items:flex-start; gap:4px;">
      <label class="form-label">Min Liquidity $</label>
      <div style="display:flex; align-items:center; gap:6px; width:100%;">
        <input type="range" id="min-liq-filter" min="0" max="500" step="25" value="0"
          style="flex:1; accent-color:var(--purple);"
          oninput="document.getElementById('min-liq-val').textContent='$'+this.value; applyFilters()">
        <span id="min-liq-val" style="font-size:.72rem; color:var(--text-dim); width:36px;">$0</span>
      </div>
    </div>
    <div class="form-group" style="flex-direction:column; align-items:flex-start; gap:4px;">
      <label class="form-label">Confidence</label>
      <select id="conf-filter" onchange="applyFilters()"
        style="width:100%; background:var(--bg); border-color:var(--border); color:var(--text-dim);">
        <option value="all">All</option>
        <option value="high">High only</option>
        <option value="medium">Medium+</option>
      </select>
    </div>
  </div>

  <span class="sidebar-label">Actions</span>
  <button class="nav-btn" onclick="exportCSV('yes')">⬇ Export YES CSV</button>
  <button class="nav-btn" onclick="exportCSV('no')">⬇ Export NO CSV</button>
  <button class="nav-btn" onclick="loadData()">↻ Refresh data</button>

  <div style="flex:1"></div>
  <div style="padding: 8px; font-size:.62rem; color:var(--text-muted);">
    <kbd class="kbd">R</kbd> run scan &nbsp;
    <kbd class="kbd">F</kbd> focus filter
  </div>
</nav>

<!-- ─── Main ────────────────────────────────────────────────────────── -->
<div class="main">

  <!-- Scan bar -->
  <div class="scanbar" id="scanbar">
    <button class="btn btn-primary" onclick="runScan()" id="scan-btn">▶ Run Scan</button>
    <div class="spinner" id="spinner"></div>
    <div class="scanner-status" id="scan-status">Ready</div>
    <div class="form-group">
      <span class="form-label">Capital $</span>
      <input type="number" id="capital" value="400" min="50" step="50">
    </div>
    <div class="form-group">
      <span class="form-label">Days</span>
      <input type="number" id="days" value="1" min="1" max="7" style="width:56px">
    </div>
    <div class="divider"></div>
    <button class="btn btn-secondary" onclick="toggleLog()" id="log-toggle-btn">Log</button>
  </div>
  <div id="log-wrap"></div>

  <!-- ── Main tab ───────────────────────────────────────────────────── -->
  <div class="tab-panel active" id="panel-main">

    <!-- Summary cards -->
    <div class="cards" id="cards">
      <div class="card yes"><div class="label">YES Clusters</div><div class="value">—</div></div>
      <div class="card no"><div class="label">NO Bets</div><div class="value">—</div></div>
      <div class="card dep"><div class="label">Est. Deploy</div><div class="value">—</div></div>
      <div class="card scan"><div class="label">Last Scan</div><div class="value">—</div></div>
    </div>

    <!-- YES Clusters -->
    <div class="section">
      <div class="section-header">
        <span class="section-title yes-title">YES Clusters</span>
        <span class="section-count badge-green badge" id="yes-count">0</span>
        <span style="font-size:.68rem; color:var(--text-muted);">Buy 2-3 adjacent brackets · guaranteed 1 winner</span>
        <div class="section-right">
          <button class="btn btn-secondary" onclick="sortTable('yes', 'return_pct', -1)" style="font-size:.7rem; padding:4px 10px;">↑ Return</button>
          <button class="btn btn-secondary" onclick="sortTable('yes', 'date', 1)" style="font-size:.7rem; padding:4px 10px;">📅 Date</button>
        </div>
      </div>
      <div class="tbl-wrap">
        <table id="yes-table">
          <thead>
            <tr>
              <th onclick="sortTable('yes','city',1)">City <span class="sort-arrow"></span></th>
              <th onclick="sortTable('yes','date',1)">Date <span class="sort-arrow"></span></th>
              <th>Brackets</th>
              <th onclick="sortTable('yes','cluster_size',1)" style="text-align:center">n <span class="sort-arrow"></span></th>
              <th onclick="sortTable('yes','total_price',1)">Cost <span class="sort-arrow"></span></th>
              <th onclick="sortTable('yes','return_pct',-1)">Return <span class="sort-arrow"></span></th>
              <th>Win Range</th>
              <th onclick="sortTable('yes','forecast_temp',1)">Forecast <span class="sort-arrow"></span></th>
              <th onclick="sortTable('yes','forecast_confidence',1)">Conf <span class="sort-arrow"></span></th>
              <th onclick="sortTable('yes','liquidity_min',-1)">Liq <span class="sort-arrow"></span></th>
              <th onclick="sortTable('yes','size_each',1)">$/bkt <span class="sort-arrow"></span></th>
              <th onclick="sortTable('yes','total_cost',1)">Total $ <span class="sort-arrow"></span></th>
              <th>Link</th>
            </tr>
          </thead>
          <tbody id="yes-body">
            <tr><td colspan="13"><div class="empty"><div class="empty-icon">📡</div>Run a scan to see opportunities</div></td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- NO Bets -->
    <div class="section">
      <div class="section-header">
        <span class="section-title no-title">NO Bets</span>
        <span class="section-count badge-red badge" id="no-count">0</span>
        <span style="font-size:.68rem; color:var(--text-muted);">Bracket clearly far from forecast · low risk</span>
        <div class="section-right">
          <button class="btn btn-secondary" onclick="sortTable('no', 'return_pct', -1)" style="font-size:.7rem; padding:4px 10px;">↑ Return</button>
          <button class="btn btn-secondary" onclick="sortTable('no', 'date', 1)" style="font-size:.7rem; padding:4px 10px;">📅 Date</button>
        </div>
      </div>
      <div class="tbl-wrap">
        <table id="no-table">
          <thead>
            <tr>
              <th onclick="sortTable('no','city',1)">City <span class="sort-arrow"></span></th>
              <th onclick="sortTable('no','date',1)">Date <span class="sort-arrow"></span></th>
              <th>Bracket</th>
              <th onclick="sortTable('no','no_price',1)">NO Price <span class="sort-arrow"></span></th>
              <th onclick="sortTable('no','return_pct',-1)">Return <span class="sort-arrow"></span></th>
              <th onclick="sortTable('no','distance',-1)">Distance <span class="sort-arrow"></span></th>
              <th onclick="sortTable('no','forecast_temp',1)">Forecast <span class="sort-arrow"></span></th>
              <th onclick="sortTable('no','forecast_confidence',1)">Conf <span class="sort-arrow"></span></th>
              <th onclick="sortTable('no','liquidity',-1)">Liq <span class="sort-arrow"></span></th>
              <th onclick="sortTable('no','recommended_size',1)">Size $ <span class="sort-arrow"></span></th>
              <th>Link</th>
            </tr>
          </thead>
          <tbody id="no-body">
            <tr><td colspan="11"><div class="empty"><div class="empty-icon">📡</div>Run a scan to see opportunities</div></td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Forecasts -->
    <div class="section">
      <div class="section-header">
        <span class="section-title fc-title">Forecasts</span>
        <span class="section-count badge-blue badge" id="fc-count">0</span>
        <span style="font-size:.68rem; color:var(--text-muted);">wttr.in · NWS (US only) · Open-Meteo</span>
      </div>
      <div class="tbl-wrap">
        <table>
          <thead>
            <tr>
              <th onclick="sortTable('fc','city',1)">City <span class="sort-arrow"></span></th>
              <th>Station</th>
              <th onclick="sortTable('fc','date',1)">Date <span class="sort-arrow"></span></th>
              <th>wttr.in</th>
              <th>NWS</th>
              <th>Open-Meteo</th>
              <th onclick="sortTable('fc','consensus',1)">Consensus <span class="sort-arrow"></span></th>
              <th onclick="sortTable('fc','confidence',1)">Confidence <span class="sort-arrow"></span></th>
            </tr>
          </thead>
          <tbody id="fc-body">
            <tr><td colspan="8"><div class="empty">Run a scan to see forecasts</div></td></tr>
          </tbody>
        </table>
      </div>
    </div>

  </div><!-- /panel-main -->

  <!-- ── History tab ─────────────────────────────────────────────────── -->
  <div class="tab-panel" id="panel-history">
    <div class="section">
      <div class="section-header">
        <span class="section-title" style="color:var(--blue)">Scan History</span>
      </div>
      <div class="history-list" id="history-list">
        <div class="empty">No past scans found</div>
      </div>
    </div>
  </div>

</div><!-- /main -->
</div><!-- /layout -->

<div id="toast"></div>

<script>
// ─── State ───────────────────────────────────────────────────────────
let _data = null;
let _yesSorted = [], _noSorted = [], _fcSorted = [];
let _yesSortCol = 'return_pct', _yesSortDir = -1;
let _noSortCol  = 'return_pct', _noSortDir  = -1;
let _fcSortCol  = 'city',       _fcSortDir  = 1;
let _logVisible = false;
let _pollTimer = null;

// ─── Utilities ───────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
function fmt(v, d=1) { return v != null ? (+v).toFixed(d) : '--'; }
function fmtTemp(v, unit) { return v != null ? `${(+v).toFixed(1)}°${unit||'F'}` : '--'; }

function retClass(pct) {
  return pct >= 25 ? 'ret-high' : (pct >= 12 ? 'ret-med' : 'ret-low');
}

function liqClass(v) {
  return v >= 200 ? 'liq-ok' : (v >= 50 ? 'liq-warn' : 'liq-low');
}

function confBadge(c) {
  if (c === 'high')   return '<span class="conf conf-high">✓✓ High</span>';
  if (c === 'medium') return '<span class="conf conf-med">✓ Med</span>';
  return '<span class="conf conf-low">? Low</span>';
}

function cityBadge(city) {
  const us = ['NYC','Chicago','Miami','Dallas','Seattle','Atlanta'];
  const cls = us.includes(city) ? 'badge-us' : 'badge-intl';
  const label = us.includes(city) ? '°F' : '°C';
  return `<span class="city-badge ${cls}">${label}</span>`;
}

function toast(msg) {
  const el = $('toast');
  el.textContent = msg;
  el.classList.add('show');
  setTimeout(() => el.classList.remove('show'), 2200);
}

function copyText(txt) {
  navigator.clipboard.writeText(txt).then(() => toast('Copied!'));
}

// ─── Tabs ────────────────────────────────────────────────────────────
function showTab(name) {
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  $('panel-'+name).classList.add('active');
  $('tab-'+name).classList.add('active');
  if (name === 'history') loadHistory();
}

// ─── Filters ─────────────────────────────────────────────────────────
function getFilters() {
  return {
    minReturn: parseFloat($('min-return-filter').value) || 0,
    minLiq:    parseFloat($('min-liq-filter').value)    || 0,
    conf:      $('conf-filter').value || 'all',
  };
}

function passesFilter(row, isYes) {
  const f = getFilters();
  const ret = row.return_pct;
  const liq = isYes ? row.liquidity_min : row.liquidity;
  const conf = row.forecast_confidence;
  if (ret < f.minReturn) return false;
  if (liq < f.minLiq)    return false;
  if (f.conf === 'high'   && conf !== 'high')   return false;
  if (f.conf === 'medium' && conf === 'low')     return false;
  return true;
}

function applyFilters() {
  if (!_data) return;
  renderYes(_yesSorted);
  renderNo(_noSorted);
}

// ─── Sort ────────────────────────────────────────────────────────────
let _sortState = { yes:{col:'return_pct',dir:-1}, no:{col:'return_pct',dir:-1}, fc:{col:'city',dir:1} };

function sortTable(tbl, col, defaultDir) {
  const s = _sortState[tbl];
  const dir = (s.col === col) ? -s.dir : defaultDir;
  s.col = col; s.dir = dir;

  const arr = tbl === 'yes' ? [...(_data.yes_clusters||[])]
            : tbl === 'no'  ? [...(_data.no_opportunities||[])]
            : [...(_data.forecasts||[])];

  arr.sort((a,b) => {
    let av = a[col], bv = b[col];
    if (av == null) av = dir > 0 ? Infinity : -Infinity;
    if (bv == null) bv = dir > 0 ? Infinity : -Infinity;
    if (typeof av === 'string') return av.localeCompare(bv) * dir;
    return (av - bv) * dir;
  });

  // Update header arrows
  const tableEl = $(tbl === 'yes' ? 'yes-table' : tbl === 'no' ? 'no-table' : 'fc-table-fake');
  document.querySelectorAll(`#${tbl==='yes'?'yes':tbl==='no'?'no':'fc'}-table th`).forEach(th => {
    th.classList.remove('sorted-asc','sorted-desc');
  });
  // Find the th with matching onclick text
  const ths = document.querySelectorAll(
    tbl==='yes' ? '#yes-table th' : tbl==='no' ? '#no-table th' : 'table:last-of-type th'
  );
  ths.forEach(th => {
    const fn = th.getAttribute('onclick') || '';
    if (fn.includes(`'${col}'`)) {
      th.classList.add(dir > 0 ? 'sorted-asc' : 'sorted-desc');
    }
  });

  if (tbl === 'yes') { _yesSorted = arr; renderYes(arr); }
  else if (tbl === 'no') { _noSorted = arr; renderNo(arr); }
  else { _fcSorted = arr; renderFc(arr); }
}

// ─── Render ──────────────────────────────────────────────────────────
function renderYes(clusters) {
  const tb = $('yes-body');
  const filtered = clusters.filter(c => passesFilter(c, true));
  $('yes-count').textContent = filtered.length;

  if (!filtered.length) {
    tb.innerHTML = '<tr><td colspan="13"><div class="empty"><div class="empty-icon">🎯</div>No YES clusters pass current filters</div></td></tr>';
    return;
  }

  tb.innerHTML = filtered.map((c, i) => {
    const u  = c.temp_unit || 'F';
    const labels = c.brackets.map(b => b.group_title).join(' + ');
    const winLo  = c.win_lo != null ? Math.round(c.win_lo) : '<lo';
    const winHi  = c.win_hi != null ? Math.round(c.win_hi)+'°'+u : '→∞';
    const winRange = `${winLo}-${winHi}`;
    const retCls   = retClass(c.return_pct);
    const liqCls   = liqClass(c.liquidity_min);
    const link = c.polymarket_url || '#';

    // Per-bracket tooltip
    const bktDetails = c.brackets.map(b => {
      const fc = b.is_forecast_bracket ? ' ⭐' : '';
      return `<div class="bracket-row">`
        + (b.is_forecast_bracket ? '<span class="forecast-dot"></span>' : '<span style="width:6px;display:inline-block"></span>')
        + `<span style="color:#c4b5fd">${b.group_title}${fc}</span>`
        + `<span style="margin-left:auto; color:var(--text-dim)">YES: ${(+b.yes_price).toFixed(3)}</span>`
        + `<button class="copy-btn" onclick="copyText('${b.yes_token_id}')">📋 token</button>`
        + `</div>`;
    }).join('');

    return `<tr class="${c.forecast_confidence==='high'?'':''}">
      <td class="city-cell">${c.city}${cityBadge(c.city)}</td>
      <td class="date-cell">${c.date}</td>
      <td class="brackets-cell">${labels}</td>
      <td style="text-align:center">${c.cluster_size}</td>
      <td class="price-cell">${fmt(c.total_price, 3)}</td>
      <td class="ret-cell ${retCls}">${fmt(c.return_pct)}%</td>
      <td class="winrange-cell">${winRange}</td>
      <td class="forecast-cell">${fmtTemp(c.forecast_temp, u)}</td>
      <td>${confBadge(c.forecast_confidence)}</td>
      <td class="${liqCls}">$${Math.round(c.liquidity_min)}</td>
      <td class="size-cell">$${fmt(c.size_each, 0)}</td>
      <td class="size-cell">$${fmt(c.total_cost, 0)}</td>
      <td class="link-cell"><a href="${link}" target="_blank">Open ↗</a></td>
    </tr>
    <tr id="detail-yes-${i}" class="bracket-detail">
      <td colspan="13" style="padding:0">
        <div style="padding:8px 16px;">${bktDetails}</div>
      </td>
    </tr>`;
  }).join('');
}

function renderNo(opps) {
  const tb = $('no-body');
  const filtered = opps.filter(o => passesFilter(o, false));
  $('no-count').textContent = filtered.length;

  if (!filtered.length) {
    tb.innerHTML = '<tr><td colspan="11"><div class="empty"><div class="empty-icon">🎯</div>No NO bets pass current filters</div></td></tr>';
    return;
  }

  tb.innerHTML = filtered.map(o => {
    const u       = o.temp_unit || 'F';
    const dist    = o.distance == null ? 'far' : fmt(o.distance, 0) + '°' + u;
    const retCls  = retClass(o.return_pct);
    const liqCls  = liqClass(o.liquidity);
    const link    = o.polymarket_url || '#';

    return `<tr>
      <td class="city-cell">${o.city}${cityBadge(o.city)}</td>
      <td class="date-cell">${o.date}</td>
      <td class="brackets-cell">${o.bracket}</td>
      <td class="price-cell">${fmt(o.no_price, 3)}</td>
      <td class="ret-cell ${retCls}">${fmt(o.return_pct)}%</td>
      <td class="forecast-cell">${dist}</td>
      <td class="forecast-cell">${fmtTemp(o.forecast_temp, u)}</td>
      <td>${confBadge(o.forecast_confidence)}</td>
      <td class="${liqCls}">$${Math.round(o.liquidity)}</td>
      <td class="size-cell">$${fmt(o.recommended_size, 0)}</td>
      <td class="link-cell">
        <a href="${link}" target="_blank">Open ↗</a>
        <button class="copy-btn" onclick="copyText('${o.no_token_id||''}')" title="Copy NO token ID">📋</button>
      </td>
    </tr>`;
  }).join('');
}

function renderFc(fcs) {
  const tb = $('fc-body');
  $('fc-count').textContent = fcs.length;

  if (!fcs.length) {
    tb.innerHTML = '<tr><td colspan="8"><div class="empty">Run a scan to see forecasts</div></td></tr>';
    return;
  }

  tb.innerHTML = fcs.map(f => {
    const u = f.unit || 'F';
    const confMap = { high:'fc-high', medium:'fc-med', low:'fc-low' };
    const cls = confMap[f.confidence] || 'fc-low';
    const spread = (f.wttr != null && f.open_meteo != null)
      ? Math.abs(f.wttr - f.open_meteo).toFixed(1)
      : null;

    return `<tr>
      <td class="city-cell">${f.city}${cityBadge(f.city)}</td>
      <td style="color:var(--text-muted); font-size:.72rem">${f.station}</td>
      <td class="date-cell">${f.date}</td>
      <td class="forecast-cell">${f.wttr  != null ? fmtTemp(f.wttr, u)       : '<span style="color:var(--text-muted)">—</span>'}</td>
      <td class="forecast-cell">${f.nws   != null ? fmtTemp(f.nws, u)        : '<span style="color:var(--text-muted)">—</span>'}</td>
      <td class="forecast-cell">${f.open_meteo != null ? fmtTemp(f.open_meteo, u) : '<span style="color:var(--text-muted)">—</span>'}</td>
      <td class="${cls}" style="font-weight:700">${fmtTemp(f.consensus, u)}${spread ? ` <span style="font-size:.65rem; font-weight:400; color:var(--text-muted)">±${spread}</span>` : ''}</td>
      <td>${confBadge(f.confidence)}</td>
    </tr>`;
  }).join('');
}

// ─── Render all ──────────────────────────────────────────────────────
function render(d) {
  if (!d || d.error) return;
  _data = d;

  // Header
  const ts = d.scan_time ? new Date(d.scan_time).toLocaleString() : '—';
  $('header-meta').innerHTML = `Last scan: <span>${ts}</span>`;

  // Cards
  const yc = (d.yes_clusters || []).length;
  const nc = (d.no_opportunities || []).length;
  const dep = (d.summary || {}).estimated_deploy_usd || 0;
  const highCount = [...(d.yes_clusters||[]), ...(d.no_opportunities||[])].filter(x => x.forecast_confidence === 'high').length;

  $('cards').innerHTML = `
    <div class="card yes">
      <div class="label">YES Clusters</div>
      <div class="value">${yc}</div>
      <div class="sub">${(d.yes_clusters||[]).filter(c=>c.forecast_confidence==='high').length} high-confidence</div>
    </div>
    <div class="card no">
      <div class="label">NO Bets</div>
      <div class="value">${nc}</div>
      <div class="sub">${(d.no_opportunities||[]).filter(o=>o.forecast_confidence==='high').length} high-confidence</div>
    </div>
    <div class="card dep">
      <div class="label">Est. Deploy</div>
      <div class="value">$${Math.round(dep)}</div>
      <div class="sub">${yc+nc} total opportunities</div>
    </div>
    <div class="card scan">
      <div class="label">Last Scan</div>
      <div class="value">${ts}</div>
    </div>
  `;

  // Sort & render tables
  _yesSorted = [...(d.yes_clusters||[])].sort((a,b) =>
    (b.forecast_confidence==='high'?1:0) - (a.forecast_confidence==='high'?1:0) || b.return_pct - a.return_pct);
  _noSorted  = [...(d.no_opportunities||[])].sort((a,b) =>
    (b.forecast_confidence==='high'?1:0) - (a.forecast_confidence==='high'?1:0) || b.return_pct - a.return_pct);
  _fcSorted  = [...(d.forecasts||[])].sort((a,b) => a.city.localeCompare(b.city));

  renderYes(_yesSorted);
  renderNo(_noSorted);
  renderFc(_fcSorted);
}

// ─── Data load ───────────────────────────────────────────────────────
function loadData(file) {
  const url = file ? `/data?file=${encodeURIComponent(file)}` : '/data';
  fetch(url)
    .then(r => r.json())
    .then(render)
    .catch(() => {});
}

// ─── History ─────────────────────────────────────────────────────────
function loadHistory() {
  fetch('/history')
    .then(r => r.json())
    .then(files => {
      $('history-badge').textContent = files.length;
      const el = $('history-list');
      if (!files.length) {
        el.innerHTML = '<div class="empty">No past scans found</div>';
        return;
      }
      el.innerHTML = files.map(f => {
        const ts = f.replace('scan_','').replace('.json','');
        const year = ts.slice(0,4), mo = ts.slice(4,6), day = ts.slice(6,8);
        const hr = ts.slice(9,11), min = ts.slice(11,13);
        const label = `${year}-${mo}-${day} ${hr}:${min}`;
        return `<div class="history-item" onclick="loadData('${f}'); showTab('main'); toast('Loaded ${label}')">
          <div style="flex:1">
            <div class="history-time">${label}</div>
            <div class="history-stats">Click to view</div>
          </div>
          <span style="color:var(--text-muted); font-size:.75rem">→</span>
        </div>`;
      }).join('');
    });
}

// ─── Scan ────────────────────────────────────────────────────────────
function runScan() {
  if ($('scanbar').classList.contains('scanning')) return;
  const capital = $('capital').value || 400;
  const days    = $('days').value    || 1;
  $('scanbar').classList.add('scanning');
  $('log-wrap').style.display = 'block';
  _logVisible = true;
  $('scan-status').textContent = 'Scanning...';
  $('log-wrap').textContent = 'Starting scan...';

  fetch('/scan', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ capital: parseInt(capital), days: parseInt(days) })
  });

  _pollTimer = setInterval(pollScan, 1500);
}

function pollScan() {
  fetch('/scan/status')
    .then(r => r.json())
    .then(s => {
      $('log-wrap').textContent = s.log.join('\n');
      $('log-wrap').scrollTop = $('log-wrap').scrollHeight;

      if (!s.running) {
        clearInterval(_pollTimer);
        $('scanbar').classList.remove('scanning');
        $('scan-status').textContent = 'Done';
        setTimeout(() => { $('scan-status').textContent = 'Ready'; }, 3000);
        loadData();
        toast('Scan complete!');
      }
    });
}

function toggleLog() {
  _logVisible = !_logVisible;
  $('log-wrap').style.display = _logVisible ? 'block' : 'none';
  $('log-toggle-btn').textContent = _logVisible ? 'Hide Log' : 'Log';
}

// ─── CSV Export ──────────────────────────────────────────────────────
function exportCSV(type) {
  if (!_data) { toast('No data to export'); return; }
  const rows = type === 'yes' ? _yesSorted : _noSorted;
  if (!rows.length) { toast('No rows to export'); return; }

  let header, lines;
  if (type === 'yes') {
    header = 'city,date,brackets,cluster_size,total_price,return_pct,win_lo,win_hi,forecast_temp,temp_unit,forecast_confidence,liquidity_min,size_each,total_cost,url\n';
    lines = rows.map(c =>
      `${c.city},${c.date},"${c.brackets.map(b=>b.group_title).join(' + ')}",${c.cluster_size},${c.total_price},${c.return_pct},${c.win_lo??''},${c.win_hi??''},${c.forecast_temp},${c.temp_unit||'F'},${c.forecast_confidence},${c.liquidity_min},${c.size_each},${c.total_cost},${c.polymarket_url}`
    );
  } else {
    header = 'city,date,bracket,no_price,return_pct,distance,forecast_temp,temp_unit,forecast_confidence,liquidity,recommended_size,url\n';
    lines = rows.map(o =>
      `${o.city},${o.date},${o.bracket},${o.no_price},${o.return_pct},${o.distance??''},${o.forecast_temp},${o.temp_unit||'F'},${o.forecast_confidence},${o.liquidity},${o.recommended_size},${o.polymarket_url}`
    );
  }

  const blob = new Blob([header + lines.join('\n')], {type: 'text/csv'});
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = `polymarket_${type}_${new Date().toISOString().slice(0,10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
  toast(`Exported ${lines.length} rows`);
}

// ─── Keyboard shortcuts ──────────────────────────────────────────────
document.addEventListener('keydown', e => {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT') return;
  if (e.key === 'r' || e.key === 'R') runScan();
  if (e.key === 'f' || e.key === 'F') $('min-return-filter').focus();
});

// ─── Init ────────────────────────────────────────────────────────────
loadData();
setInterval(loadData, 90000);  // auto-refresh every 90s
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/data")
def data():
    file = request.args.get("file")
    if file:
        path = os.path.join(DATA_DIR, os.path.basename(file))
    else:
        path = LATEST

    if not os.path.exists(path):
        return jsonify({"error": "no scan data"})

    with open(path) as f:
        scan = json.load(f)

    # Normalize old format
    if "opportunities" in scan and "yes_clusters" not in scan:
        no_list = [o for o in scan["opportunities"] if o.get("signal") == "NO"]
        scan["yes_clusters"] = []
        scan["no_opportunities"] = [{
            "city": o.get("city"),
            "date": o.get("date"),
            "station": o.get("station"),
            "event_slug": o.get("event_slug"),
            "bracket": o.get("bracket"),
            "no_price": o.get("price"),
            "yes_price": 1 - (o.get("price") or 0),
            "return_pct": o.get("return_pct"),
            "distance": o.get("distance_f"),
            "temp_unit": "F",
            "forecast_temp": o.get("forecast_temp"),
            "forecast_confidence": o.get("forecast_confidence"),
            "liquidity": o.get("liquidity"),
            "recommended_size": o.get("recommended_size"),
            "yes_token_id": "",
            "no_token_id": "",
            "polymarket_url": o.get("polymarket_url"),
        } for o in no_list]

    scan.setdefault("forecasts", [])
    return jsonify(scan)


@app.route("/history")
def history():
    return jsonify(list_scans())


@app.route("/scan", methods=["POST"])
def trigger_scan():
    global _scan_running
    if _scan_running:
        return jsonify({"status": "already running"})
    body = request.get_json(silent=True) or {}
    capital = body.get("capital", 400)
    days    = body.get("days", 1)
    t = threading.Thread(target=run_scan_bg, args=(None, capital, days), daemon=True)
    t.start()
    return jsonify({"status": "started"})


@app.route("/scan/status")
def scan_status():
    return jsonify({"running": _scan_running, "log": _scan_log[-150:]})


@app.route("/export/csv/<type>")
def export_csv(type):
    """Server-side CSV export fallback."""
    scan = load_scan()
    if not scan:
        return "No scan data", 404

    output = io.StringIO()
    writer = csv.writer(output)

    if type == "yes":
        writer.writerow(["city","date","brackets","cluster_size","total_price",
                          "return_pct","win_lo","win_hi","forecast_temp","temp_unit",
                          "forecast_confidence","liquidity_min","size_each","total_cost","url"])
        for c in scan.get("yes_clusters", []):
            labels = " + ".join(b["group_title"] for b in c.get("brackets", []))
            writer.writerow([c["city"], c["date"], labels, c["cluster_size"],
                             c["total_price"], c["return_pct"],
                             c.get("win_lo",""), c.get("win_hi",""),
                             c["forecast_temp"], c.get("temp_unit","F"),
                             c["forecast_confidence"], c["liquidity_min"],
                             c["size_each"], c["total_cost"], c.get("polymarket_url","")])
    else:
        writer.writerow(["city","date","bracket","no_price","return_pct","distance",
                          "forecast_temp","temp_unit","forecast_confidence",
                          "liquidity","recommended_size","url"])
        for o in scan.get("no_opportunities", []):
            writer.writerow([o["city"], o["date"], o["bracket"], o["no_price"],
                             o["return_pct"], o.get("distance",""),
                             o["forecast_temp"], o.get("temp_unit","F"),
                             o["forecast_confidence"], o["liquidity"],
                             o["recommended_size"], o.get("polymarket_url","")])

    output.seek(0)
    return send_file(
        io.BytesIO(output.read().encode()),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"polymarket_{type}_{datetime.now().strftime('%Y%m%d')}.csv",
    )


def _start_scheduler():
    """Start background auto-scan scheduler (Railway + local)."""
    if SCAN_INTERVAL_HOURS <= 0:
        print("Auto-scan disabled (SCAN_INTERVAL_HOURS=0)")
        return
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
    except ImportError:
        print("apscheduler not installed — auto-scan disabled. pip install apscheduler")
        return

    scheduler = BackgroundScheduler(daemon=True)

    # Run on schedule
    scheduler.add_job(
        lambda: threading.Thread(target=run_scan_bg, daemon=True).start(),
        "interval",
        hours=SCAN_INTERVAL_HOURS,
        id="auto_scan",
    )

    # Also run once at startup (30s delay so the server is fully up first)
    run_at = datetime.now() + timedelta(seconds=30)
    scheduler.add_job(
        lambda: threading.Thread(target=run_scan_bg, daemon=True).start(),
        "date",
        run_date=run_at,
        id="startup_scan",
    )

    scheduler.start()
    print(f"Auto-scan scheduled every {SCAN_INTERVAL_HOURS}h (first run in 30s)")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8888))
    # Bind to 0.0.0.0 on Railway so it's reachable externally
    host = "0.0.0.0" if os.environ.get("RAILWAY_ENVIRONMENT") else "127.0.0.1"
    _start_scheduler()
    print(f"\nPolymarket Weather Scanner Dashboard")
    print(f"Open: http://localhost:{port}")
    print(f"Auto-scan: every {SCAN_INTERVAL_HOURS}h | Capital: ${SCAN_CAPITAL}")
    print(f"Keys: R = run scan, F = focus filter\n")
    app.run(host=host, port=port, debug=False)
