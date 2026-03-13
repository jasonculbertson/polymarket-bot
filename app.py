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
from flask import Flask, jsonify, render_template, request, send_file
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

_scan_lock    = threading.Lock()
_scan_running = False
_scan_log     = []

# Quick monitor state — runs every 15 min independent of full scan
_monitor_log:  list     = []
_last_monitor: str      = None   # ISO timestamp of last successful quick monitor

_last_resolve_time: datetime = None
_RESOLVE_COOLDOWN_SECS = 300  # re-run resolve at most once every 5 min
_resolve_lock = threading.Lock()


def _maybe_run_resolve_background():
    """Run resolve_outcomes in a background thread if cooldown has passed (so dashboard load also fills pending)."""
    global _last_resolve_time
    now = datetime.now()
    with _resolve_lock:
        if _last_resolve_time is not None and (now - _last_resolve_time).total_seconds() <= _RESOLVE_COOLDOWN_SECS:
            return
        _last_resolve_time = now
    def _run():
        try:
            from tracker import resolve_outcomes
            resolve_outcomes()
        except Exception as e:
            print(f"[WARN] background resolve: {e}")
    t = threading.Thread(target=_run, daemon=True)
    t.start()


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


def _run_quick_monitor(log: list = None):
    """
    Lightweight position monitor — no full market scan needed.
    Runs every 15 min (and also at the end of every full scan).

    Steps:
      1. Refresh open position prices + simulated stop-loss tracking
      2. Forecast drift check — flag positions whose edge has evaporated
      3. Micro-learn — update city volatility, source divergence, volume trend

    log: optional list to append status lines to (pass _scan_log during full scan,
         or _monitor_log during the standalone 15-min job).
    """
    global _last_monitor
    if log is None:
        log = _monitor_log

    # 1. Price monitor
    try:
        from tracker import update_open_position_prices
        price_result = update_open_position_prices()
        log.append(f"[price-monitor] {price_result}")
    except Exception as e:
        log.append(f"[price-monitor] ERROR: {e}")

    # 2. Forecast drift
    try:
        from tracker import check_forecast_drift
        drift_result = check_forecast_drift()
        if drift_result.get("flagged", 0):
            log.append(f"[forecast-drift] ⚠ {drift_result['flagged']} position(s) edge-gone — {drift_result}")
        else:
            log.append(f"[forecast-drift] {drift_result}")
    except Exception as e:
        log.append(f"[forecast-drift] ERROR: {e}")

    # 3. Micro-learn
    try:
        from micro_learner import post_scan_learn
        ml = post_scan_learn()
        msg = f"[micro-learn] #{ml.get('scans_recorded')} | vol={ml.get('volume_trend','?')}"
        if ml.get("volatile_cities"):
            msg += f" | volatile: {ml['volatile_cities']}"
        if ml.get("divergent_cities"):
            msg += f" | WU≠meteo: {ml['divergent_cities']}"
        log.append(msg)
    except Exception as e:
        log.append(f"[micro-learn] ERROR: {e}")

    _last_monitor = datetime.now().isoformat()


def _quick_monitor_job():
    """
    APScheduler wrapper for _run_quick_monitor().
    Skips silently if a full scan is already running (it will cover monitoring at the end).
    Keeps the last 200 monitor log lines for the dashboard.
    """
    global _monitor_log
    if _scan_running:
        return   # full scan already running — it handles monitoring at completion
    if len(_monitor_log) > 200:
        _monitor_log = _monitor_log[-200:]
    _monitor_log.append(f"--- monitor {datetime.now().strftime('%H:%M:%S')} ---")
    threading.Thread(target=_run_quick_monitor, daemon=True).start()


def run_scan_bg(cities=None, capital=None, days=1, target_date=None):
    global _scan_running, _scan_log
    if not _scan_lock.acquire(blocking=False):
        return
    _scan_running = True
    _scan_log     = []
    capital = capital or SCAN_CAPITAL
    # Use sys.executable so Railway uses the correct venv Python
    # Default: scan both today + tomorrow (target_date=None means "both")
    cmd = [sys.executable, "scan.py", "--capital", str(capital)]
    if target_date:
        cmd += ["--date", target_date]
    # no --days needed — scan.py now defaults to today+tomorrow automatically
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
        _run_quick_monitor(log=_scan_log)
    except Exception as e:
        _scan_log.append(f"ERROR: {e}")
    finally:
        _scan_running = False
        _scan_lock.release()


TAKEN_FILE = os.path.join(DATA_DIR, "taken.json")
DATABASE_URL = os.environ.get("DATABASE_URL", "")


def _pg_kv_load(key: str):
    """Load a JSON value from kv_store table (created by tracker.py)."""
    try:
        import psycopg2, json as _json
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT data FROM kv_store WHERE key = %s", (key,))
                row = cur.fetchone()
            if not row:
                return None
            val = row[0]
            # psycopg2 returns a dict when column is JSONB, str when TEXT
            if isinstance(val, (dict, list)):
                return val
            return _json.loads(val)
        finally:
            conn.close()
    except Exception as e:
        print(f"[WARN] _pg_kv_load({key}) failed: {e}")
        return None


def _pg_ensure_table():
    """Ensure kv_store table exists (matches tracker.py schema: JSONB, TIMESTAMPTZ)."""
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS kv_store (
                        key        TEXT PRIMARY KEY,
                        data       JSONB NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        print(f"[WARN] _pg_ensure_table failed: {e}")


def _pg_kv_save(key: str, data):
    _pg_ensure_table()
    try:
        import psycopg2, json as _json
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO kv_store (key, data, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE
                        SET data = EXCLUDED.data, updated_at = NOW()
                """, (key, _json.dumps(data)))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        print(f"[WARN] PG save({key}) failed: {e}")


def _pg_list_scan_dates() -> list:
    """Return sorted list of scan date keys stored in Postgres (newest first)."""
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn.cursor() as cur:
                # Order by key name as fallback when updated_at is NULL
                cur.execute(
                    "SELECT key, updated_at FROM kv_store WHERE key LIKE 'scan_%' ORDER BY key DESC"
                )
                rows = cur.fetchall()
            dates = []
            for key, updated_at in rows:
                date_part = key[len("scan_"):]
                saved_at = updated_at.isoformat() if updated_at else ""
                dates.append({"date": date_part, "saved_at": saved_at})
            return dates
        finally:
            conn.close()
    except Exception as e:
        print(f"[WARN] pg_list_scan_dates failed: {e}")
        return []


@app.route("/debug/data")
def debug_data():
    """Show what _pg_merge_latest() returns — useful for diagnosing blank dashboard."""
    dates = _pg_list_scan_dates() if DATABASE_URL else []
    merged = _pg_merge_latest() if DATABASE_URL else None
    from datetime import date as _date, timedelta
    today    = _date.today().isoformat()
    tomorrow = (_date.today() + timedelta(days=1)).isoformat()
    return jsonify({
        "database_url_set": bool(DATABASE_URL),
        "pg_scan_dates": dates,
        "today": today,
        "tomorrow": tomorrow,
        "merged_keys": list(merged.keys()) if merged else None,
        "yes_clusters_count": len(merged.get("yes_clusters", [])) if merged else None,
        "no_opps_count": len(merged.get("no_opportunities", [])) if merged else None,
        "scan_time": merged.get("scan_time") if merged else None,
        "local_latest_exists": os.path.exists(LATEST),
    })


@app.route("/debug/pg")
def debug_pg():
    """Show all keys in Postgres kv_store — useful for diagnosing blank dashboard."""
    if not DATABASE_URL:
        return jsonify({"error": "DATABASE_URL not set"})
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT key, updated_at, length(data::text) as size FROM kv_store ORDER BY key")
                rows = cur.fetchall()
            return jsonify([
                {"key": r[0], "updated_at": r[1].isoformat() if r[1] else None, "bytes": r[2]}
                for r in rows
            ])
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)})


def load_taken() -> set:
    if DATABASE_URL:
        data = _pg_kv_load("taken")
        if data is not None:
            return set(data.get("taken", []))
    if not os.path.exists(TAKEN_FILE):
        return set()
    with open(TAKEN_FILE) as f:
        return set(json.load(f).get("taken", []))


def save_taken(taken_set: set):
    payload = {"taken": list(taken_set), "updated": datetime.now().isoformat()}
    if DATABASE_URL:
        _pg_kv_save("taken", payload)
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(TAKEN_FILE, "w") as f:
        json.dump(payload, f)




@app.route("/favicon.ico")
def favicon():
    """Avoid 404 when the browser requests a favicon."""
    return "", 204


@app.route("/")
def index():
    return render_template("index.html")


def _normalize_scan(scan: dict) -> dict:
    """Normalize old scan formats to current structure."""
    if "opportunities" in scan and "yes_clusters" not in scan:
        no_list = [o for o in scan["opportunities"] if o.get("signal") == "NO"]
        scan["yes_clusters"] = []
        scan["no_opportunities"] = [{
            "city": o.get("city"), "date": o.get("date"),
            "station": o.get("station"), "event_slug": o.get("event_slug"),
            "bracket": o.get("bracket"), "no_price": o.get("price"),
            "yes_price": 1 - (o.get("price") or 0), "return_pct": o.get("return_pct"),
            "distance": o.get("distance_f"), "temp_unit": "F",
            "forecast_temp": o.get("forecast_temp"),
            "forecast_confidence": o.get("forecast_confidence"),
            "liquidity": o.get("liquidity"), "recommended_size": o.get("recommended_size"),
            "yes_token_id": "", "no_token_id": "", "polymarket_url": o.get("polymarket_url"),
        } for o in no_list]
    scan.setdefault("forecasts", [])
    return scan


def _pg_merge_latest() -> dict | None:
    """Load and merge the most recent scans from Postgres (today + tomorrow preferred)."""
    if not DATABASE_URL:
        return None
    dates = _pg_list_scan_dates()
    if not dates:
        return None
    from datetime import date as _date, timedelta
    today    = _date.today().isoformat()
    tomorrow = (_date.today() + timedelta(days=1)).isoformat()
    target_dates = {today, tomorrow}

    # First pass: try to load today + tomorrow
    loaded = []
    for entry in dates:
        d = entry["date"]
        if d in target_dates:
            scan = _pg_kv_load(f"scan_{d}")
            if scan:
                loaded.append(_normalize_scan(scan))
        if len(loaded) >= 2:
            break

    # Fallback: load the two most-recent scans regardless of date
    if not loaded:
        for entry in dates[:2]:
            scan = _pg_kv_load(f"scan_{entry['date']}")
            if scan:
                loaded.append(_normalize_scan(scan))

    if not loaded:
        return None
    if len(loaded) == 1:
        return loaded[0]

    # Merge multiple date scans into one combined payload
    merged = {
        "scan_time": loaded[0].get("scan_time", ""),
        "yes_clusters":    [],
        "no_opportunities":[],
        "forecasts":       [],
        "summary":         {},
    }
    for s in loaded:
        merged["yes_clusters"]     += s.get("yes_clusters", [])
        merged["no_opportunities"] += s.get("no_opportunities", [])
        merged["forecasts"]        += s.get("forecasts", [])
    total = len(merged["yes_clusters"]) + len(merged["no_opportunities"])
    deploy = sum(c.get("total_cost", 0) for c in merged["yes_clusters"]) + \
             sum(o.get("recommended_size", 0) for o in merged["no_opportunities"])
    merged["summary"] = {
        "yes_clusters": len(merged["yes_clusters"]),
        "no_bets": len(merged["no_opportunities"]),
        "total_opportunities": total,
        "estimated_deploy_usd": round(deploy, 2),
    }
    return merged


@app.route("/data")
def data():
    scan_date = request.args.get("date")   # YYYY-MM-DD
    file      = request.args.get("file")   # legacy file-based lookup

    # 1. Specific date requested → load that date from Postgres only (no silent fallthrough)
    if scan_date:
        if DATABASE_URL:
            scan = _pg_kv_load(f"scan_{scan_date}")
            if scan:
                return jsonify(_normalize_scan(scan))
        return jsonify({"error": f"No scan data found for {scan_date}"}), 404

    # 2. No specific date → try Postgres merged view first (survives redeployments)
    if not file and DATABASE_URL:
        merged = _pg_merge_latest()
        if merged:
            try:
                from tracker import record_scan_from_merged
                record_scan_from_merged(merged)
            except Exception as e:
                print(f"[WARN] outcomes backfill from merged: {e}")
            _maybe_run_resolve_background()
            return jsonify(merged)

    # 3. Fall back to local file (only available on current deployment)
    path = os.path.join(DATA_DIR, os.path.basename(file)) if file else LATEST
    if not os.path.exists(path):
        return jsonify({"error": "no scan data"}), 404

    with open(path) as f:
        scan = json.load(f)

    return jsonify(_normalize_scan(scan))


@app.route("/data/dates")
def data_dates():
    """Return list of available scan dates from Postgres + local files."""
    dates = []
    if DATABASE_URL:
        dates = _pg_list_scan_dates()
    # Also include any local scan files as fallback
    if not dates:
        files = list_scans()
        for f in files:
            # filename: scan_YYYYMMDD_HHMM.json → extract date
            try:
                d = f[5:13]  # YYYYMMDD
                date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                dates.append({"date": date_str, "saved_at": "", "file": f})
            except Exception:
                pass
    return jsonify(dates)


@app.route("/history")
def history():
    return jsonify(list_scans())


@app.route("/taken")
def get_taken():
    return jsonify(list(load_taken()))


@app.route("/take", methods=["POST"])
def take():
    body = request.get_json(silent=True) or {}
    opp_id = body.get("id", "").strip()
    action = body.get("action", "take")
    if not opp_id:
        return jsonify({"error": "missing id"}), 400
    taken = load_taken()
    if action == "take":
        taken.add(opp_id)
    else:
        taken.discard(opp_id)
    save_taken(taken)
    return jsonify({"taken": opp_id in taken, "total": len(taken)})


@app.route("/outcomes")
def outcomes():
    global _last_resolve_time
    try:
        from tracker import get_summary, resolve_outcomes
        # Throttle resolve_outcomes to at most once per cooldown period
        now = datetime.now()
        if (_last_resolve_time is None or
                (now - _last_resolve_time).total_seconds() > _RESOLVE_COOLDOWN_SECS):
            resolve_outcomes()
            _last_resolve_time = now
        return jsonify(get_summary())
    except Exception as e:
        return jsonify({"error": str(e), "total": 0, "resolved": 0,
                        "wins": 0, "losses": 0, "pending": 0, "recent": []})


@app.route("/outcomes/correct", methods=["POST"])
def outcomes_correct():
    """
    Manually correct a resolved market's actual temperature and re-infer outcome.

    POST JSON: { "event_slug": "...", "actual_temp": 75.0 }
      OR       { "opp_id": "yes_...", "actual_temp": 75.0 }

    Use when the bot resolved a market with a bad WU value (e.g. resolved too
    early before WU compiled the day's history). The corrected actual_temp is
    used to re-infer win/loss from the bracket and update P&L.
    """
    try:
        from tracker import _load, _save, _tracker_lock, _infer_outcome_from_actual_temp, PAPER_SIZE_USD
        body = request.get_json(force=True) or {}
        actual_temp = body.get("actual_temp")
        event_slug  = body.get("event_slug", "")
        opp_id      = body.get("opp_id", "")

        if actual_temp is None:
            return jsonify({"error": "actual_temp required"}), 400
        actual_temp = float(actual_temp)

        with _tracker_lock:
            data = _load()

        matched = []
        for opp in data["opportunities"]:
            if opp_id and opp.get("id") != opp_id:
                continue
            if event_slug and opp.get("event_slug") != event_slug:
                continue
            matched.append(opp)

        if not matched:
            return jsonify({"error": "no matching opportunity found"}), 404

        updated = []
        for opp in matched:
            outcome = _infer_outcome_from_actual_temp(opp, actual_temp)
            if not outcome:
                continue
            entry = opp["entry_price"]
            stake = opp.get("paper_size_usd") or round(PAPER_SIZE_USD * opp.get("cluster_size", 1), 2)
            pnl_pct = round((1.0 - entry) / entry * 100, 2) if outcome == "win" else -100.0
            wu_pred = (opp.get("forecast_sources") or {}).get("wunderground")

            opp["outcome"]       = outcome
            opp["actual_temp"]   = actual_temp
            opp["final_yes_price"] = 0.0 if outcome == "win" else 1.0
            opp["pnl_pct"]       = pnl_pct
            opp["paper_size_usd"] = stake
            # Equal-shares paper P&L: payout = shares×$1 when any bracket wins
            if outcome == "win" and opp.get("shares") and opp["shares"] > 0:
                opp["paper_pnl_usd"] = round(opp["shares"] - stake, 2)
            else:
                opp["paper_pnl_usd"] = round(stake * (pnl_pct / 100.0), 2)
            opp["corrected"]     = True
            if wu_pred is not None:
                opp["wu_error"] = round(abs(wu_pred - actual_temp), 1)

            updated.append({
                "id": opp["id"], "city": opp["city"], "bracket": opp["bracket"],
                "actual_temp": actual_temp, "outcome": outcome, "pnl_pct": pnl_pct,
            })

        if updated:
            with _tracker_lock:
                _save(data)

        return jsonify({"corrected": len(updated), "markets": updated})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/outcomes/backfill")
def outcomes_backfill():
    """Run resolution once (no cooldown) to fill pending outcomes. Call once to manually fill the table."""
    try:
        from tracker import get_summary, resolve_outcomes
        n = resolve_outcomes()
        summary = get_summary()
        summary["backfill_run"] = True
        summary["newly_resolved"] = n
        return jsonify(summary)
    except Exception as e:
        return jsonify({"error": str(e), "backfill_run": False, "newly_resolved": 0})


@app.route("/learning")
def learning():
    try:
        from learner import get_learning_stats, WU_PWS_KEY
        stats = get_learning_stats()
        stats["wu_active"]  = True   # page scraper always available
        stats["pws_active"] = bool(WU_PWS_KEY)
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/learn", methods=["POST"])
def api_learn():
    """
    Run the full daily learn pipeline on demand:
      resolve_outcomes() → learn_from_outcomes() → run_daily_optimizer() → save to Postgres

    Returns the optimizer report (includes issues list).
    Also available automatically via the daily 8am UTC cron job.
    """
    try:
        from tracker import resolve_outcomes, _load
        from learner import learn_from_outcomes
        from optimizer import run_daily_optimizer

        resolve_outcomes()
        learn_result = learn_from_outcomes()
        data = _load()
        report = run_daily_optimizer(data)
        _pg_kv_save("daily_report", report)

        return jsonify({
            "ok":        True,
            "learn":     {k: v for k, v in learn_result.items() if k != "current_weights"},
            "optimizer": report,
        })
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/report")
def api_report():
    """Return the latest daily optimization report from Postgres."""
    report = _pg_kv_load("daily_report")
    if report is None:
        return jsonify({"error": "No report yet — call POST /api/learn first"}), 404
    return jsonify(report)


@app.route("/api/positions")
def api_positions():
    """
    Return all open (unresolved) positions with current mark-to-market prices,
    unrealized P&L, and simulated stop-loss status.

    Query params:
      ?refresh=1  — re-fetch current prices before returning (slower but fresh)
    """
    try:
        from tracker import _load, update_open_position_prices
        if request.args.get("refresh") == "1":
            update_open_position_prices()
        data = _load()
        open_opps = [o for o in data["opportunities"] if o.get("outcome") is None]
        # Sort by unrealized P&L (worst first so problem bets surface at top)
        open_opps.sort(key=lambda o: o.get("unrealized_pnl_usd") or 0)
        total_unrealized = round(
            sum(o.get("unrealized_pnl_usd") or 0 for o in open_opps), 2
        )
        stop_loss_count = sum(1 for o in open_opps if o.get("simulated_stop_loss_triggered"))
        return jsonify({
            "open_positions": len(open_opps),
            "total_unrealized_pnl_usd": total_unrealized,
            "simulated_stop_losses_triggered": stop_loss_count,
            "positions": open_opps,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/scan", methods=["POST"])
def trigger_scan():
    global _scan_running
    if _scan_running:
        return jsonify({"status": "already running"})
    body = request.get_json(silent=True) or {}
    capital     = body.get("capital", 400)
    target_date = body.get("date") or None  # None = scan both today+tomorrow
    t = threading.Thread(target=run_scan_bg,
                         kwargs=dict(capital=capital, target_date=target_date),
                         daemon=True)
    t.start()
    return jsonify({"status": "started"})


@app.route("/scan/status")
def scan_status():
    return jsonify({"running": _scan_running, "log": _scan_log[-150:]})


@app.route("/export/csv/<type>")
def export_csv(type):
    """Server-side CSV export. Uses same data source as dashboard (Postgres merged or local LATEST)."""
    raw = None
    if DATABASE_URL:
        raw = _pg_merge_latest()
    if not raw:
        raw = load_scan()
    if not raw:
        return "No scan data", 404
    scan = _normalize_scan(raw)

    output = io.StringIO()
    writer = csv.writer(output)

    if type == "yes":
        writer.writerow(["city","date","brackets","cluster_size","total_price",
                          "return_pct","win_lo","win_hi","forecast_temp","temp_unit",
                          "forecast_confidence","liquidity_min","size_each","total_cost","url"])
        for c in scan.get("yes_clusters", []):
            labels = " + ".join(b["group_title"] for b in c.get("brackets", []))
            writer.writerow([c.get("city",""), c.get("date",""), labels, c.get("cluster_size",""),
                             c.get("total_price",""), c.get("return_pct",""),
                             c.get("win_lo",""), c.get("win_hi",""),
                             c.get("forecast_temp",""), c.get("temp_unit","F"),
                             c.get("forecast_confidence",""), c.get("liquidity_min",""),
                             c.get("size_each",""), c.get("total_cost",""), c.get("polymarket_url","")])
    else:
        writer.writerow(["city","date","bracket","no_price","return_pct","distance",
                          "forecast_temp","temp_unit","forecast_confidence",
                          "liquidity","recommended_size","url"])
        for o in scan.get("no_opportunities", []):
            writer.writerow([o.get("city",""), o.get("date",""), o.get("bracket",""), o.get("no_price",""),
                             o.get("return_pct",""), o.get("distance",""),
                             o.get("forecast_temp",""), o.get("temp_unit","F"),
                             o.get("forecast_confidence",""), o.get("liquidity",""),
                             o.get("recommended_size",""), o.get("polymarket_url","")])

    output.seek(0)
    return send_file(
        io.BytesIO(output.read().encode()),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"polymarket_{type}_{datetime.now().strftime('%Y%m%d')}.csv",
    )


_scheduler      = None
_last_auto_scan = None   # ISO string of last auto-scan start time


def _is_circuit_breaker_tripped() -> bool:
    """Return True if today's paper P&L loss exceeds the daily limit."""
    from config import TRADING
    limit = TRADING.get("daily_loss_limit_usd", 0)
    if limit <= 0:
        return False
    try:
        from tracker import get_today_pnl
        return get_today_pnl() <= -limit
    except Exception:
        return False


def _auto_scan_job():
    global _last_auto_scan
    if _scan_running:
        return
    if _is_circuit_breaker_tripped():
        print("[auto-scan] circuit breaker: daily loss limit reached — scan skipped")
        return
    _last_auto_scan = datetime.now().isoformat()
    threading.Thread(target=run_scan_bg, daemon=True).start()


def _daily_learn_job():
    """
    Daily pipeline (8am UTC): resolve → learn → optimize → save report.
    Runs in background thread. Results saved to Postgres under 'daily_report'.
    """
    def _run():
        try:
            print("[daily-learn] starting resolve + learn + optimize pipeline")
            from tracker import resolve_outcomes, _load
            from learner import learn_from_outcomes
            from optimizer import run_daily_optimizer

            resolve_outcomes()
            learn_result = learn_from_outcomes()
            data = _load()
            report = run_daily_optimizer(data)
            _pg_kv_save("daily_report", report)
            print(f"[daily-learn] done — {report.get('issue_count', 0)} issues, "
                  f"{report.get('critical_count', 0)} critical")

            # Backtest optimizer: test per-city thresholds on resolved history
            try:
                from backtest_optimizer import optimize_city_thresholds
                bt_rec = optimize_city_thresholds(data)
                print(f"[daily-learn] backtest: {bt_rec.get('n_recommendations', 0)} recommendations")
            except Exception as bt_e:
                print(f"[daily-learn] backtest error: {bt_e}")
        except Exception as e:
            print(f"[daily-learn] error: {e}")

    threading.Thread(target=_run, daemon=True).start()


def _start_scheduler():
    """Start background auto-scan scheduler (Railway + local)."""
    global _scheduler
    if SCAN_INTERVAL_HOURS <= 0:
        print("Auto-scan disabled (SCAN_INTERVAL_HOURS=0)")
        return
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
    except ImportError:
        print("apscheduler not installed — auto-scan disabled. pip install apscheduler")
        return

    _scheduler = BackgroundScheduler(daemon=True)

    _scheduler.add_job(
        _auto_scan_job,
        "interval",
        hours=SCAN_INTERVAL_HOURS,
        id="auto_scan",
    )

    # Market-open cron: scan at configurable UTC time when Polymarket adds next-day markets
    try:
        from config import MARKET_OPEN_UTC
        h_str, m_str = MARKET_OPEN_UTC.split(":")
        _scheduler.add_job(
            _auto_scan_job,
            "cron",
            hour=int(h_str),
            minute=int(m_str),
            id="market_open_scan",
            timezone="UTC",
        )
        print(f"Market-open scan scheduled at {MARKET_OPEN_UTC} UTC daily")
    except Exception as e:
        print(f"[WARN] Could not schedule market-open scan: {e}")

    # Run once at startup after a short delay
    run_at = datetime.now() + timedelta(seconds=30)
    _scheduler.add_job(
        _auto_scan_job,
        "date",
        run_date=run_at,
        id="startup_scan",
    )

    # JOB 4: Daily learn + optimize pipeline (8am UTC = midnight Pacific)
    # Runs after overnight markets have resolved; updates weights, calibration, city_adjustments
    _scheduler.add_job(
        _daily_learn_job,
        "cron",
        hour=8,
        minute=0,
        id="daily_learn",
        timezone="UTC",
    )
    print("Daily learn+optimize scheduled at 08:00 UTC")

    # JOB 5: Quick position monitor every 15 min
    # Lightweight: price refresh + drift check + micro-learn — no full market scan.
    # Skips if a full scan is already running (full scan runs monitoring at completion).
    _scheduler.add_job(
        _quick_monitor_job,
        "interval",
        minutes=15,
        id="quick_monitor",
    )
    print("Quick position monitor scheduled every 15 min")

    _scheduler.start()
    print(f"Auto-scan scheduled every {SCAN_INTERVAL_HOURS}h (first run in 30s)")


@app.route("/schedule")
def schedule_status():
    """Return scheduler state for the dashboard."""
    if _scheduler is None or not _scheduler.running:
        return jsonify({
            "enabled":           False,
            "interval_hrs":      SCAN_INTERVAL_HOURS,
            "next_scan":         None,
            "last_scan":         _last_auto_scan,
            "next_monitor":      None,
            "last_monitor":      _last_monitor,
        })
    scan_job    = _scheduler.get_job("auto_scan")
    monitor_job = _scheduler.get_job("quick_monitor")
    return jsonify({
        "enabled":           True,
        "interval_hrs":      SCAN_INTERVAL_HOURS,
        "next_scan":         scan_job.next_run_time.isoformat()    if scan_job    and scan_job.next_run_time    else None,
        "last_scan":         _last_auto_scan,
        "next_monitor":      monitor_job.next_run_time.isoformat() if monitor_job and monitor_job.next_run_time else None,
        "last_monitor":      _last_monitor,
        "monitor_log":       _monitor_log[-50:],
    })


@app.route("/trade", methods=["POST"])
def trade():
    """
    Execute a real (or paper) trade for a scanned opportunity.

    Body: {
      "id":        opportunity ID from tracker,
      "token_id":  YES or NO token ID to buy,
      "side":      "buy" (default) or "sell",
      "size_usd":  USDC to deploy (default: PAPER_SIZE_USD),
      "price":     price per share (required for limit orders),
      "neg_risk":  bool (optional, default false)
    }
    """
    try:
        import trader as _trader
        from tracker import record_live_trade, PAPER_SIZE_USD
        from config import TRADING

        body     = request.get_json(silent=True) or {}
        opp_id   = body.get("id", "").strip()
        token_id = body.get("token_id", "").strip()
        side     = body.get("side", "buy").lower()
        size_usd = float(body.get("size_usd", PAPER_SIZE_USD))
        price    = body.get("price")
        neg_risk = bool(body.get("neg_risk", False))

        if not opp_id or not token_id:
            return jsonify({"error": "id and token_id are required"}), 400

        if price is None:
            return jsonify({"error": "price is required"}), 400
        price = float(price)

        if side == "buy":
            result = _trader.buy(token_id, size_usd, price, neg_risk=neg_risk)
            record_live_trade(
                opp_id=opp_id,
                order_id=result["order_id"],
                size_usd=size_usd,
                shares=result["shares"],
                token_id=token_id,
                execution_price=result.get("execution_price"),
            )
        elif side == "sell":
            result = _trader.sell(token_id, float(body.get("shares", 0)), price)
        else:
            return jsonify({"error": f"unknown side: {side}"}), 400

        return jsonify({
            "ok":        True,
            "live":      result.get("live", False),
            "order_id":  result.get("order_id"),
            "shares":    result.get("shares"),
            "exit_price": result.get("exit_price"),
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/trade/balance")
def trade_balance():
    """Return available USDC balance in the trading wallet."""
    try:
        import trader as _trader
        balance = _trader.get_balance()
        return jsonify({
            "balance_usdc": balance,
            "live_mode": os.environ.get("LIVE_MODE", "false").lower() == "true",
        })
    except Exception as e:
        return jsonify({"error": str(e), "balance_usdc": None})


@app.route("/circuit-breaker/status")
def circuit_breaker_status():
    """Return circuit breaker state — whether daily loss limit has been reached."""
    from config import TRADING
    limit = TRADING.get("daily_loss_limit_usd", 0)
    today_pnl = None
    try:
        from tracker import get_today_pnl
        today_pnl = get_today_pnl()
    except Exception:
        pass
    return jsonify({
        "enabled": limit > 0,
        "daily_loss_limit_usd": limit,
        "today_pnl_usd": today_pnl,
        "tripped": _is_circuit_breaker_tripped(),
    })


@app.route("/monitor/status")
def monitor_status():
    """Return monitor thread status and recent stop-loss events."""
    try:
        from monitor import get_status
        return jsonify(get_status())
    except Exception as e:
        return jsonify({"error": str(e), "running": False})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8888))
    # Bind to 0.0.0.0 on Railway so it's reachable externally (Railway sets RAILWAY_ENVIRONMENT_NAME)
    host = "0.0.0.0" if os.environ.get("RAILWAY_ENVIRONMENT_NAME") else "127.0.0.1"
    _start_scheduler()

    # Start position monitor (no-op if POLY_PRIVATE_KEY not set or LIVE_MODE=false)
    try:
        from monitor import start_monitor
        start_monitor()
    except Exception as e:
        print(f"[WARN] Monitor not started: {e}")

    print(f"\nPolymarket Weather Scanner Dashboard")
    print(f"Open: http://localhost:{port}")
    print(f"Auto-scan: every {SCAN_INTERVAL_HOURS}h | Capital: ${SCAN_CAPITAL}")
    print(f"Keys: R = run scan, F = focus filter\n")
    app.run(host=host, port=port, debug=False)
