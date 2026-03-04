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


TAKEN_FILE = os.path.join(DATA_DIR, "taken.json")


def load_taken() -> set:
    if not os.path.exists(TAKEN_FILE):
        return set()
    with open(TAKEN_FILE) as f:
        return set(json.load(f).get("taken", []))


def save_taken(taken_set: set):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(TAKEN_FILE, "w") as f:
        json.dump({"taken": list(taken_set), "updated": datetime.now().isoformat()}, f)




@app.route("/")
def index():
    return render_template("index.html")


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
    try:
        from tracker import get_summary
        return jsonify(get_summary())
    except Exception as e:
        return jsonify({"error": str(e), "total": 0, "resolved": 0,
                        "wins": 0, "losses": 0, "pending": 0, "recent": []})


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
