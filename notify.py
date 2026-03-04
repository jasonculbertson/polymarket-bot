"""
Slack/email notifications for high-confidence opportunities.

Set environment variables to enable:
  SLACK_WEBHOOK_URL      — Slack incoming webhook URL
  NOTIFY_MIN_RETURN_PCT  — Minimum return % to notify on (default: 20)
"""

import os
import requests
from datetime import datetime

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
NOTIFY_MIN_RETURN_PCT = float(os.environ.get("NOTIFY_MIN_RETURN_PCT", "20"))


def _send_slack(message: str) -> bool:
    if not SLACK_WEBHOOK_URL:
        return False
    try:
        r = requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": message, "unfurl_links": False},
            timeout=8,
        )
        return r.status_code == 200
    except Exception:
        return False


def notify_opportunities(yes_clusters, no_opps, scan_time: str = None) -> int:
    """
    Send Slack notification for high-confidence opportunities above the return threshold.
    Returns the number of qualifying opportunities found (0 if no webhook configured).
    """
    if not SLACK_WEBHOOK_URL:
        return 0

    min_ret = NOTIFY_MIN_RETURN_PCT
    ts = scan_time or datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    high_yes = [
        c for c in yes_clusters
        if c.forecast_confidence == "high" and c.return_pct >= min_ret
    ]
    high_no = [
        o for o in no_opps
        if o.forecast_confidence == "high" and o.return_pct >= min_ret
    ]

    if not high_yes and not high_no:
        return 0

    total = len(high_yes) + len(high_no)
    lines = [
        f":chart_with_upwards_trend: *Polymarket Scanner* — {ts}",
        f"Found *{total}* high-confidence opportunities ≥{min_ret:.0f}% return\n",
    ]

    if high_yes:
        lines.append("*YES Clusters:*")
        for c in high_yes[:6]:
            u = c.temp_unit
            labels = " + ".join(b.group_title for b in c.brackets)
            lines.append(
                f"  • `{c.city}` {c.date} | {labels} | "
                f"*{c.return_pct:.1f}%* return | "
                f"Forecast: {c.forecast_temp:.1f}°{u} | "
                f"Cost: ${c.total_cost:.0f}"
            )
        if len(high_yes) > 6:
            lines.append(f"  _...and {len(high_yes) - 6} more YES clusters_")

    if high_no:
        lines.append("\n*NO Bets:*")
        for o in high_no[:6]:
            u = o.temp_unit
            lines.append(
                f"  • `{o.city}` {o.date} | {o.group_title} | "
                f"NO @ {o.no_price:.3f} | "
                f"*{o.return_pct:.1f}%* return | "
                f"Dist: {o.distance_f:.0f}°{u} from forecast"
            )
        if len(high_no) > 6:
            lines.append(f"  _...and {len(high_no) - 6} more NO bets_")

    _send_slack("\n".join(lines))
    return total
