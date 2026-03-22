"""
Slack/email notifications for high-confidence opportunities.

Set environment variables to enable:
  SLACK_WEBHOOK_URL      — Slack incoming webhook URL
  NOTIFY_MIN_RETURN_PCT  — Minimum return % to notify on (default: 20)
"""

import os
import requests
from datetime import datetime, timedelta

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


def send_daily_summary(data: dict, learn_result: dict, report: dict) -> bool:
    """
    Send a daily Slack summary covering:
      - What resolved yesterday (wins/losses, P&L)
      - New positions entered in the last 24 hours
      - Currently open portfolio
      - What the system learned
      - Issues flagged and changes recommended

    Called at the end of the daily 8am UTC learn pipeline.
    Returns True if the message was sent.
    """
    if not SLACK_WEBHOOK_URL:
        return False

    now = datetime.utcnow()
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    today     = now.strftime("%Y-%m-%d")
    ts        = now.strftime("%Y-%m-%d")

    opps = data.get("opportunities", [])

    # ── Resolved in the last ~24h ─────────────────────────────────────────────
    def _recent_exit(opp):
        exit_at = opp.get("exit_at") or opp.get("resolution_date") or ""
        return exit_at.startswith(yesterday) or exit_at.startswith(today)

    resolved = [o for o in opps if o.get("outcome") is not None and _recent_exit(o)]
    wins   = [o for o in resolved if o.get("outcome") == "win"]
    losses = [o for o in resolved if o.get("outcome") == "loss"]
    total_pnl = sum(float(o.get("paper_pnl_usd") or 0) for o in resolved)
    win_rate  = len(wins) / len(resolved) if resolved else None

    # ── New positions opened in the last 24h ──────────────────────────────────
    new_positions = [
        o for o in opps
        if (o.get("live_at") or "").startswith(yesterday)
        or (o.get("live_at") or "").startswith(today)
        or (o.get("scanned_at") or "").startswith(yesterday)
        or (o.get("scanned_at") or "").startswith(today)
        if o.get("is_live") or o.get("outcome") is None
    ]
    # Deduplicate by id — live_at filter may overlap with open positions
    new_positions = list({o["id"]: o for o in new_positions}.values())
    # Only show ones actually entered (have live_at or is_live)
    entered = [o for o in new_positions if o.get("live_at") or o.get("is_live")]

    # ── Open portfolio ────────────────────────────────────────────────────────
    open_positions = [o for o in opps if o.get("outcome") is None]
    total_unrealized = sum(float(o.get("unrealized_pnl_usd") or 0) for o in open_positions)

    # ── Build message ─────────────────────────────────────────────────────────
    pnl_sign   = "+" if total_pnl >= 0 else ""
    unr_sign   = "+" if total_unrealized >= 0 else ""

    lines = [f":bar_chart: *Polymarket Bot — Daily Summary* | {ts}"]
    lines.append("")

    # Results section
    if resolved:
        wr_str = f" | Win rate: {win_rate:.0%}" if win_rate is not None else ""
        lines.append(
            f"*Yesterday's Results* — {len(resolved)} resolved | "
            f"{len(wins)}W / {len(losses)}L | "
            f"P&L: *{pnl_sign}${total_pnl:.2f}*{wr_str}"
        )
        for o in resolved[:8]:
            icon     = ":white_check_mark:" if o.get("outcome") == "win" else ":x:"
            pnl_usd  = float(o.get("paper_pnl_usd") or 0)
            pnl_str  = f"{'+' if pnl_usd >= 0 else ''}${pnl_usd:.2f}"
            city     = o.get("city", "?")
            bracket  = o.get("bracket", "?")[:40]
            reason   = o.get("exit_reason") or o.get("outcome") or ""
            actual   = o.get("actual_temp")
            act_str  = f" | actual {actual:.0f}°" if actual is not None else ""
            reason_label = f" ({reason})" if reason not in ("win", "loss", None, "") else ""
            lines.append(
                f"  {icon} `{city}` {bracket} — *{o.get('outcome', '?').upper()}* "
                f"{pnl_str}{act_str}{reason_label}"
            )
        if len(resolved) > 8:
            lines.append(f"  _...and {len(resolved) - 8} more_")
    else:
        lines.append("*Yesterday's Results* — No positions resolved in the last 24h")

    lines.append("")

    # New positions
    if entered:
        lines.append(f"*New Positions Entered* — {len(entered)} bet{'s' if len(entered) != 1 else ''}")
        for o in entered[:6]:
            ret_str  = f"{o.get('return_pct', 0):.1f}%" if o.get("return_pct") else "?"
            price    = o.get("entry_price") or o.get("no_price") or 0
            size_str = f"${float(o.get('live_size_usd') or o.get('paper_size_usd') or 0):.0f}"
            dist     = o.get("distance")
            dist_str = f" | {dist:.0f}° gap" if dist is not None else ""
            lines.append(
                f"  • `{o.get('city','?')}` {o.get('bracket','?')[:40]} | "
                f"{o.get('type','?').upper()} @ {float(price):.3f} | "
                f"*{ret_str}* return | {size_str}{dist_str}"
            )
        if len(entered) > 6:
            lines.append(f"  _...and {len(entered) - 6} more_")
    else:
        lines.append("*New Positions Entered* — None in the last 24h")

    lines.append("")

    # Open portfolio
    if open_positions:
        lines.append(
            f"*Open Portfolio* — {len(open_positions)} position{'s' if len(open_positions) != 1 else ''} | "
            f"Unrealized P&L: *{unr_sign}${total_unrealized:.2f}*"
        )
    else:
        lines.append("*Open Portfolio* — No open positions")

    lines.append("")

    # What we learned
    learned      = learn_result.get("learned", 0)
    temps_found  = learn_result.get("temps_fetched", 0)
    w_updated    = learn_result.get("weights_updated", False)
    c_updated    = learn_result.get("calib_updated", False)
    city_adj     = learn_result.get("city_adjustments", {})

    learn_parts = [f"Processed *{learned}* outcome{'s' if learned != 1 else ''}"]
    if temps_found:
        learn_parts.append(f"fetched {temps_found} actual temps")
    if w_updated:
        learn_parts.append("forecast weights updated")
    if c_updated:
        learn_parts.append("calibration sigma updated")
    if city_adj:
        learn_parts.append(f"city distance bonuses: {', '.join(city_adj.keys())}")

    lines.append("*What We Learned* — " + " | ".join(learn_parts))

    # Overall stats from report
    overall = report.get("overall", {})
    if overall.get("n", 0) > 0:
        wr_14 = overall.get("win_rate")
        roi_14 = overall.get("roi_pct")
        n_14   = overall.get("n", 0)
        wr_str  = f"{wr_14:.0%}" if wr_14 is not None else "n/a"
        roi_str = f"{'+' if (roi_14 or 0) >= 0 else ''}{roi_14:.1f}%" if roi_14 is not None else "n/a"
        lines.append(f"  _14-day: {n_14} resolved | win rate {wr_str} | ROI {roi_str}_")

    lines.append("")

    # Issues → what changes are recommended
    issues = report.get("issues", [])
    critical = [i for i in issues if i.get("severity") == "critical"]
    warnings = [i for i in issues if i.get("severity") == "warning"]

    if issues:
        sev_icons = {"critical": ":red_circle:", "warning": ":large_yellow_circle:", "info": ":large_blue_circle:"}
        lines.append(f"*Issues & Recommended Changes* — {len(issues)} flagged ({len(critical)} critical)")
        for iss in issues[:6]:
            icon = sev_icons.get(iss.get("severity", "info"), ":white_circle:")
            lines.append(f"  {icon} {iss['message']}")
        if len(issues) > 6:
            lines.append(f"  _...and {len(issues) - 6} more issues_")
    else:
        lines.append("*Issues & Recommended Changes* — :white_check_mark: No issues flagged — strategy is on track")

    return _send_slack("\n".join(lines))


# ─── Desktop markdown summary ─────────────────────────────────────────────────

DESKTOP_SUMMARY_DIR = os.path.expanduser("~/Desktop")


def write_daily_summary_md(data: dict, learn_result: dict, report: dict,
                           auto_applied: dict = None) -> str:
    """
    Write a daily summary .md file to ~/Desktop/polymarket_daily_YYYY-MM-DD.md.
    Returns the file path written.
    """
    from datetime import date as _date
    today_str = _date.today().isoformat()
    filepath = os.path.join(DESKTOP_SUMMARY_DIR, f"polymarket_daily_{today_str}.md")

    opps = data.get("opportunities", [])
    now = datetime.utcnow()
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    today_d   = now.strftime("%Y-%m-%d")

    # Resolved
    def _recent_exit(opp):
        exit_at = opp.get("exit_at") or opp.get("resolution_date") or ""
        return exit_at.startswith(yesterday) or exit_at.startswith(today_d)

    resolved = [o for o in opps if o.get("outcome") is not None and _recent_exit(o)]
    wins   = [o for o in resolved if o.get("outcome") == "win"]
    losses = [o for o in resolved if o.get("outcome") == "loss"]
    total_pnl = sum(float(o.get("paper_pnl_usd") or 0) for o in resolved)

    # Open
    open_pos = [o for o in opps if o.get("is_live") and o.get("outcome") is None]
    open_cost = sum(float(o.get("live_size_usd") or o.get("paper_size_usd") or 0) for o in open_pos)

    # Overall stats
    overall = report.get("overall", {})
    no_stats = report.get("by_type", {}).get("no", {})
    yes_stats = report.get("by_type", {}).get("yes", {})

    lines = [
        f"# Polymarket Bot Daily Summary - {today_str}",
        "",
        "## Yesterday's Results",
        "",
    ]

    if resolved:
        wr = len(wins) / len(resolved) if resolved else 0
        pnl_sign = "+" if total_pnl >= 0 else ""
        lines.append(f"**{len(resolved)} resolved** | {len(wins)}W / {len(losses)}L | "
                     f"Win rate: {wr:.0%} | P&L: **{pnl_sign}${total_pnl:.2f}**")
        lines.append("")
        lines.append("| City | Bracket | Type | Result | P&L | Actual Temp |")
        lines.append("|------|---------|------|--------|-----|-------------|")
        for o in resolved:
            result = o.get("outcome", "?").upper()
            pnl_usd = float(o.get("paper_pnl_usd") or 0)
            actual = o.get("actual_temp")
            act_str = f"{actual:.0f}" if actual is not None else "?"
            dist = o.get("distance")
            dist_str = f" ({dist:.0f} gap)" if dist is not None else ""
            lines.append(
                f"| {o.get('city','?')} | {o.get('bracket','?')[:30]} | "
                f"{o.get('type','?').upper()} | {result} | "
                f"{'+'if pnl_usd>=0 else ''}${pnl_usd:.2f} | {act_str}{dist_str} |"
            )
    else:
        lines.append("No positions resolved in the last 24h.")

    lines.extend(["", "## Open Positions", ""])

    if open_pos:
        lines.append(f"**{len(open_pos)} open** | Total deployed: ${open_cost:.0f}")
        lines.append("")
        lines.append("| City | Bracket | Type | Entry | Shares | Size | Token ID |")
        lines.append("|------|---------|------|-------|--------|------|----------|")
        for o in open_pos:
            entry = float(o.get("execution_price") or o.get("entry_price") or 0)
            lines.append(
                f"| {o.get('city','?')} | {o.get('bracket','?')[:30]} | "
                f"{o.get('type','?').upper()} | {entry:.3f} | "
                f"{o.get('shares','?')} | ${float(o.get('live_size_usd') or 0):.0f} | "
                f"{(o.get('token_id') or 'MISSING')[:16]} |"
            )
    else:
        lines.append("No open positions.")

    # 14-day stats
    lines.extend(["", "## 14-Day Performance", ""])
    if overall.get("n", 0) > 0:
        wr_14 = overall.get("win_rate")
        roi_14 = overall.get("roi_pct")
        lines.append(f"- **Total resolved:** {overall['n']}")
        lines.append(f"- **Win rate:** {wr_14:.0%}" if wr_14 is not None else "- **Win rate:** n/a")
        lines.append(f"- **ROI:** {'+' if (roi_14 or 0) >= 0 else ''}{roi_14:.1f}%" if roi_14 is not None else "- **ROI:** n/a")
        lines.append(f"- **Total P&L:** ${overall.get('total_pnl', 0):.2f}")
        lines.append(f"- **Total staked:** ${overall.get('total_staked', 0):.2f}")
    else:
        lines.append("Not enough data yet.")

    # NO stats
    if no_stats.get("n", 0) > 0:
        lines.extend(["", "### NO Bets"])
        lines.append(f"- {no_stats['n']} resolved, {no_stats.get('win_rate',0):.0%} win rate, "
                     f"${no_stats.get('total_pnl',0):.2f} P&L")

    # YES stats
    if yes_stats.get("n", 0) > 0:
        lines.extend(["", "### YES Bets"])
        lines.append(f"- {yes_stats['n']} resolved, {yes_stats.get('win_rate',0):.0%} win rate, "
                     f"${yes_stats.get('total_pnl',0):.2f} P&L")

    # Distance buckets
    by_distance = report.get("by_distance", {})
    if by_distance:
        lines.extend(["", "### Win Rate by Distance Bucket (NO bets)"])
        lines.append("| Distance | N | Win Rate | P&L |")
        lines.append("|----------|---|----------|-----|")
        for bucket, stats in sorted(by_distance.items()):
            wr = f"{stats['win_rate']:.0%}" if stats.get("win_rate") is not None else "n/a"
            lines.append(f"| {bucket} | {stats['n']} | {wr} | ${stats.get('total_pnl',0):.2f} |")

    # City stats
    by_city = report.get("by_city_no", {})
    if by_city:
        lines.extend(["", "### Win Rate by City (NO bets)"])
        lines.append("| City | N | Win Rate | WU Error | P&L |")
        lines.append("|------|---|----------|----------|-----|")
        for city, stats in sorted(by_city.items(), key=lambda x: x[1].get("win_rate", 1)):
            wr = f"{stats['win_rate']:.0%}" if stats.get("win_rate") is not None else "n/a"
            wu = f"{stats.get('avg_wu_error_f', 0):.1f}" if stats.get("avg_wu_error_f") else "?"
            lines.append(f"| {city} | {stats['n']} | {wr} | {wu} | ${stats.get('total_pnl',0):.2f} |")

    # Learning
    lines.extend(["", "## What the System Learned", ""])
    learned = learn_result.get("learned", 0)
    lines.append(f"- Processed {learned} outcome(s)")
    if learn_result.get("weights_updated"):
        lines.append("- Forecast weights updated")
    if learn_result.get("calib_updated"):
        lines.append("- Calibration sigma updated")
    city_adj = learn_result.get("city_adjustments", {})
    if city_adj:
        lines.append(f"- City distance bonuses: {', '.join(city_adj.keys())}")

    # Issues
    issues = report.get("issues", [])
    if issues:
        lines.extend(["", "## Issues Flagged", ""])
        for iss in issues:
            icon = {"critical": "!!!", "warning": "!!", "info": "i"}.get(iss.get("severity"), "?")
            lines.append(f"- **[{icon}]** {iss['message']}")

    # Auto-applied changes
    if auto_applied and auto_applied.get("changes"):
        lines.extend(["", "## Auto-Applied Strategy Changes", ""])
        for change in auto_applied["changes"]:
            lines.append(f"- {change}")
    elif auto_applied:
        lines.extend(["", "## Auto-Applied Strategy Changes", "", "No changes needed."])

    lines.extend(["", "---", f"*Generated {now.strftime('%Y-%m-%d %H:%M UTC')}*", ""])

    try:
        with open(filepath, "w") as f:
            f.write("\n".join(lines))
        print(f"[notify] daily summary written to {filepath}")
    except Exception as e:
        print(f"[notify] failed to write summary: {e}")

    return filepath
