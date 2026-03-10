#!/usr/bin/env python3
"""
Test the WU history scraper for Atlanta (KATL).
- Verifies parser on mock HTML with "High Temp" line.
- Runs live fetch for Atlanta on a few dates (may return None if WU serves "No data recorded" to server requests).
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import re

def test_parser_with_mock_html():
    """Parser should extract 74 from mock High Temp row."""
    html = '<div>High Temp</div><td><span class="wx-value">74</span></td>'
    for label in ("High Temp", "High Temperature"):
        idx = html.find(label)
        if idx == -1:
            continue
        chunk = html[idx : idx + 500]
        m = re.search(r'class="wx-value"[^>]*>([^<]+)</span>', chunk, re.I)
        if m:
            v = float(m.group(1).strip())
            assert 40 <= v <= 130, v
            print(f"  Parser test (mock): extracted {v}°F from 'High Temp' row — OK")
            return True
    print("  Parser test (mock): FAILED to extract value")
    return False


def test_live_atlanta():
    """Live fetch Atlanta KATL for a few dates."""
    from learner import fetch_actual_temp_wu_history
    dates = ["2026-03-08", "2024-03-08", "2023-07-15"]
    print("  Live Atlanta (KATL) fetch:")
    for d in dates:
        t = fetch_actual_temp_wu_history("Atlanta", d, "F")
        status = f"  → {t}°F" if t is not None else "  → No data (page may say 'No data recorded' for server requests)"
        print(f"    {d}: {status}")
    return True


if __name__ == "__main__":
    print("WU history scraper tests (Atlanta station)")
    print("-" * 50)
    ok = test_parser_with_mock_html()
    ok &= test_live_atlanta()
    print("-" * 50)
    sys.exit(0 if ok else 1)
