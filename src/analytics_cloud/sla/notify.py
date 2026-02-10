import os
from pathlib import Path

import requests


TELEGRAM_API_BASE = "https://api.telegram.org"


def _log(msg: str) -> None:
    print(f"[sla][notify] {msg}")


def _read_report(path_str: str) -> str | None:
    p = Path(path_str)
    if not p.exists():
        _log(f"report not found: {p}")
        return None
    txt = p.read_text(encoding="utf-8", errors="replace").strip()
    if not txt:
        _log(f"report is empty: {p}")
        return None
    return txt


def _truncate_for_telegram(text: str, limit: int = 3900) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "\n\n…(truncated)"


def _send_telegram(token: str, chat_id: str, text: str) -> bool:
    url = f"{TELEGRAM_API_BASE}/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=15)
    except Exception as e:
        _log(f"telegram request failed: {type(e).__name__}: {e}")
        return False

    if resp.status_code != 200:
        _log(f"telegram HTTP {resp.status_code}: {resp.text[:500]}")
        return False

    try:
        data = resp.json()
    except Exception:
        _log(f"telegram response not JSON: {resp.text[:500]}")
        return False

    if not data.get("ok"):
        _log(f"telegram API error: {str(data)[:500]}")
        return False

    return True


def main() -> None:
    report_path = os.environ.get("ALERT_REPORT_PATH", "data/alert_report.txt")
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        _log("telegram not configured (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID). Skipping.")
        return

    report = _read_report(report_path)
    if report is None:
        _log("no report to send. Skipping.")
        return

    msg = _truncate_for_telegram(report)

    sent = _send_telegram(token=token, chat_id=chat_id, text=msg)
    if sent:
        _log("telegram sent OK")
    else:
        _log("telegram send FAILED (non-fatal)")


if __name__ == "__main__":
    main()
