import os
import sys
from datetime import datetime, timezone
from typing import Any

import requests
import smtplib
from email.message import EmailMessage


SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

# Gmail SMTP（GitHub Secrets: SMTP_HOST/PORT/USER/PASS）
SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587") or "587")  # ✅ default 587
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")

# GitHub Secrets（添付の名前に合わせる）
REPORT_TO = os.environ.get("REPORT_TO", "")     # 例: a@x.com,b@y.com
REPORT_FROM = os.environ.get("REPORT_FROM", "") # 例: you@gmail.com


def die(msg: str, code: int = 1):
    print(msg)
    sys.exit(code)


def supabase_get(path: str):
    if not SUPABASE_URL:
        raise RuntimeError("SUPABASE_URL is empty")
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    r = requests.get(url, headers=headers, timeout=30)
    if not r.ok:
        raise RuntimeError(f"Supabase GET failed: {r.status_code} {r.text}")
    return r.json()


def fetch_thresholds() -> dict[str, Any]:
    rows = supabase_get("ocr_alert_thresholds?select=*&id=eq.1")
    if not rows:
        return {
            "window_minutes": 60,
            "max_fail_count": 5,
            "max_fail_rate": 0.20,
            "quality_score_threshold": 40,
            "max_low_quality_rate": 0.30,
            "unknown_ratio_threshold": 0.50,
            "max_high_unknown_rate": 0.30,
        }
    return rows[0]


def fetch_window_summary() -> dict[str, Any]:
    rows = supabase_get("v_ocr_alert_window?select=*")
    if not rows:
        raise RuntimeError("v_ocr_alert_window returned empty")
    return rows[0]


def fetch_error_stage_rank(limit: int = 8) -> list[dict[str, Any]]:
    rows = supabase_get(
        f"v_ocr_alert_error_stage_rank?select=*&order=cnt.desc&limit={limit}"
    )
    return rows or []


def build_subject(is_alert: bool) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return ("[ALERT] OCR監視 異常検知" if is_alert else "[OK] OCR監視 正常") + f" - {ts}"


def build_body(th: dict[str, Any], w: dict[str, Any], rank: list[dict[str, Any]], is_alert: bool, reasons: list[str]) -> str:
    lines: list[str] = []
    lines.append(f"判定: {'ALERT' if is_alert else 'OK'}")
    lines.append(f"集計ウィンドウ: {w.get('window_minutes')} 分")
    lines.append("")

    lines.append("■ サマリ")
    lines.append(f"- total_count: {w.get('total_count')}")
    lines.append(f"- fail_count: {w.get('fail_count')} / fail_rate: {w.get('fail_rate')}")
    lines.append(f"- avg_quality_score: {w.get('avg_quality_score')}")
    lines.append(f"- low_quality_count: {w.get('low_quality_count')} / low_quality_rate: {w.get('low_quality_rate')}")
    lines.append(f"- high_unknown_count: {w.get('high_unknown_count')} / high_unknown_rate: {w.get('high_unknown_rate')}")
    lines.append("")

    lines.append("■ 閾値")
    lines.append(f"- max_fail_count: {th.get('max_fail_count')}")
    lines.append(f"- max_fail_rate: {th.get('max_fail_rate')}")
    lines.append(f"- quality_score_threshold: {th.get('quality_score_threshold')}")
    lines.append(f"- max_low_quality_rate: {th.get('max_low_quality_rate')}")
    lines.append(f"- unknown_ratio_threshold: {th.get('unknown_ratio_threshold')}")
    lines.append(f"- max_high_unknown_rate: {th.get('max_high_unknown_rate')}")
    lines.append("")

    if reasons:
        lines.append("■ 超過した条件")
        for r in reasons:
            lines.append(f"- {r}")
        lines.append("")

    lines.append("■ error_stage（失敗のみ上位）")
    if rank:
        for row in rank:
            lines.append(f"- {row.get('error_stage')}: {row.get('cnt')}")
    else:
        lines.append("- (なし)")
    lines.append("")
    return "\n".join(lines)


def _parse_recipients(to_raw: str) -> list[str]:
    parts = [p.strip() for p in to_raw.replace(";", ",").split(",")]
    return [p for p in parts if p]


def send_mail_smtp(subject: str, body: str):
    if not REPORT_TO or not REPORT_FROM:
        die("REPORT_TO / REPORT_FROM が未設定です（GitHub Secrets を確認）")

    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS):
        die("SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_PASS が未設定です（GitHub Secrets を確認）")

    tos = _parse_recipients(REPORT_TO)
    if not tos:
        die("REPORT_TO の形式が不正です（宛先が空）")

    msg = EmailMessage()
    msg["From"] = REPORT_FROM
    msg["To"] = ", ".join(tos)
    msg["Subject"] = subject
    msg.set_content(body)

    # Gmail: STARTTLS(587) 想定
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.set_debuglevel(1)  # ✅ ActionsログにSMTPの会話が出る（原因特定しやすい）
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)


def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        die("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY が未設定です")

    th = fetch_thresholds()
    w = fetch_window_summary()
    rank = fetch_error_stage_rank()

    reasons: list[str] = []
    fail_count = int(w.get("fail_count") or 0)
    total_count = int(w.get("total_count") or 0)
    fail_rate = float(w.get("fail_rate") or 0)
    low_quality_rate = float(w.get("low_quality_rate") or 0)
    high_unknown_rate = float(w.get("high_unknown_rate") or 0)

    if fail_count >= int(th["max_fail_count"]):
        reasons.append(f"fail_count {fail_count} >= {th['max_fail_count']}")
    if total_count >= 10 and fail_rate >= float(th["max_fail_rate"]):
        reasons.append(f"fail_rate {fail_rate} >= {th['max_fail_rate']} (total_count>=10)")
    if total_count >= 10 and low_quality_rate >= float(th["max_low_quality_rate"]):
        reasons.append(f"low_quality_rate {low_quality_rate} >= {th['max_low_quality_rate']} (total_count>=10)")
    if total_count >= 10 and high_unknown_rate >= float(th["max_high_unknown_rate"]):
        reasons.append(f"high_unknown_rate {high_unknown_rate} >= {th['max_high_unknown_rate']} (total_count>=10)")

    is_alert = len(reasons) > 0
    subject = build_subject(is_alert)
    body = build_body(th, w, rank, is_alert, reasons)

    if is_alert:
        try:
            send_mail_smtp(subject, body)
            print("ALERT sent")
        except Exception as e:
            # ✅ ここが出れば「届かない」の原因がログに出ます
            print(f"[MAIL ERROR] {type(e).__name__}: {e}")
            raise
    else:
        print("OK (no alert)")


if __name__ == "__main__":
    main()
