# send_ocr_monthly_report.py

import os
from datetime import datetime, date, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# メール設定（Gmail想定・App Password を使う）
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")           # 送信元 Gmail
SMTP_PASS = os.getenv("SMTP_PASS")           # アプリパスワード
REPORT_FROM = os.getenv("REPORT_FROM", SMTP_USER)
REPORT_TO = os.getenv("REPORT_TO")           # 管理者のメール

if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
    raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY が未設定です")

if not (SMTP_USER and SMTP_PASS and REPORT_TO):
    raise RuntimeError("SMTP / REPORT の設定が未完了です")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def get_last_month_range() -> tuple[date, date]:
    """先月の月初・月末を返す"""
    today = date.today()
    first_this_month = today.replace(day=1)
    # 前月末 = 今月1日の前日
    last_month_end = first_this_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    return last_month_start, last_month_end

def fetch_monthly_summary(start: date, end: date):
    """v_ocr_scan_monthly_summary から先月分を取得"""
    resp = (
        supabase.table("v_ocr_scan_monthly_summary")
        .select("*")
        .gte("month", start.isoformat())
        .lte("month", end.isoformat())
        .execute()
    )
    rows = resp.data or []
    return rows[0] if rows else None


def fetch_high_risk_ranking(start: date, end: date, limit: int = 5):
    """高リスク添加物ランキング（期間内合算）"""
    resp = (
        supabase.table("v_ocr_high_risk_additives_daily")
        .select("code, name_ja, category, risk_level, detect_count, scan_date")
        .gte("scan_date", start.isoformat())
        .lte("scan_date", end.isoformat())
        .execute()
    )
    rows = resp.data or []
    agg = {}
    for r in rows:
        key = (r.get("code"), r.get("name_ja"), r.get("category"), r.get("risk_level"))
        agg.setdefault(key, 0)
        agg[key] += r.get("detect_count", 0)

    result = []
    for (code, name_ja, category, risk_level), cnt in agg.items():
        result.append(
            {
                "code": code,
                "name_ja": name_ja,
                "category": category,
                "risk_level": risk_level,
                "total_count": cnt,
            }
        )

    result.sort(key=lambda x: x["total_count"], reverse=True)
    return result[:limit]


def build_report_body(month_row, high_risk_list, start: date, end: date) -> str:
    title_range = f"{start.strftime('%Y-%m-%d')} 〜 {end.strftime('%Y-%m-%d')}"

    if not month_row:
        return f"""【原材料チェック 月次レポート】

対象期間: {title_range}

この期間のスキャンデータはありませんでした。
"""

    body_lines = [
        "【原材料チェック 月次レポート】",
        "",
        f"対象期間: {title_range}",
        "",
        "■ 基本指標",
        f"- スキャン数            : {month_row['scan_count']}",
        f"- エラー数（いずれか） : {month_row['any_fail_count']}",
        f"- エラー率（％）        : {month_row['error_rate_pct']}%",
        f"- 平均レスポンス時間    : {month_row['avg_duration_ms']} ms",
        f"- 利用ユーザー数        : {month_row['user_count']}",
        "",
        "■ 高リスク添加物ランキング（検出回数 上位）",
    ]

    if high_risk_list:
        for i, item in enumerate(high_risk_list, start=1):
            name = item["name_ja"] or "(名称不明)"
            cat = item["category"] or "-"
            risk = item["risk_level"]
            cnt = item["total_count"]
            code = item["code"] or "-"
            body_lines.append(
                f"{i}. {name}（code: {code}, カテゴリ: {cat}, リスクLv: {risk}）… {cnt}回"
            )
    else:
        body_lines.append("（この期間に高リスク添加物は検出されませんでした）")

    body_lines.append("")
    body_lines.append("※ このメールは自動送信されています。")

    return "\n".join(body_lines)


def send_mail(subject: str, body: str):
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = REPORT_FROM
    msg["To"] = REPORT_TO

    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)


def main():
    start, end = get_last_month_range()
    month_row = fetch_monthly_summary(start, end)
    high_risk_list = fetch_high_risk_ranking(start, end, limit=5)

    body = build_report_body(month_row, high_risk_list, start, end)
    subject = f"原材料チェック 月次レポート（{start.strftime('%Y-%m')}）"

    send_mail(subject, body)
    print("Monthly report sent.")


if __name__ == "__main__":
    main()
