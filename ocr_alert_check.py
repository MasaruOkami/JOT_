# ocr_alert_check.py
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
SMTP_PORT = int(os.environ.get("SMTP_PORT", "0") or 0)
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")

# 宛先
REPORT_TO = os.environ.get("REPORT_TO", "")       # 例: a@x.com,b@y.com
REPORT_FROM = os.environ.get("REPORT_FROM", "")   # 例: you@gmail.com

# OKでも送る（デバッグ用）
FORCE_SEND_OK = os.environ.get("FORCE_SEND_OK", "").lower() == "true"


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

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)


def _pct(x: Any) -> str:
    # 0.123 -> "12.3%"
    try:
        if x is None:
            return "未算出"
        v = float(x)
        return f"{v*100:.1f}%"
    except Exception:
        return "未算出"


def _num(x: Any) -> str:
    if x is None:
        return "未算出"
    return str(x)


def build_subject(is_alert: bool, window_minutes: int) -> str:
    # 件名はわかりやすく固定
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    prefix = "【重要】OCR監視で問題が検出されました" if is_alert else "OCR監視結果のお知らせ（正常）"
    return f"{prefix} - 直近{window_minutes}分 - {ts}"


def build_body_ok(th: dict[str, Any], w: dict[str, Any]) -> str:
    window_minutes = int(w.get("window_minutes") or th.get("window_minutes") or 60)

    total_count = int(w.get("total_count") or 0)
    fail_count = int(w.get("fail_count") or 0)

    # 0件なら「未算出」を明示
    no_runs_note = ""
    if total_count == 0:
        no_runs_note = (
            "※ この時間帯は OCR の実行自体が無かったため、\n"
            "　すべての指標が「0件」または「未算出」になっています。\n"
        )

    body = f"""OCR監視結果のお知らせ（正常）

直近 {window_minutes} 分間の OCR 処理について確認しましたが、
問題は検出されませんでした。

────────────────────
■ 全体状況
────────────────────
・今回チェックした件数：{total_count} 件
・処理エラー：{fail_count} 件
・判定結果：正常（OK）

{no_runs_note}────────────────────
■ 品質・異常のチェック結果
────────────────────
・画像や文字認識の品質に問題はありませんでした
・原因不明の添加物（未登録ワード）は検出されていません
・エラーが発生した処理ステージはありません

────────────────────
■ 現在の判定ルール（参考）
────────────────────
以下の条件を超えた場合に「異常」として通知されます。

・エラー件数が {int(th.get("max_fail_count") or 5)} 件以上
・エラー率が {int(float(th.get("max_fail_rate") or 0.2) * 100)}% 以上
・品質スコアが低い画像が {int(float(th.get("max_low_quality_rate") or 0.3) * 100)}% 以上
・未登録の添加物が多く検出された場合

────────────────────
■ 今後の運用について
────────────────────
・このまま自動監視を継続します
・問題が発生した場合のみ、改めて通知されます

ご確認ありがとうございました。
"""
    return body


def build_body_alert(th: dict[str, Any], w: dict[str, Any], rank: list[dict[str, Any]], reasons: list[str]) -> str:
    window_minutes = int(w.get("window_minutes") or th.get("window_minutes") or 60)

    total_count = int(w.get("total_count") or 0)
    fail_count = int(w.get("fail_count") or 0)
    fail_rate = w.get("fail_rate")
    low_quality_rate = w.get("low_quality_rate")
    high_unknown_rate = w.get("high_unknown_rate")

    # エラーステージ一覧（上位）
    stage_lines = []
    for row in rank or []:
        stage = row.get("error_stage") or "(unknown)"
        cnt = row.get("cnt") or 0
        stage_lines.append(f"・{stage}：{cnt} 件")
    if not stage_lines:
        stage_lines = ["・（なし）"]

    # 検出条件（わかる日本語）
    reason_lines = "\n".join([f"・{r}" for r in reasons]) if reasons else "・（詳細条件はログを参照）"

    body = f"""【重要】OCR監視で問題が検出されました

直近 {window_minutes} 分間の OCR 処理において、
いくつか注意が必要な状態が確認されました。

────────────────────
■ 検出された問題
────────────────────
・処理エラーが多く発生しています
・または、画像品質や未登録ワードが基準を超えています

（詳細は下記をご確認ください）

────────────────────
■ 詳細状況
────────────────────
・チェック件数：{total_count} 件
・エラー件数：{fail_count} 件
・エラー率：{_pct(fail_rate)}
・品質が低い画像の割合：{_pct(low_quality_rate)}
・未登録の添加物が多く検出された割合：{_pct(high_unknown_rate)}

────────────────────
■ 今回「異常」と判定した理由
────────────────────
{reason_lines}

────────────────────
■ エラーが多い処理工程（上位）
────────────────────
{chr(10).join(stage_lines)}

────────────────────
■ 推奨対応
────────────────────
・最近アップロードされた画像を確認してください
・撮影条件（明るさ・ピント・角度）を見直してください
・未登録の添加物があれば辞書に追加してください

早めの対応をおすすめします。
"""
    return body


def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        die("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY が未設定です")

    th = fetch_thresholds()
    w = fetch_window_summary()
    rank = fetch_error_stage_rank()

    fail_count = int(w.get("fail_count") or 0)
    total_count = int(w.get("total_count") or 0)
    fail_rate = float(w.get("fail_rate") or 0)
    low_quality_rate = float(w.get("low_quality_rate") or 0)
    high_unknown_rate = float(w.get("high_unknown_rate") or 0)

    reasons: list[str] = []
    max_fail_count = int(th.get("max_fail_count") or 5)
    max_fail_rate = float(th.get("max_fail_rate") or 0.20)
    max_low_quality_rate = float(th.get("max_low_quality_rate") or 0.30)
    max_high_unknown_rate = float(th.get("max_high_unknown_rate") or 0.30)

    if fail_count >= max_fail_count:
        reasons.append(f"エラー件数が多い（{fail_count} 件 / 閾値 {max_fail_count} 件）")

    # total_count が小さい時のノイズ対策（あなたの方針を維持）
    if total_count >= 10 and fail_rate >= max_fail_rate:
        reasons.append(f"エラー率が高い（{fail_rate*100:.1f}% / 閾値 {max_fail_rate*100:.0f}%）")
    if total_count >= 10 and low_quality_rate >= max_low_quality_rate:
        reasons.append(f"低品質画像の割合が高い（{low_quality_rate*100:.1f}% / 閾値 {max_low_quality_rate*100:.0f}%）")
    if total_count >= 10 and high_unknown_rate >= max_high_unknown_rate:
        reasons.append(f"未登録ワードの割合が高い（{high_unknown_rate*100:.1f}% / 閾値 {max_high_unknown_rate*100:.0f}%）")

    is_alert = len(reasons) > 0
    window_minutes = int(w.get("window_minutes") or th.get("window_minutes") or 60)

    subject = build_subject(is_alert=is_alert, window_minutes=window_minutes)
    if is_alert:
        body = build_body_alert(th, w, rank, reasons)
    else:
        body = build_body_ok(th, w)

    # 方針：ALERT時のみ送信。必要なら FORCE_SEND_OK=true で OK も送る
    if is_alert or FORCE_SEND_OK:
        send_mail_smtp(subject, body)
        print("MAIL sent")
    else:
        print("OK (no alert)")


if __name__ == "__main__":
    main()
