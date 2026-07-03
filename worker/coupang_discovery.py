"""
쿠팡 파트너스 골드박스 발굴 잡 (운영용).
골드박스 API를 호출하여 상위 10개 상품을 coupang_candidates 테이블에 저장한다.
차단기(job_state)·재시도·텔레그램 알림을 포함한다.
"""

import hashlib
import hmac
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests as req_lib
from dotenv import load_dotenv
from supabase import create_client

# ── [0] .env 로드 ───────────────────────────────────────────────
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

COUPANG_ACCESS_KEY = os.getenv("COUPANG_ACCESS_KEY", "")
COUPANG_SECRET_KEY = os.getenv("COUPANG_SECRET_KEY", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "false").strip().lower() == "true"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# 필수 키 검증
_missing = []
if not COUPANG_ACCESS_KEY:
    _missing.append("COUPANG_ACCESS_KEY")
if not COUPANG_SECRET_KEY:
    _missing.append("COUPANG_SECRET_KEY")
if not SUPABASE_URL:
    _missing.append("SUPABASE_URL")
if not SUPABASE_KEY:
    _missing.append("SUPABASE_SERVICE_ROLE_KEY")
if _missing:
    print(f"[FATAL] 환경변수 누락: {', '.join(_missing)}")
    sys.exit(1)

# Supabase URL 정리 (프로젝트 루트만 필요)
if "/rest/" in SUPABASE_URL:
    SUPABASE_URL = SUPABASE_URL.split("/rest/")[0]
SUPABASE_URL = SUPABASE_URL.rstrip("/")

print(f"[ENV] COUPANG  KEY: {COUPANG_ACCESS_KEY[:4]}****")
print(f"[ENV] SUPABASE URL: {SUPABASE_URL}")
print(f"[ENV] TELEGRAM    : {'ON' if TELEGRAM_ENABLED else 'OFF'}")
print()

# ── 상수 ────────────────────────────────────────────────────────
DOMAIN = "https://api-gateway.coupang.com"
METHOD = "GET"
PATH = "/v2/providers/affiliate_open_api/apis/openapi/v1/products/goldbox"
JOB_NAME = "coupang_discovery"

# ── Supabase 클라이언트 ─────────────────────────────────────────
sb = create_client(SUPABASE_URL, SUPABASE_KEY)


# ── HMAC 서명 (coupang_discovery_test.py 동일) ──────────────────
def generate_hmac(method: str, path: str, secret_key: str, access_key: str) -> str:
    """CEA HmacSHA256 인증 헤더 값을 생성한다."""
    os.environ["TZ"] = "GMT+0"
    if hasattr(time, "tzset"):
        time.tzset()

    gmt = time.gmtime()
    signed_date = time.strftime("%y%m%dT%H%M%SZ", gmt)

    message = signed_date + method + path
    signature = hmac.new(
        secret_key.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return (
        f"CEA algorithm=HmacSHA256, access-key={access_key}, "
        f"signed-date={signed_date}, signature={signature}"
    )


# ── 텔레그램 알림 ──────────────────────────────────────────────
def notify(text: str) -> None:
    """TELEGRAM_ENABLED 시 텔레그램 전송, 아니면 콘솔 출력."""
    if not TELEGRAM_ENABLED:
        print(f"[알림 OFF] {text}")
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        req_lib.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        print(f"[알림 전송] {text}")
    except Exception as e:
        print(f"[알림 실패] {e} — 원문: {text}")


# ── 유틸: 현재 시각 ISO 문자열 ──────────────────────────────────
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── [1] 차단기 체크 ─────────────────────────────────────────────
def check_circuit_breaker() -> bool:
    """enabled=True 이면 True 반환. False 이면 종료."""
    row = (
        sb.table("job_state")
        .select("enabled")
        .eq("job_name", JOB_NAME)
        .single()
        .execute()
    )
    enabled = row.data.get("enabled", False)
    if not enabled:
        print("[차단기] OFF -> 골드박스 호출 없이 종료합니다.")
        return False
    print("[차단기] ON -> 골드박스 호출을 진행합니다.")
    return True


# ── 차단기 작동 (trip) ──────────────────────────────────────────
def trip_breaker(reason: str, extra_fail: int = 1) -> None:
    """차단기를 끄고 fail_count 를 증가시킨다."""
    current = (
        sb.table("job_state")
        .select("fail_count")
        .eq("job_name", JOB_NAME)
        .single()
        .execute()
    )
    new_fail = (current.data.get("fail_count") or 0) + extra_fail

    sb.table("job_state").update({
        "enabled": False,
        "fail_count": new_fail,
        "last_error": reason,
        "paused_at": now_iso(),
        "paused_reason": reason,
    }).eq("job_name", JOB_NAME).execute()

    print(f"[차단기] 작동! enabled=False, fail_count={new_fail}")
    print(f"[차단기] 사유: {reason}")
    notify(f"[coupang_discovery 정지] {reason}")


# ── 일시 오류 기록 (차단기는 건드리지 않음) ─────────────────────
def record_transient_error(reason: str) -> None:
    current = (
        sb.table("job_state")
        .select("fail_count")
        .eq("job_name", JOB_NAME)
        .single()
        .execute()
    )
    new_fail = (current.data.get("fail_count") or 0) + 1

    sb.table("job_state").update({
        "fail_count": new_fail,
        "last_error": reason,
    }).eq("job_name", JOB_NAME).execute()

    print(f"[일시 오류] fail_count={new_fail}, 차단기 유지(ON)")
    notify(f"[coupang_discovery 일시 오류] {reason}")


# ── 성공 시 job_state 갱신 ──────────────────────────────────────
def mark_success() -> None:
    sb.table("job_state").update({
        "fail_count": 0,
        "last_error": None,
        "last_run_at": now_iso(),
    }).eq("job_name", JOB_NAME).execute()
    print("[job_state] 성공 갱신: fail_count=0, last_run_at=now")


# ── [2] 골드박스 호출 ──────────────────────────────────────────
def call_goldbox() -> req_lib.Response:
    """HMAC 서명을 생성하고 골드박스 API를 1회 호출한다."""
    auth = generate_hmac(METHOD, PATH, COUPANG_SECRET_KEY, COUPANG_ACCESS_KEY)
    headers = {
        "Authorization": auth,
        "Content-Type": "application/json",
    }
    url = DOMAIN + PATH
    print(f"[API] GET {url}")
    return req_lib.get(url, headers=headers, timeout=10)


# ── [4] 상품 저장 ──────────────────────────────────────────────
def save_candidates(products: list) -> None:
    """상위 10개 상품을 coupang_candidates 에 upsert 한다."""
    top10 = products[:10]
    rows = []
    for p in top10:
        rows.append({
            "product_id": str(p["productId"]),
            "product_name": p["productName"],
            "product_price": round(p["productPrice"]),
            "product_image": p["productImage"],
            "product_url": p["productUrl"],
            "is_rocket": p.get("isRocket", False),
            "category": p.get("categoryName", ""),
        })

    result = (
        sb.table("coupang_candidates")
        .upsert(rows, on_conflict="product_id", ignore_duplicates=True)
        .execute()
    )

    saved = len(result.data) if result.data else 0
    skipped = len(rows) - saved
    print(f"[저장] 저장 {saved}개 / 스킵(중복) {skipped}개  (총 {len(rows)}개 시도)")


# ── 메인 실행 ───────────────────────────────────────────────────
def main() -> None:
    # [1] 차단기 체크
    if not check_circuit_breaker():
        return

    # [2] 골드박스 호출 + [3] 결과 분기
    try:
        resp = call_goldbox()
    except req_lib.exceptions.RequestException as e:
        # 네트워크/타임아웃 → 최대 2번 재시도
        print(f"[네트워크 오류] {e} — 재시도 1/2")
        for attempt in range(2):
            try:
                time.sleep(2)
                resp = call_goldbox()
                break
            except req_lib.exceptions.RequestException as e2:
                if attempt == 1:
                    record_transient_error(f"네트워크 오류 3회 연속: {e2}")
                    return
                print(f"[네트워크 오류] {e2} — 재시도 2/2")
                continue

    status = resp.status_code
    print(f"[API] HTTP {status}")

    # ── HTTP 401: 인증 실패 → 서명 재생성 후 1회 재시도 ─────────
    if status == 401:
        print("[API] 401 인증 실패 — 서명 재생성 후 1회 재시도")
        try:
            time.sleep(1)
            resp = call_goldbox()
            status = resp.status_code
            print(f"[API] 재시도 HTTP {status}")
        except req_lib.exceptions.RequestException as e:
            trip_breaker(f"401 재시도 중 네트워크 오류: {e}")
            return

        if status == 401:
            trip_breaker("401 인증 실패 2회 연속")
            return

    # ── HTTP 429 / 403: 거절·제한 ──────────────────────────────
    if status in (429, 403):
        trip_breaker(f"HTTP {status} 거절/제한")
        return

    # ── HTTP 200 이외의 기타 오류 ───────────────────────────────
    if status != 200:
        trip_breaker(f"HTTP {status} 예상치 못한 응답")
        return

    # ── HTTP 200: JSON 파싱 ────────────────────────────────────
    try:
        body = resp.json()
    except json.JSONDecodeError:
        trip_breaker(f"JSON 파싱 실패: {resp.text[:200]}")
        return

    r_code = str(body.get("rCode", ""))
    r_message = body.get("rMessage", "")

    if r_code != "0":
        trip_breaker(f"rCode={r_code}, rMessage={r_message}")
        return

    # ── 정상: 저장 + 성공 갱신 ─────────────────────────────────
    products = body.get("data", [])
    print(f"[API] 상품 {len(products)}개 수신")

    if not products:
        print("[WARN] data 가 비어 있습니다. 저장 스킵.")
        mark_success()
        return

    save_candidates(products)
    mark_success()
    print("\n[완료] coupang_discovery 잡 정상 종료.")


if __name__ == "__main__":
    main()
