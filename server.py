"""
Mustit 가격비교 앱 — Flask 백엔드
"""
import re
import json
import time
import os
import requests
try:
    from curl_cffi import requests as cffi_requests
    _HAS_CFFI = True
except ImportError:
    _HAS_CFFI = False

try:
    from playwright.sync_api import sync_playwright
    _HAS_PLAYWRIGHT = True
except ImportError:
    _HAS_PLAYWRIGHT = False
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify, send_from_directory, after_this_request

app = Flask(__name__, static_folder="static")

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
# Railway Volume이 마운트된 경우 /data 사용, 아니면 로컬 폴더 사용
_DATA_DIR    = "/data" if os.path.isdir("/data") else BASE_DIR
CONFIG_FILE  = os.path.join(_DATA_DIR, "seller_config.json")
KEYS_FILE    = os.path.join(_DATA_DIR, "api_keys.json")
API_URL      = "https://openapi.naver.com/v1/search/shop.json"
PRICE_RANGE  = 0.20
MUSTIT_PRICE_FLOOR_RATIO = 0.50   # anchor 기준 -50% 미만 제품 제외
# anchor 가격 탐색 우선순위: 이 순서로 첫 번째 플랫폼 최저가를 기준점으로 사용
PRICE_ANCHOR_PRIORITY = ["머스트잇", "트렌비", "SSG", "롯데온"]

# 각 플랫폼은 (1) Open API mallName 정확 일치 또는 (2) 상품 link URL 키워드 포함
# 둘 중 어느 조건이라도 맞으면 해당 플랫폼으로 분류. 스마트스토어(=소호)는
# mallName이 개별 스토어명으로 내려오므로 반드시 link URL로 판별.
PLATFORM_MAP = {
    "머스트잇":    {"names": ["머스트잇", "MUSTIT"],
                   "url_keywords": ["mustit.co.kr"]},
    "트렌비":      {"names": ["트렌비", "TRENBE", "TRENBE(트렌비)", "트렌비(TRENBE)"],
                   "url_keywords": ["trenbe.com", "trenbe.co.kr"]},
    "SSG":        {"names": ["SSG.COM"],
                   "url_keywords": ["www.ssg.com"]},
    "롯데온":      {"names": ["롯데ON", "롯데온", "LOTTE ON", "LOTTEON", "lotteon"],
                   "url_keywords": ["lotteon.com"]},
    "스마트스토어": {"names": [],
                   "url_keywords": ["smartstore.naver.com", "brand.naver.com"]},
}

# 각 플랫폼별 이름 목록을 lowercase set으로 미리 계산 (대소문자 무시 비교용)
_PLAT_NAMES_LC = {
    plat: {n.lower() for n in cfg["names"]}
    for plat, cfg in PLATFORM_MAP.items()
}

def detect_platform(item):
    """아이템이 어느 플랫폼에 속하는지 판별. 해당 없으면 None.
    mallName은 대소문자 무시 정확 매칭 + link URL 키워드 포함 매칭."""
    mall_lc = (item.get("mallName") or "").strip().lower()
    link_lc = (item.get("link") or "").lower()
    for plat, cfg in PLATFORM_MAP.items():
        if mall_lc and mall_lc in _PLAT_NAMES_LC[plat]:
            return plat
        if any(k in link_lc for k in cfg["url_keywords"]):
            return plat
    return None


def get_anchor_price(sim_items):
    """sort=sim 1위 상품 가격을 anchor로 반환.
    반환: (anchor_price: int)
    """
    if not sim_items:
        return 0
    p_str = sim_items[0].get("lprice", "0")
    return int(p_str) if p_str.isdigit() else 0


# ── CORS: PyWebView / 로컬 파일 접근 허용 ─────────────────────────────────────
@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response

@app.route("/<path:p>", methods=["OPTIONS"])
@app.route("/", methods=["OPTIONS"])
def handle_options(p=""):
    return "", 204


# ── 파일 I/O ──────────────────────────────────────────────────────────────────
def load_json(path, default):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return default

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_keys():
    """API 키 로드. 환경변수 우선 → api_keys.json 폴백 (로컬 개발용)."""
    cid  = os.environ.get("NAVER_CLIENT_ID",  "").strip()
    csec = os.environ.get("NAVER_CLIENT_SECRET", "").strip()
    if cid and csec:
        return {"client_id": cid, "client_secret": csec}
    return load_json(KEYS_FILE, {"client_id": "", "client_secret": ""})

def load_config():return load_json(CONFIG_FILE, {})


# ── 네이버 API ─────────────────────────────────────────────────────────────────
def call_api(query: str, max_items: int = 300, sort: str = "asc") -> list:
    """Naver shop.json을 start 파라미터로 페이지네이션하며 최대 max_items개 수집.
    sort: 'asc'(가격 오름차순) | 'sim'(유사도=네이버쇼핑 기본 랭킹순) | 'dsc' | 'date'
    """
    keys = load_keys()
    cid  = keys.get("client_id",  "").strip()
    csec = keys.get("client_secret", "").strip()
    if not cid or not csec:
        raise ValueError("API 키 미설정 — 설정 화면에서 네이버 Client ID / Secret을 입력하세요.")
    headers = {"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": csec}

    all_items = []
    start = 1
    # Naver shop.json: start 1~1000, display 1~100
    while len(all_items) < max_items and start <= 1000:
        display = min(100, max_items - len(all_items))
        try:
            r = requests.get(
                API_URL,
                params={"query": query, "display": display, "start": start, "sort": sort},
                headers=headers, timeout=10,
            )
        except requests.exceptions.Timeout:
            raise ValueError("요청 시간 초과 — 잠시 후 다시 시도하세요.")
        except requests.exceptions.ConnectionError:
            raise ValueError("네트워크 연결 오류 — 인터넷 연결을 확인하세요.")
        if r.status_code == 401:
            raise ValueError("인증 실패(401) — Client ID / Secret을 확인하세요.")
        if r.status_code == 403:
            raise ValueError("권한 없음(403) — 네이버 개발자센터에서 검색 API가 활성화됐는지 확인하세요.")
        if r.status_code != 200:
            # 페이지네이션 중 일부 페이지 실패 시 거기까지 수집된 것만 반환
            if all_items: break
            raise ValueError(f"API 오류({r.status_code}): {r.text[:120]}")
        batch = r.json().get("items", [])
        if not batch: break
        all_items.extend(batch)
        if len(batch) < display: break  # 더 이상 결과 없음
        start += display
    return all_items

def strip_html(t): return re.sub(r"<[^>]+>", "", t).strip()


def call_api_asc_from_floor(query: str, floor_price: int, max_items: int = 200) -> list:
    """sort=asc로 1위부터 순서대로 탐색.
    floor_price 이상인 첫 상품을 발견한 시점부터 max_items개 수집.
    floor_price=0 이면 처음부터 수집."""
    keys = load_keys()
    cid  = keys.get("client_id",  "").strip()
    csec = keys.get("client_secret", "").strip()
    if not cid or not csec:
        raise ValueError("API 키 미설정 — 설정 화면에서 네이버 Client ID / Secret을 입력하세요.")
    headers = {"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": csec}

    collected   = []
    start       = 1
    above_floor = (floor_price <= 0)   # floor 없으면 처음부터 수집

    while len(collected) < max_items and start <= 1000:
        try:
            r = requests.get(
                API_URL,
                params={"query": query, "display": 100, "start": start, "sort": "asc"},
                headers=headers, timeout=10,
            )
        except requests.exceptions.Timeout:
            raise ValueError("요청 시간 초과 — 잠시 후 다시 시도하세요.")
        except requests.exceptions.ConnectionError:
            raise ValueError("네트워크 연결 오류 — 인터넷 연결을 확인하세요.")
        if r.status_code == 401:
            raise ValueError("인증 실패(401) — Client ID / Secret을 확인하세요.")
        if r.status_code == 403:
            raise ValueError("권한 없음(403) — 네이버 개발자센터에서 검색 API가 활성화됐는지 확인하세요.")
        if r.status_code != 200:
            if collected: break
            raise ValueError(f"API 오류({r.status_code}): {r.text[:120]}")

        batch = r.json().get("items", [])
        if not batch:
            break

        for item in batch:
            p_str = item.get("lprice", "0")
            price = int(p_str) if p_str.isdigit() else 0
            if price == 0:
                continue
            if not above_floor:
                if price >= floor_price:
                    above_floor = True   # 이 상품부터 수집 시작
                else:
                    continue             # floor 미달 → 스킵
            collected.append(item)
            if len(collected) >= max_items:
                break

        if len(collected) >= max_items:
            break
        if len(batch) < 100:
            break   # 더 이상 결과 없음
        start += 100

    return collected


def search(query, ref_price=0, top_n=10):
    # Step 1: sim 200개 (네이버쇼핑 랭킹순)
    sim_items = call_api(query, 200, "sim")

    # Step 2: anchor = sim 1위 상품 가격
    anchor_price = get_anchor_price(sim_items)
    price_floor  = int(anchor_price * MUSTIT_PRICE_FLOOR_RATIO) if anchor_price > 0 else 0

    # Step 3: asc 1위부터 탐색, floor 이상 첫 상품부터 200개 수집
    asc_items = call_api_asc_from_floor(query, price_floor, 200)

    results = []
    seen    = set()
    for item in asc_items:
        p_str = item.get("lprice","0")
        price = int(p_str) if p_str.isdigit() else 0
        if price == 0: continue
        plat = detect_platform(item)
        if plat is None: continue
        # anchor 최저가 기준 -30% 미만 제품 제외
        if price_floor > 0 and price < price_floor:
            continue
        # 판매자 ID: 스마트스토어의 경우 mallName이 곧 개별 셀러 스토어명.
        # 트렌비/SSG/롯데ON/머스트잇은 mallName이 플랫폼 자신이며 셀러도 동일.
        seller = (item.get("mallName") or "").strip() or plat
        # 동일 (플랫폼, 셀러, 링크) 중복 제거
        key  = f"{plat}|{seller}|{item.get('link','')}"
        if key in seen: continue
        seen.add(key)
        results.append({
            "rank": 0, "platform": plat, "seller": seller,
            "name":  strip_html(item.get("title","")),
            "price": price,
            "image": item.get("image",""),
            "link":  item.get("link",""),
            "brand": strip_html(item.get("brand","")),
        })
        if len(results) >= top_n * 6: break
    results.sort(key=lambda x: x["price"])
    for i, r in enumerate(results): r["rank"] = i+1
    return results[:top_n]

def build_naver_rank_map(query, max_items=200):
    """sort=sim(유사도=네이버쇼핑 기본 랭킹순)로 호출 후 link → 노출순위(1-indexed) 매핑.
    실패해도 전체 검색은 진행되도록 예외 무시."""
    try:
        sim_items = call_api(query, max_items=max_items, sort="sim")
    except Exception:
        return {}
    rmap = {}
    for idx, it in enumerate(sim_items):
        link = (it.get("link") or "").strip()
        if link and link not in rmap:
            rmap[link] = idx + 1
    return rmap


def _extract_naver_nmid(link: str) -> str:
    """머스트잇 네이버 링크에서 nvMid(Naver 고유 mall 상품 ID)를 추출.

    Naver API가 내려주는 머스트잇 링크는 두 가지 형태:
      ① naver_session.php?URL=<encoded_inner_url>   ← inner URL에 nvMid= 포함
      ② 직접 mustit URL에 ?nvMid=... 포함

    nvMid를 찾으면 search.shopping.naver.com/product/<nvMid> 로 바로 연결
    (해당 머스트잇 listing 페이지 → "구매하기" 클릭 시 CPC 할인 자동 적용).
    """
    if not link:
        return ""
    # 1단계: naver_session 래퍼 안에 있는 경우 내부 URL 추출
    decoded = unquote(link)
    ns = re.search(r'naver_session\.php\?URL=([^\s"]+)', decoded)
    inner = unquote(ns.group(1)) if ns else decoded
    # 2단계: nvMid= 파라미터 탐색
    nv = re.search(r'[?&]nvMid=(\d+)', inner)
    if nv:
        return nv.group(1)
    return ""


def search_by_platform(query, ref_price=0, top_n=10, skip_enrich=False):
    """각 플랫폼별로 독립적으로 가격 오름차순 정렬 후 top_n개 + 플랫폼 내부 순위 부여.
    5개 주요 몰(머스트잇/트렌비/SSG/롯데온/스마트스토어)에 해당 없는 항목은 "기타" 버킷에 담아
    mallName을 그대로 seller(=사이트명)로 노출.
    각 항목에는 네이버쇼핑 기본 랭킹순 노출 순위(naver_rank)도 부가.
    skip_enrich=True 이면 enrich_sellers_in_place 를 건너뜀 (Phase-1 fast path).
    Phase-2(skip_enrich=False)는 Phase-1 캐시를 재사용해 Naver API 재호출 생략."""
    _cache_key = (query, ref_price, top_n)

    # ── Phase-2: 캐시된 by_plat 재사용 (Naver API 재호출 없음) ──
    if not skip_enrich:
        _ce = _BYPLAT_CACHE.get(_cache_key)
        if _ce and (time.time() - _ce[0]) < _BYPLAT_TTL:
            by_plat          = _ce[1]
            _cached_mustit   = _ce[2] if len(_ce) > 2 else 0
            _cached_anchor_p = _ce[3] if len(_ce) > 3 else None
            enrich_timing = enrich_sellers_in_place(
                [it for p, lst in by_plat.items() if p != "기타" for it in lst]
            )
            return by_plat, enrich_timing, _cached_mustit, _cached_anchor_p

    # ── Step 1: sim 200개 (네이버쇼핑 랭킹순) ────────────────────────────────────────
    sim_items = call_api(query, 200, "sim")

    # rank_map: sim 순서 → 네이버쇼핑 노출순위
    rank_map = {}
    for _idx, _it in enumerate(sim_items):
        _lnk = (_it.get("link") or "").strip()
        if _lnk and _lnk not in rank_map:
            rank_map[_lnk] = _idx + 1

    # ── Step 2: anchor = sim 1위 상품 가격 ────────────────────────────────────────
    anchor_price = get_anchor_price(sim_items)
    anchor_plat  = None
    mustit_min_price = anchor_price
    price_floor  = int(anchor_price * MUSTIT_PRICE_FLOOR_RATIO) if anchor_price > 0 else 0

    # ── Step 3: asc 1위부터 탐색, floor 이상 첫 상품부터 200개 수집 ──────────────────
    asc_items = call_api_asc_from_floor(query, price_floor, 200)

    by_plat  = {p: [] for p in PLATFORM_MAP}
    by_plat["기타"] = []
    seen    = set()
    for item in asc_items:
        p_str = item.get("lprice", "0")
        price = int(p_str) if p_str.isdigit() else 0
        if price == 0: continue
        # anchor 최저가 기준 -30% 미만 제품 제외
        if price_floor > 0 and price < price_floor:
            continue
        plat = detect_platform(item)
        mall_raw = (item.get("mallName") or "").strip()
        link_raw = item.get("link", "")
        # 네이버 가격비교 카탈로그 URL 제외 (search.shopping.naver.com/catalog/...)
        if "search.shopping.naver.com/catalog/" in link_raw:
            continue
        is_other = plat is None
        if is_other:
            if not mall_raw: continue   # 정체불명 항목은 스킵
            plat = "기타"
        # dedup 키에는 mallName을 써서 동일 셀러 중복만 거름
        key = f"{plat}|{mall_raw}|{link_raw}"
        if key in seen: continue
        seen.add(key)
        link = link_raw
        by_plat[plat].append({
            "rank": 0, "platform": plat,
            # 기타 버킷은 mallName이 곧 사이트명. 5개 주요 몰은 이후 enrich 단계에서 채워짐.
            "seller": mall_raw if is_other else "",
            "mallName": mall_raw,
            "name":  strip_html(item.get("title", "")),
            "price": price,
            "image": item.get("image", ""),
            "link":  link,
            "brand": strip_html(item.get("brand", "")),
            # 네이버쇼핑 기본 랭킹순 노출 순위 (미발견 시 None)
            "naver_rank": rank_map.get(link),
            # nvMid: Naver 고유 mall 상품 ID — link URL의 nvMid= 파라미터에서 추출.
            # 없으면 productId(API 반환값)를 폴백으로 사용.
            # search.shopping.naver.com/product/<id> 경유 시 CPC 할인 자동 적용됨.
            "naver_nmid":       _extract_naver_nmid(link),
            "naver_product_id": item.get("productId", ""),
        })
    # 플랫폼 내부 가격 오름차순 정렬 + 플랫폼 내 순위 부여 + top_n 컷
    for plat, lst in by_plat.items():
        lst.sort(key=lambda x: x["price"])
        for i, r in enumerate(lst):
            r["rank"] = i + 1
        by_plat[plat] = lst[:top_n]
    # PDP 판매자 ID 병렬 추정은 5개 주요 몰만 대상 (기타는 mallName 그대로 사용)
    # skip_enrich=True: Phase-1 fast path — 결과를 캐시에 저장하고 즉시 반환
    enrich_timing = {}
    if skip_enrich:
        _BYPLAT_CACHE[_cache_key] = (time.time(), by_plat, mustit_min_price, anchor_plat)
    else:
        enrich_timing = enrich_sellers_in_place(
            [it for p, lst in by_plat.items() if p != "기타" for it in lst]
        )
    return by_plat, enrich_timing, mustit_min_price, anchor_plat


# ── PDP 판매자 추정 (각 몰별 best-effort 스크래핑) ───────────────────────────────
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
_SELLER_CACHE = {}  # link → seller_id (세션 내 재사용)
_DETAIL_CACHE = {}  # link → mustit detail dict (세션 내 재사용)
_BYPLAT_CACHE = {}  # (query,ref,top_n) → (timestamp, by_plat)
_BYPLAT_TTL   = 90  # Phase-1 결과 재사용 TTL (초)

# ── 머스트잇 요청 Rate Limiter ──────────────────────────────────────────────
import threading
_MUSTIT_LOCK         = threading.Lock()
_MUSTIT_LAST_REQ     = 0.0   # 마지막 요청 시각
_MUSTIT_MIN_INTERVAL = 1.5   # 요청 간 최소 간격 (초)
# Circuit Breaker: 봇 감지 응답 연속 N회 → 일정 시간 중단
_MUSTIT_BOT_COUNT    = 0
_MUSTIT_BOT_LIMIT    = 3     # 연속 봇 감지 허용 횟수
_MUSTIT_BOT_COOLDOWN = 60.0  # 봇 감지 후 대기 시간 (초)
_MUSTIT_BOT_UNTIL    = 0.0   # cooldown 만료 시각

# ── 머스트잇 curl_cffi 세션 (쿠키 유지용) ────────────────────────────────────
_MUSTIT_SESSION      = None
_MUSTIT_SESSION_LOCK = threading.Lock()

def _get_mustit_session():
    """m.web.mustit.co.kr 홈 방문으로 쿠키를 세팅한 curl_cffi Session 반환.
    없으면 None (requests 폴백).
    핵심: mustit.co.kr 쿠키는 m.web.mustit.co.kr 요청에 전달되지 않으므로
    반드시 m.web.mustit.co.kr 도메인으로 워밍업해야 한다.
    """
    global _MUSTIT_SESSION
    if _MUSTIT_SESSION is not None:
        return _MUSTIT_SESSION
    if not _HAS_CFFI:
        return None
    with _MUSTIT_SESSION_LOCK:
        if _MUSTIT_SESSION is not None:
            return _MUSTIT_SESSION
        try:
            sess = cffi_requests.Session(impersonate="chrome124")
            # m.web.mustit.co.kr 홈 방문 → 동일 도메인 세션 쿠키 발급
            sess.get("https://m.web.mustit.co.kr/", timeout=10,
                     headers={"Accept-Language": "ko-KR,ko;q=0.9",
                               "Sec-Fetch-Dest": "document",
                               "Sec-Fetch-Mode": "navigate",
                               "Sec-Fetch-Site": "none"})
            _MUSTIT_SESSION = sess
            print("[mustit] session warmed up (m.web.mustit.co.kr)")
        except Exception as e:
            print(f"[mustit] session warmup failed: {e}")
    return _MUSTIT_SESSION

# ── Playwright 헤드리스 Chrome 싱글톤 ────────────────────────────────────────
_PW_PLAYWRIGHT = None
_PW_BROWSER    = None
_PW_BROWSER_LOCK = threading.Lock()

_PW_CTX        = None   # 쿠키가 살아있는 공유 브라우저 컨텍스트
_PW_CTX_LOCK   = threading.Lock()

# Cloudflare / 자동화 감지 우회용 스텔스 스크립트
_PW_STEALTH_JS = """
// webdriver 플래그 제거
Object.defineProperty(navigator, 'webdriver', { get: () => undefined, configurable: true });
// Chrome DevTools Protocol 흔적 제거
['cdc_adoQpoasnfa76pfcZLmcfl_Array',
 'cdc_adoQpoasnfa76pfcZLmcfl_Promise',
 'cdc_adoQpoasnfa76pfcZLmcfl_Symbol'].forEach(k => { try { delete window[k]; } catch(_) {} });
// 실제 브라우저와 동일한 언어/플러그인
Object.defineProperty(navigator, 'languages', { get: () => ['ko-KR', 'ko', 'en-US', 'en'] });
Object.defineProperty(navigator, 'plugins', {
    get: () => {
        const arr = [
            { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
            { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai.pdf', description: '' },
            { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' },
        ];
        arr.refresh = () => {};
        return arr;
    }
});
// Chrome 객체 위장
if (!window.chrome) {
    window.chrome = { runtime: {}, loadTimes: function(){}, csi: function(){}, app: {} };
}
// notification permission 위장
const origQuery = window.navigator.permissions ? window.navigator.permissions.query.bind(window.navigator.permissions) : null;
if (origQuery) {
    window.navigator.permissions.query = (parameters) =>
        parameters.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : origQuery(parameters);
}
"""

def _get_pw_browser():
    """Playwright Chromium 싱글톤 반환. 실패 시 None."""
    global _PW_PLAYWRIGHT, _PW_BROWSER
    if _PW_BROWSER is not None:
        return _PW_BROWSER
    if not _HAS_PLAYWRIGHT:
        return None
    with _PW_BROWSER_LOCK:
        if _PW_BROWSER is not None:
            return _PW_BROWSER
        try:
            _PW_PLAYWRIGHT = sync_playwright().start()
            _PW_BROWSER = _PW_PLAYWRIGHT.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox", "--disable-dev-shm-usage",
                    "--disable-setuid-sandbox",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            print("[mustit-pw] Chromium launched")
        except Exception as e:
            print(f"[mustit-pw] launch failed: {e}")
    return _PW_BROWSER


def _pw_wait_challenge(page, timeout_ms=25000):
    """Cloudflare JS 챌린지('Human Verification' 타이틀)가 자동 해결될 때까지 기다린다.
    챌린지가 풀리면 Cloudflare가 알아서 원래 페이지로 리다이렉트한다.
    """
    try:
        title = page.title()
        if "Human Verification" not in title and "Just a moment" not in title:
            return  # 챌린지 없음
        print(f"[mustit-pw] CF challenge detected (title='{title}'), waiting up to {timeout_ms}ms ...")
        page.wait_for_function(
            """() => {
                const t = document.title;
                return t !== 'Human Verification' && t !== 'Just a moment...';
            }""",
            timeout=timeout_ms,
        )
        # 챌린지 통과 후 DOM 안정화 대기
        page.wait_for_load_state("domcontentloaded", timeout=15000)
        page.wait_for_timeout(1000)
        print(f"[mustit-pw] CF challenge resolved → title='{page.title()}' url={page.url}")
    except Exception as e:
        print(f"[mustit-pw] CF challenge wait error (will proceed anyway): {e}")


def _get_pw_context():
    """쿠키가 유지되는 공유 BrowserContext 반환.
    최초 호출 시 m.web.mustit.co.kr 홈을 방문해 Cloudflare 세션 쿠키를 획득한다.
    """
    global _PW_CTX
    if _PW_CTX is not None:
        return _PW_CTX
    browser = _get_pw_browser()
    if not browser:
        return None
    with _PW_CTX_LOCK:
        if _PW_CTX is not None:
            return _PW_CTX
        try:
            ctx = browser.new_context(
                locale="ko-KR",
                user_agent=_UA,
                viewport={"width": 390, "height": 844},   # 모바일 사이즈
                extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9"},
            )
            # 스텔스 JS 주입 (webdriver 감지 우회)
            ctx.add_init_script(_PW_STEALTH_JS)
            # 이미지·폰트만 차단 (CSS·JS는 Cloudflare 챌린지에 필요)
            ctx.route(
                "**/*.{png,jpg,jpeg,gif,webp,svg,ico,woff,woff2,ttf,otf}",
                lambda r: r.abort(),
            )
            # 홈페이지 워밍업 → Cloudflare 챌린지 통과 + cf_clearance 쿠키 획득
            warmup = ctx.new_page()
            print("[mustit-pw] warming up context on m.web.mustit.co.kr ...")
            warmup.goto("https://m.web.mustit.co.kr/", wait_until="domcontentloaded", timeout=30000)
            _pw_wait_challenge(warmup, timeout_ms=30000)
            cookies_after = ctx.cookies()
            cf_cookies = [c["name"] for c in cookies_after if "cf_" in c["name"].lower()]
            print(f"[mustit-pw] warmup done, total_cookies={len(cookies_after)}, cf_cookies={cf_cookies}")
            warmup.close()
            _PW_CTX = ctx
        except Exception as e:
            print(f"[mustit-pw] context init failed: {e}")
    return _PW_CTX


def _fetch_mustit_html_playwright(pd_id):
    """Playwright로 머스트잇 상품 상세 HTML 반환 (Cloudflare JS 챌린지 통과).
    공유 컨텍스트를 사용해 세션 쿠키를 유지한다.
    """
    global _PW_CTX
    ctx = _get_pw_context()
    if not ctx:
        return None
    # 모바일 상품 상세 URL을 직접 사용
    url = f"https://m.web.mustit.co.kr/v2/m/product/product_detail/{pd_id}"
    page = None
    try:
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=40000)
        # Cloudflare JS 챌린지가 발동됐으면 해결될 때까지 대기
        _pw_wait_challenge(page, timeout_ms=30000)
        page.wait_for_timeout(1500)  # RSC push 스크립트 로딩 대기
        final_url = page.url
        html = page.content()
        page.close()

        if "sellerId" in html:
            print(f"[mustit-pw] OK pd_id={pd_id} final_url={final_url} len={len(html)}")
            return html

        # 여전히 챌린지 또는 홈으로 리다이렉트된 경우 → 컨텍스트 초기화 후 재시도
        still_challenge = "Human Verification" in html or "Just a moment" in html
        redirected_home = pd_id not in final_url
        if still_challenge or redirected_home:
            reason = "challenge" if still_challenge else f"redirect→{final_url}"
            print(f"[mustit-pw] {reason}, resetting context and retrying...")
            try:
                ctx.close()
            except Exception:
                pass
            _PW_CTX = None
            ctx2 = _get_pw_context()
            if not ctx2:
                return None
            page2 = ctx2.new_page()
            page2.goto(url, wait_until="domcontentloaded", timeout=40000)
            _pw_wait_challenge(page2, timeout_ms=30000)
            page2.wait_for_timeout(1500)
            html2 = page2.content()
            page2.close()
            if "sellerId" in html2:
                print(f"[mustit-pw] OK (retry) pd_id={pd_id} len={len(html2)}")
                return html2
            print(f"[mustit-pw] no sellerId after retry pd_id={pd_id} head={html2[:300]}")
            return None

        print(f"[mustit-pw] no sellerId pd_id={pd_id} final_url={final_url} len={len(html)} head={html[:200]}")
        return None
    except Exception as e:
        if page:
            try:
                page.close()
            except Exception:
                pass
        print(f"[mustit-pw] error pd_id={pd_id}: {e}")
        # 컨텍스트가 깨진 경우 초기화
        _PW_CTX = None
        return None


def _find_first_key(obj, keys):
    """중첩 dict/list를 재귀 탐색해 keys 중 하나를 value 값이 비어있지 않은 상태로 최초 발견 시 반환."""
    if isinstance(obj, dict):
        for k in keys:
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for v in obj.values():
            r = _find_first_key(v, keys)
            if r: return r
    elif isinstance(obj, list):
        for v in obj:
            r = _find_first_key(v, keys)
            if r: return r
    return None

def _find_list_key(obj, keys):
    """중첩 dict/list를 재귀 탐색해 keys 중 하나에 해당하는 비어있지 않은 list를 최초 발견 시 반환."""
    if isinstance(obj, dict):
        for k in keys:
            v = obj.get(k)
            if isinstance(v, list) and v:
                return v
        for v in obj.values():
            r = _find_list_key(v, keys)
            if r is not None: return r
    elif isinstance(obj, list):
        for v in obj:
            r = _find_list_key(v, keys)
            if r is not None: return r
    return None

def _extract_pay_discs_from_html(html, sell_price):
    """상품 페이지 HTML에서 결제수단별 추가 할인을 추출.
    returns list[{name: str, price: int}]  (price = 할인 금액, 양수)
    """
    if not html or not sell_price:
        return []
    discs = []

    # 즉시할인 (스마트스토어, SSG 등) — "즉시할인 X,XXX원" / "즉시 X,XXX원 할인"
    m = re.search(r'즉시\s*할인[^\d<]{0,30}([\d,]+)원', html)
    if m:
        d = int(m.group(1).replace(',', ''))
        if 0 < d < sell_price * 0.5:
            if not any(x["name"] == "즉시할인" for x in discs):
                discs.append({"name": "즉시할인", "price": d})

    # 토스페이 결제혜택 — "토스페이 결제혜택 412,527원" 형식 (최종가 제시)
    m = re.search(r'토스페이\s*결제혜택[^\d<]{0,60}([\d,]+)원', html)
    if m:
        final = int(m.group(1).replace(',', ''))
        d = sell_price - final
        if 0 < d < sell_price * 0.5:
            discs.append({"name": "토스페이 결제혜택", "price": d})

    # 카드 즉시할인 — "카드할인 최대 X,XXX원" / "카드 즉시할인 X,XXX원"
    m = re.search(r'(카드\s*(?:즉시\s*)?할인|신용카드\s*할인)[^\d<]{0,40}([\d,]+)원', html)
    if m:
        d = int(m.group(2).replace(',', ''))
        if 0 < d < sell_price * 0.3:
            name = re.sub(r'\s+', '', m.group(1))
            if not any(x["name"] == name for x in discs):
                discs.append({"name": name, "price": d})

    # 엘페이 (롯데온) — "엘페이 포인트 X,XXX원" / "엘페이 결제 시 X,XXX원 할인"
    m = re.search(r'엘페이[^\d<]{0,40}([\d,]+)원', html)
    if m:
        d = int(m.group(1).replace(',', ''))
        if 0 < d < sell_price * 0.3:
            if not any(x["name"].startswith("엘페이") for x in discs):
                discs.append({"name": "엘페이 결제혜택", "price": d})

    # SSG머니 / 신세계포인트
    m = re.search(r'(SSG머니|신세계포인트)[^\d<]{0,40}최대\s*([\d,]+)원', html)
    if m:
        d = int(m.group(2).replace(',', ''))
        if 0 < d < sell_price * 0.2:
            discs.append({"name": m.group(1) + " 혜택", "price": d})

    # 네이버페이 포인트 (스마트스토어) — 금액으로 표시되는 경우만
    m = re.search(r'네이버페이\s*포인트[^\d<]{0,40}최대\s*([\d,]+)원', html)
    if m:
        d = int(m.group(1).replace(',', ''))
        if 0 < d < sell_price * 0.2:
            if not any(x["name"].startswith("네이버페이") for x in discs):
                discs.append({"name": "네이버페이 포인트", "price": d})

    # 네이버플러스 멤버십 (스마트스토어)
    m = re.search(r'네이버플러스\s*(?:멤버십)?\s*[^\d<]{0,30}([\d,]+)원', html)
    if m:
        d = int(m.group(1).replace(',', ''))
        if 0 < d < sell_price * 0.3:
            if not any("플러스" in x["name"] or "멤버십" in x["name"] for x in discs):
                discs.append({"name": "네이버플러스 멤버십", "price": d})

    # 쿠폰 할인 (스마트스토어 등)
    m = re.search(r'쿠폰\s*(?:최대\s*)?할인[^\d<]{0,20}([\d,]+)원', html)
    if m:
        d = int(m.group(1).replace(',', ''))
        if 0 < d < sell_price * 0.5:
            if not any("쿠폰" in x["name"] for x in discs):
                discs.append({"name": "쿠폰 혜택", "price": d})

    # 나의 혜택가 (롯데온) — 최종 혜택 가격이 표시되는 경우
    # 예: "나의 혜택가 412,000원" → discount = sell_price - 412000
    m = re.search(r'나의\s*혜택가[^\d<]{0,40}([\d,]+)원', html)
    if m:
        final = int(m.group(1).replace(',', ''))
        d = sell_price - final
        if 0 < d < sell_price * 0.5:
            if not any("혜택가" in x["name"] for x in discs):
                discs.append({"name": "나의 혜택가", "price": d})

    # 즉시할인 금액 (롯데온 등 다양한 표현) — "X,XXX원 즉시할인"
    m2 = re.search(r'([\d,]+)원\s*즉시\s*할인', html)
    if m2:
        d = int(m2.group(1).replace(',', ''))
        if 0 < d < sell_price * 0.5:
            if not any(x["name"] == "즉시할인" for x in discs):
                discs.append({"name": "즉시할인", "price": d})

    return discs


def _extract_pay_discs_from_json(data, sell_price):
    """API JSON에서 결제수단별 추가 할인 추출.
    returns list[{name: str, price: int}]
    """
    if not data:
        return []
    discs = []

    # ── 트렌비 전용: data.data.product.promotionBenefits ──────────────────────
    # displaygateway.trenbe.com/v3/sdp 응답 구조:
    # {data: {product: {promotionBenefits: [{methodName, paymentDiscount, ...}]}}}
    try:
        promo_benefits = (
            data.get("data") or {}
        ).get("product", {}).get("promotionBenefits") or []
        for pb in promo_benefits:
            if not isinstance(pb, dict): continue
            amt = pb.get("paymentDiscount") or pb.get("discountPrice") or 0
            try: amt = int(amt)
            except Exception: amt = 0
            if amt <= 0: continue
            method = (pb.get("methodName") or pb.get("displayContent") or "결제혜택").strip()
            # "토스페이" → "토스페이 결제혜택"
            name = method if "결제" in method or "할인" in method else method + " 결제혜택"
            if not any(x["name"] == name for x in discs):
                discs.append({"name": name, "price": amt})
    except Exception:
        pass

    if discs:
        return discs  # 트렌비 구조에서 찾은 경우 바로 반환

    if not sell_price:
        return []

    # ── 범용: 혜택 배열 키 탐색 ──────────────────────────────────────────────
    benefit_list = _find_list_key(data, (
        "benefitList", "cardBenefitList", "paymentBenefitList",
        "payBenefitList", "addBenefitList", "priceBenefitList",
        "cardDcInfoList", "paymentDiscountList",
    ))
    if benefit_list:
        for b in (benefit_list if isinstance(benefit_list, list) else []):
            if not isinstance(b, dict): continue
            name = (b.get("benefitName") or b.get("cardNm") or
                    b.get("name") or b.get("paymentName") or "").strip()
            da = (b.get("discountAmount") or b.get("dcAmt") or
                  b.get("cardDcAmt") or b.get("benefitAmount"))
            bp = (b.get("benefitPrice") or b.get("finalPrice") or b.get("afterDcPrice"))
            if name and da:
                try:
                    amt = int(da)
                    if 0 < amt < sell_price * 0.5:
                        discs.append({"name": name, "price": amt})
                except Exception: pass
            elif name and bp:
                try:
                    final = int(bp)
                    d = sell_price - final
                    if 0 < d < sell_price * 0.3:
                        discs.append({"name": name, "price": d})
                except Exception: pass

    # ── 토스페이 단일 필드 ────────────────────────────────────────────────────
    for key in ("tossPayBenefitPrice", "tossBenefitPrice", "tossPayPrice"):
        v = _find_int_key(data, (key,))
        if v and v < sell_price:
            d = sell_price - v
            if 0 < d < sell_price * 0.3 and not any(x["name"].startswith("토스페이") for x in discs):
                discs.append({"name": "토스페이 결제혜택", "price": d})
            break
    for key in ("tossDiscountAmount", "tossPayDcAmt", "tossDcAmt"):
        v = _find_int_key(data, (key,))
        if v and v > 0 and not any(x["name"].startswith("토스페이") for x in discs):
            if 0 < v < sell_price * 0.3:
                discs.append({"name": "토스페이 결제혜택", "price": v})
            break

    # ── 엘페이 단일 필드 (롯데온) ─────────────────────────────────────────────
    for key in ("elPayDcAmt", "elDcAmt", "lPayDcAmt", "lottiePayDcAmt"):
        v = _find_int_key(data, (key,))
        if v and v > 0 and not any(x["name"].startswith("엘페이") for x in discs):
            if 0 < v < sell_price * 0.3:
                discs.append({"name": "엘페이 결제혜택", "price": v})
            break

    return discs


def _build_price_info(sell_price, origin_price=None, discount_rate=None,
                      discounts=None, max_benefit=None, payment_discounts=None):
    """공통 price_info dict 생성. 머스트잇 외 플랫폼 공용.
    머스트잇 price_info와 동일한 스키마 사용 → 프론트엔드 렌더러 공유 가능."""
    if not sell_price:
        return None
    discs = discounts or []
    msrp_rate = None
    if discount_rate:
        msrp_rate = int(discount_rate)
    elif origin_price and origin_price > sell_price:
        msrp_rate = round((origin_price - sell_price) / origin_price * 100)
    if max_benefit is None:
        total_disc = sum(d.get("price", 0) for d in discs)
        max_benefit = sell_price - total_disc if total_disc > 0 else sell_price
    return {
        "sell_price":        sell_price,
        "max_benefit":       max_benefit,
        "msrp":              origin_price,
        "msrp_rate":         msrp_rate,
        "discounts":         discs,
        "naver_discount":    0,
        "naver_pct":         0,
        "payment_discounts": payment_discounts or [],
    }

def _parse_options(opts_raw):
    """플랫폼별 field name 차이를 흡수하는 공통 옵션 파싱.
    반환: [{"label": str, "stock": int}, ...] 또는 None."""
    result = []
    for opt in (opts_raw or []):
        if not isinstance(opt, dict): continue
        color = (opt.get("color") or opt.get("colorNm") or opt.get("colorName") or "").strip()
        size  = (opt.get("size")  or opt.get("sizeNm")  or opt.get("sizeName")  or "").strip()
        name  = (opt.get("optionName") or opt.get("name") or opt.get("nm") or "").strip()
        # label 조합: color/size 우선, 없으면 optionValue1/2, 없으면 optionName1/2 (스마트스토어 optionCombinations)
        parts = [x for x in [color or name, size] if x]
        if not parts:
            parts = [str(opt.get(k, "")).strip()
                     for k in ("optionValue1","optionValue2","optionValue","value")
                     if str(opt.get(k,"")).strip()]
        if not parts:
            # 스마트스토어 optionCombinations: {optionName1:"IT 46", optionName2:"", stockQuantity:5}
            parts = [str(opt.get(k, "")).strip()
                     for k in ("optionName1","optionName2")
                     if str(opt.get(k,"")).strip()]
        if not parts:
            # SSG itemOptionList: {optionAttrList:[{optionAttrValue:"L"}, ...]}
            attr_list = opt.get("optionAttrList") or []
            if isinstance(attr_list, list):
                parts = [str(a.get("optionAttrValue","")).strip()
                         for a in attr_list if isinstance(a, dict) and a.get("optionAttrValue","").strip()]
        label = " / ".join(p for p in parts[:2] if p)
        if not label: continue
        # stockQuantity(스마트스토어), stockCnt(SSG), stock, qty, quantity 순으로 탐색
        stock_raw = (opt.get("stockQuantity") or opt.get("stock") or opt.get("stockCnt") or
                     opt.get("stockCount") or opt.get("qty") or opt.get("quantity") or
                     opt.get("rtlStockQty") or opt.get("inventoryQuantity") or opt.get("remainQty") or 0)
        try: stock = int(stock_raw)
        except: stock = 0
        result.append({"label": label, "stock": stock})
    return result if result else None

def _find_int_key(obj, keys):
    """중첩 dict/list를 재귀 탐색해 keys 중 하나를 양수 int 값으로 최초 발견 시 반환."""
    if isinstance(obj, dict):
        for k in keys:
            v = obj.get(k)
            if isinstance(v, (int, float)) and v > 0:
                return int(v)
            if isinstance(v, str) and v.isdigit() and int(v) > 0:
                return int(v)
        for v in obj.values():
            r = _find_int_key(v, keys)
            if r is not None: return r
    elif isinstance(obj, list):
        for v in obj:
            r = _find_int_key(v, keys)
            if r is not None: return r
    return None

def _fetch_trenbe_detail(link):
    """트렌비 displaygateway API로 상세정보 일괄 수집 (SPA HTML 우회).
    반환: dict(seller, condition, shipping_fee, stock, actual_price,
               origin_price, discount_rate, options, price_info) | None
    """
    # /good/141721751 또는 /good/3375+141721751 형식 모두 처리
    # Naver Shopping 링크: /good/{goodsno} (7~10자리 숫자)
    # 트렌비 리다이렉트: /good/{brandId}+{goodsno}  → + 이후 숫자가 실제 goodsno
    m = re.search(r'/good/[\d+]*?(\d{7,})', link)  # 7자리 이상 숫자 = 실제 상품번호
    if not m:
        m = re.search(r'/good/(\d+)', link)         # 폴백: 첫 번째 숫자
    if not m: return None
    goodsno = m.group(1)

    if link in _DETAIL_CACHE:
        return _DETAIL_CACHE[link]

    api_url = f"https://displaygateway.trenbe.com/v3/sdp?goodsno={goodsno}&relatedProductsSize=0"
    try:
        r = requests.get(api_url, timeout=6,
                         headers={"User-Agent": _UA,
                                  "Accept": "application/json",
                                  "Referer": "https://www.trenbe.com/"})
        if r.status_code != 200: return None
        data = r.json()
    except Exception:
        return None

    detail = {}

    # 판매자
    seller = _find_first_key(data, ("sellerName", "officialName", "brandName", "brand"))
    if seller: detail["seller"] = seller.strip()[:50]

    # 판매가
    sell = _find_int_key(data, ("salePrice", "sellPrice", "discountPrice", "finalPrice"))
    if sell: detail["actual_price"] = sell

    # 정상가 (MSRP)
    origin = _find_int_key(data, ("originPrice", "originalPrice", "retailPrice",
                                   "consumerPrice", "listPrice", "normalPrice"))
    if origin: detail["origin_price"] = origin

    # 할인율
    dc = _find_int_key(data, ("discountRate", "discountRatio", "dcRate"))
    if dc: detail["discount_rate"] = dc

    # 배송비
    free_key = _find_first_key(data, ("freeDlvr", "freeDlvrYn", "isFreeDelivery", "freeShipping"))
    ship_fee = _find_int_key(data, ("deliveryFee", "shippingFee", "dlvrAmt", "deliveryPrice"))
    if free_key in ("Y", "true", "1") or free_key is True:
        detail["shipping_fee"] = 0
    elif ship_fee is not None:
        detail["shipping_fee"] = ship_fee

    # 상품 상태
    cond = _find_first_key(data, ("goodsCondition", "condition", "usedStatus",
                                   "productStatus", "conditionCd"))
    if cond:
        detail["condition"] = "새상품" if str(cond).upper() in ("NEW", "N", "01") else cond

    # 재고
    stock = _find_int_key(data, ("stockCnt", "stockCount", "stock", "quantity",
                                  "qty", "totalStock", "remainQty"))
    if stock is not None: detail["stock"] = stock

    # 옵션
    opts_raw = _find_list_key(data, ("optionList", "options", "goodsOptions",
                                      "stockList", "itemOptions", "sizeList"))
    opts = _parse_options(opts_raw)
    if opts: detail["options"] = opts

    # ── 결제수단별 추가 할인 ─────────────────────────────────────────────────
    sell = detail.get("actual_price")
    pay_discs = _extract_pay_discs_from_json(data, sell)
    # API에서 못 찾으면 트렌비 상품 페이지 HTML 폴백
    if not pay_discs and sell:
        try:
            rh = requests.get(f"https://www.trenbe.com/good/{goodsno}",
                              timeout=5, headers={"User-Agent": _UA, "Accept-Language": "ko-KR,ko;q=0.9"})
            if rh.status_code == 200:
                pay_discs = _extract_pay_discs_from_html(rh.text, sell)
        except Exception:
            pass

    # price_info (공통 스키마)
    pi = _build_price_info(
        sell_price=sell,
        origin_price=detail.get("origin_price"),
        discount_rate=detail.get("discount_rate"),
        payment_discounts=pay_discs or None,
    )
    if pi: detail["price_info"] = pi

    if not detail.get("seller"): return None
    _DETAIL_CACHE[link] = detail
    return detail

def _fetch_mustit_detail(link):
    """머스트잇 product_detail 페이지에서 sellerId + 상세정보를 한 번에 추출.
    반환: dict(seller, condition, auth_status, shipping_fee, seller_grade,
               stock, actual_price, origin_price, product_no) | None
    """
    global _MUSTIT_LAST_REQ, _MUSTIT_BOT_COUNT, _MUSTIT_BOT_UNTIL

    decoded = unquote(link)
    m = re.search(r'/product_detail/(\d+)', decoded)
    if not m: return None
    pd_id = m.group(1)

    # detail 캐시 확인
    if link in _DETAIL_CACHE:
        return _DETAIL_CACHE[link]

    # ── Circuit Breaker: cooldown 중이면 즉시 포기 ──────────────────────
    now = time.time()
    if now < _MUSTIT_BOT_UNTIL:
        remaining = int(_MUSTIT_BOT_UNTIL - now)
        print(f"[mustit] circuit breaker 활성 — {remaining}초 후 재시도 가능, pd_id={pd_id} skip")
        return None

    # ── Rate Limiter: 요청 간 최소 간격 보장 (직렬화) ───────────────────
    with _MUSTIT_LOCK:
        now = time.time()
        wait = _MUSTIT_MIN_INTERVAL - (now - _MUSTIT_LAST_REQ)
        if wait > 0:
            time.sleep(wait)
        _MUSTIT_LAST_REQ = time.time()

    _headers = {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Ch-Ua": '"Google Chrome";v="124", "Chromium";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }

    html = ""

    # ── ① Playwright 우선: Cloudflare JS 챌린지 실제 통과 ────────────────
    if _HAS_PLAYWRIGHT:
        html = _fetch_mustit_html_playwright(pd_id) or ""

    # ── ② curl_cffi / requests 폴백 (Playwright 미설치 환경) ────────────
    if not html:
        try:
            target = f"https://m.web.mustit.co.kr/v2/m/product/product_detail/{pd_id}"
            referer = "https://m.web.mustit.co.kr/"
            sess = _get_mustit_session()
            if sess:
                r = sess.get(target, timeout=10,
                             headers={**_headers, "Referer": referer},
                             allow_redirects=True)
            else:
                r = requests.get(target, timeout=8,
                                 headers={**_headers, "Referer": referer},
                                 allow_redirects=True)

            final_url = str(getattr(r, 'url', target))
            if 'product_detail' not in final_url:
                print(f"[mustit] 리다이렉트 감지 pd_id={pd_id} → {final_url}")
                global _MUSTIT_SESSION
                _MUSTIT_SESSION = None
                sess2 = _get_mustit_session()
                if sess2:
                    r = sess2.get(target, timeout=10,
                                  headers={**_headers, "Referer": referer},
                                  allow_redirects=True)
                    final_url = str(getattr(r, 'url', target))

            if r.status_code == 200 and len(r.text) > 500 and 'product_detail' in final_url:
                html = r.text
                _MUSTIT_BOT_COUNT = 0
            else:
                _MUSTIT_BOT_COUNT += 1
                print(f"[mustit] HTTP 실패 pd_id={pd_id} status={r.status_code} final={final_url}")
                if _MUSTIT_BOT_COUNT >= _MUSTIT_BOT_LIMIT:
                    _MUSTIT_BOT_UNTIL = time.time() + _MUSTIT_BOT_COOLDOWN
        except Exception as e:
            print(f"[mustit] HTTP EXCEPTION pd_id={pd_id} err={e}")

    if not html:
        return None

    detail = {"product_no": pd_id}

    # ── ① __NEXT_DATA__ JSON (Next.js SSR) ─────────────────────────────
    nd = re.search(r'<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)</script>', html)
    if nd:
        try:
            _apply_mustit_json(json.loads(nd.group(1)), detail)
        except Exception:
            pass

    # ── ② RSC 플라이트 데이터 (이스케이프된 따옴표 패턴) ───────────────
    _apply_mustit_rsc(html, detail)

    # ── ③ 이미지 업로드 경로 폴백 (sellerId) ───────────────────────────
    if not detail.get("seller"):
        m2 = re.search(r'/lib/upload/product/([A-Za-z0-9_]+)/\d{4}/', html)
        if m2: detail["seller"] = m2.group(1).strip()

    if not detail.get("seller"):
        return None  # sellerId 없으면 유효하지 않은 상품

    # 정품 인증 bool 값 한글로 정규화
    auth = str(detail.get("auth_status", "")).strip().lower()
    if auth in ("y", "true", "1", "yes"):
        detail["auth_status"] = "인증완료"
    elif auth in ("n", "false", "0", "no"):
        detail["auth_status"] = "미인증"
    elif not auth:
        detail.pop("auth_status", None)

    _DETAIL_CACHE[link] = detail
    return detail


def _apply_mustit_json(data, out):
    """__NEXT_DATA__ JSON 구조에서 mustit 상세 필드를 재귀 탐색해 out에 채움."""
    if not out.get("seller"):
        v = _find_first_key(data, ("sellerId",))
        if v: out["seller"] = v

    if not out.get("condition"):
        v = _find_first_key(data, ("productStatus", "goodsCondition", "itemCondition",
                                   "condition", "productCondition"))
        if v: out["condition"] = v

    if not out.get("auth_status"):
        v = _find_first_key(data, ("certYn", "isAuthenticated", "authStatus",
                                   "certStatus", "authenticate", "isAuth"))
        if v: out["auth_status"] = str(v)

    if not out.get("shipping_fee"):
        v = _find_int_key(data, ("deliveryFee", "shippingFee", "deliveryPrice",
                                 "shipFee", "shipPrice"))
        if v is not None: out["shipping_fee"] = v

    if not out.get("seller_grade"):
        v = _find_first_key(data, ("sellerGrade", "memberGrade", "grade",
                                   "sellerLevel", "userGrade"))
        if v: out["seller_grade"] = v

    if not out.get("stock"):
        v = _find_int_key(data, ("stockCount", "stockCnt", "stock",
                                 "quantity", "qty", "remainQty"))
        if v is not None: out["stock"] = v

    if not out.get("actual_price"):
        v = _find_int_key(data, ("salePrice", "sellPrice", "sellingPrice", "goodsPrice"))
        if v: out["actual_price"] = v

    if not out.get("origin_price"):
        v = _find_int_key(data, ("orgPrice", "originPrice", "originalPrice",
                                 "regularPrice", "listPrice", "msrp"))
        if v: out["origin_price"] = v


# 할인 타입 → 한글 매핑 (서버/클라이언트 공유용)
_MUSTIT_DISCOUNT_KO = {
    "BARO":               "바로구매할인",
    "SPECIAL":            "스페셜쿠폰",
    "APP":                "앱전용할인",
    "NAVER":              "네이버 할인",
    "ITCOUPON":           "아이템쿠폰",
    "DANGOL":             "단골할인",
    "NEW_COUPON":         "핫딜쿠폰",
    "DIRECT_DISCOUNT":    "직접할인",
    "MEMBERSHIP_DISCOUNT":"멤버십할인",
    "SELLER_ITEM_COUPON": "판매자쿠폰",
    "DPS_DISCOUNT":       "DPS할인",
    "DIRECT_BUYER":       "직구매할인",
    "PERSONAL_MEDIA":     "인플루언서할인",
}
# 표시하지 않을 aggregate 타입
_MUSTIT_DISCOUNT_SKIP = {"ACCOUNT", "ACCOUNT_OUTPUT"}


def _extract_mustit_price_info(html):
    """RSC 2a 청크에서 priceGroup / discountGroup / jungsangPriceGroup 파싱.
    반환: dict 또는 None.
      {
        "sell_price":   int,          # 판매가 (NORMAL)
        "max_benefit":  int,          # 최대혜택가 (MAX_BENEFIT)
        "msrp":         int,          # 정상가 (jungsangPriceGroup.price)
        "msrp_rate":    int,          # 정상가 대비 할인율 %
        "discounts":    [             # apply=True 이고 price>0 인 항목 (leaf 할인)
          {"type": str, "name": str, "price": int, "pct": float},
          ...
        ],
        "naver_discount": int or 0,  # NAVER 할인액 (미적용 — 유입 시 추가)
      }
    """
    m = re.search(r'2a:\{([\s\S]+?)\}\}\}\s*\\n"\]\)', html)
    if not m:
        return None
    raw = '{' + m.group(1) + '}}}'
    try:
        data = json.loads(raw.replace('\\"', '"'))
    except Exception:
        return None
    pd = data.get("data", data)

    # 판매가 / 최대혜택가
    sell_price = None
    max_benefit = None
    for pg in pd.get("priceGroup", []):
        if pg.get("type") == "NORMAL":
            sell_price = pg.get("price")
        elif pg.get("type") == "MAX_BENEFIT":
            max_benefit = pg.get("price")

    if sell_price is None:
        return None  # 핵심 필드 없으면 skip

    # 정상가
    jp = pd.get("jungsangPriceGroup", {})
    msrp = jp.get("price")
    msrp_rate = jp.get("discountRatio")

    # 할인 목록 (apply=True + price>0 인 leaf 항목만)
    discounts = []
    naver_discount = 0
    for dg in pd.get("discountGroup", []):
        dtype = dg.get("type", "")
        dprice = dg.get("price") or 0
        if dtype in _MUSTIT_DISCOUNT_SKIP:
            continue
        if dtype == "NAVER":
            naver_discount = dprice
            continue
        if dg.get("apply") and dprice > 0:
            pct = round(dprice / sell_price * 100, 1) if sell_price else 0
            discounts.append({
                "type":  dtype,
                "name":  _MUSTIT_DISCOUNT_KO.get(dtype, dtype),
                "price": dprice,
                "pct":   pct,
            })

    # NAVER 할인 % 계산
    naver_pct = round(naver_discount / sell_price * 100, 1) if sell_price and naver_discount else 0

    return {
        "sell_price":     sell_price,
        "max_benefit":    max_benefit,
        "msrp":           msrp,
        "msrp_rate":      msrp_rate,
        "discounts":      discounts,
        "naver_discount": naver_discount,
        "naver_pct":      naver_pct,
    }


def _extract_mustit_options(html):
    """RSC itemOptions 배열 파싱.
    반환: [{"label": "색상/사이즈", "stock": int}, ...] 또는 None.
    """
    m = re.search(r'itemOptions\\":\[([\s\S]+?)\](?=,\\"priceInfo|\})', html)
    if not m:
        return None
    raw = '[' + m.group(1) + ']'
    try:
        opts = json.loads(raw.replace('\\"', '"').replace('\\\\', '\\'))
    except Exception:
        return None
    result = []
    for opt in opts:
        color = (opt.get('color') or '').strip()
        size  = (opt.get('size')  or '').strip()
        stock = opt.get('stock', 0)
        label = ' / '.join(x for x in [color, size] if x)
        if label:
            result.append({"label": label, "stock": stock})
    return result if result else None


def _apply_mustit_rsc(html, out):
    """RSC 플라이트 데이터(이스케이프된 JSON 따옴표 패턴)에서 mustit 상세 필드 추출."""

    def rstr(*keys):
        """문자열 필드 — \\?"key\\?":"value" 패턴."""
        for key in keys:
            m = re.search(r'\\?"' + re.escape(key) + r'\\?"\s*:\s*\\?"([^"\\]{1,80})', html)
            if m:
                v = m.group(1).strip()
                if v: return v
        return None

    def rint(*keys):
        """숫자 필드 — \\?"key\\?":12345 패턴."""
        for key in keys:
            m = re.search(r'\\?"' + re.escape(key) + r'\\?"\s*:\s*(\d+)', html)
            if m and int(m.group(1)) > 0:
                return int(m.group(1))
        return None

    def rbool(*keys):
        """bool 필드 — \\?"key\\?":(true|false|"Y"|"N") 패턴."""
        for key in keys:
            m = re.search(r'\\?"' + re.escape(key) + r'\\?"\s*:\s*(true|false|\\?"Y\\?"|\\?"N\\?")', html)
            if m: return m.group(1)
        return None

    # sellerId — 이스케이프/비이스케이프 모두 허용
    if not out.get("seller"):
        m = re.search(r'[\\]?"sellerId[\\]?"\s*:\s*[\\]?"([A-Za-z0-9_]+)', html)
        if m: out["seller"] = m.group(1).strip()

    # 상품 상태
    # ① HTML <dl><dt>상품상태</dt><dd>새상품</dd></dl> (가장 신뢰)
    if not out.get("condition"):
        m = re.search(r'상품상태</dt><dd>([^<]{1,20})</dd>', html)
        if m: out["condition"] = m.group(1).strip()
    # ② usedStatus 필드 — NEW → 새상품, 그 외 그대로
    if not out.get("condition"):
        m = re.search(r'[\\]?"usedStatus[\\]?"\s*:\s*[\\]?"([^"\\]{1,20})', html)
        if m:
            v = m.group(1).strip()
            out["condition"] = "새상품" if v.upper() == "NEW" else v
    # 중고 등급 (usedGrade: "S"/"A"/"B" 등)
    if not out.get("used_grade"):
        m = re.search(r'[\\]?"usedGrade[\\]?"\s*:\s*[\\]?"([^"\\null]{1,5})', html)
        if m:
            v = m.group(1).strip()
            if v and v.lower() != "null": out["used_grade"] = v

    # 배송비 — 숫자값 우선, FREE 텍스트 폴백
    if "shipping_fee" not in out:
        m = re.search(r'[\\]?"(?:deliveryFee|shippingFee|deliveryPrice|shipFee)[\\]?"\s*:\s*(\d+)', html)
        if m:
            out["shipping_fee"] = int(m.group(1))
        elif re.search(r'[\\]?"shippingFeeType[\\]?"\s*:\s*[\\]?"FREE', html):
            out["shipping_fee"] = 0

    # 재고
    if not out.get("stock"):
        m = re.search(r'[\\]?"stock[\\]?"\s*:\s*(\d+)', html)
        if m: out["stock"] = int(m.group(1))

    # 머스트잇 자체 판매가 (sellPrice / appPrice / sellPriceWeb)
    if not out.get("actual_price"):
        m = re.search(r'[\\]?"(?:sellPrice|appPrice|sellPriceWeb)[\\]?"\s*:\s*(\d{4,10})', html)
        if m: out["actual_price"] = int(m.group(1))

    # 정가 — normalPrice(MSRP) 우선, 없으면 시중가 순으로
    if not out.get("origin_price"):
        for pf in ("normalPrice", "sijoongPrice", "streetPrice",
                   "orgPrice", "originPrice", "regularPrice", "listPrice"):
            m = re.search(r'[\\]?"' + pf + r'[\\]?"\s*:\s*(\d{4,10})', html)
            if m:
                out["origin_price"] = int(m.group(1))
                break

    # 옵션 (색상/사이즈 + 재고)
    if "options" not in out:
        opts = _extract_mustit_options(html)
        if opts:
            out["options"] = opts

    # 가격 할인내역 (priceGroup / discountGroup / jungsangPriceGroup)
    if "price_info" not in out:
        pi = _extract_mustit_price_info(html)
        if pi:
            out["price_info"] = pi

def _fetch_lotteon_detail(link, naver_price=None):
    """롯데온 pbf API로 상세정보 일괄 수집 (SPA HTML 우회).
    naver_price: 네이버 검색에서 가져온 실제 판매가 폴백 (pbf API에 실제가 없을 때 사용)
    반환: dict(seller, condition, shipping_fee, stock, actual_price,
               origin_price, discount_rate, options, price_info) | None
    """
    m = re.search(r'[?&]sitmNo=([A-Z0-9_]+)', link, re.I)
    if m:
        sitm_no = m.group(1)
    else:
        m = re.search(r'/p/product/([A-Z0-9]+)', link, re.I)
        if not m: return None
        pd_no = m.group(1)
        sitm_no = f"{pd_no}_{pd_no}"

    if link in _DETAIL_CACHE:
        return _DETAIL_CACHE[link]

    api_url = (f"https://pbf.lotteon.com/product/v2/detail/search/base/sitm/{sitm_no}"
               f"?sitmNo={sitm_no}&ch_no=100065&ch_dtl_no=1000030"
               f"&entryPoint=pcs&srchOnlyThisItm=true&isNotContainOptMapping=true")
    try:
        r = requests.get(api_url, timeout=2.5,
                         headers={"User-Agent": _UA,
                                  "Accept": "application/json",
                                  "Referer": "https://www.lotteon.com/"})
        if r.status_code != 200: return None
        data = r.json()
    except Exception:
        return None

    detail = {}

    # 판매자
    seller = _find_first_key(data, ("sellerNm", "trNm", "sellerName", "sellerNickName"))
    if seller: detail["seller"] = seller.strip()[:50]

    # 롯데온 base API: slPrc = 정상가(list price).
    # 실제 판매가는 favorBox/benefits API의 totAmt에서 가져옴.
    sl_prc_base = _find_int_key(data, ("slPrc",)) or 0

    # favorBox/benefits POST API 파라미터 추출
    _spd_no = sitm_no.split("_")[0]
    _tr_grp_cd  = _find_first_key(data, ("trGrpCd",))  or "SR"
    _tr_no      = _find_first_key(data, ("trNo",))     or ""
    _ctrt_typ   = _find_first_key(data, ("ctrtTypCd",)) or "A"
    _scat_no    = _find_first_key(data, ("scatNo",))   or ""
    _brd_no     = _find_first_key(data, ("brdNo",))    or ""
    _sfco_mrgn  = _find_int_key(data, ("sfcoPdMrgnRt",)) or 0
    _dv_cst     = _find_int_key(data, ("dvCst",))      or 0
    _max_pur    = _find_int_key(data, ("maxPurQty",))  or 999999
    _stk_mgt    = _find_first_key(data, ("stkMgtYn",)) or "Y"
    _mall_no    = _find_first_key(data, ("mallNo",))   or "1"

    # 할인율
    dc = _find_int_key(data, ("dcRate", "discountRate", "discRate", "discountRatio",
                               "immdDcRt", "dcRt"))
    if dc: detail["discount_rate"] = dc

    # 배송비
    free_dlvr = _find_first_key(data, ("freeDlvrYn", "freeDlvr", "freeShippingYn"))
    dlvr_amt = _find_int_key(data, ("dlvrAmt", "deliveryFee", "shippingFee", "deliveryPrice"))
    if free_dlvr in ("Y", "1", "true") or dlvr_amt == 0:
        detail["shipping_fee"] = 0
    elif dlvr_amt is not None:
        detail["shipping_fee"] = dlvr_amt

    # 상품 상태
    cond = _find_first_key(data, ("goodsCondition", "condition", "usedStatus", "goodsCd"))
    if cond:
        detail["condition"] = "새상품" if str(cond).upper() in ("NEW","N","01","10","ITMTYPCD01") else cond

    # 재고 (stkQty = stckInfo.stkQty 포함)
    stock = _find_int_key(data, ("stockQty", "stkQty", "rtlStockQty", "stock",
                                  "stockCnt", "quantity", "remainQty"))
    if stock is not None: detail["stock"] = stock

    # 옵션 — 롯데온 전용: optionList 중첩 구조
    # data.data.optionInfo.optionList = [{title:"의류 사이즈", options:[{label:"IT 46", disabled:False}, ...]}]
    # disabled=True → 품절(stock=0), False → 재고있음(stock=1)
    lotteon_opts = []
    try:
        opt_groups = (data.get("data") or {}).get("optionInfo", {}).get("optionList") or []
        for grp in opt_groups:
            if not isinstance(grp, dict): continue
            for opt in (grp.get("options") or []):
                if not isinstance(opt, dict): continue
                lbl = (opt.get("label") or "").strip()
                if not lbl: continue
                disabled = bool(opt.get("disabled", False))
                lotteon_opts.append({"label": lbl, "stock": 0 if disabled else 1})
    except Exception:
        pass

    if lotteon_opts:
        detail["options"] = lotteon_opts
    else:
        # 폴백: 범용 파싱 (단일 옵션 구조 대비)
        opts_raw = _find_list_key(data, ("optionList", "optList", "options",
                                          "sitmOptionList", "itemOptionList"))
        opts = _parse_options(opts_raw)
        if opts: detail["options"] = opts

    # ── favorBox/benefits API: 실제 판매가 + 결제수단별 할인 ─────────────────
    # POST /product/v2/extlmsa/promotion/favorBox/benefits
    # totAmt = 즉시할인 적용 후 실제 판매가 (비로그인 기준)
    # discountGroups = 추가 결제 할인 목록 (카드/쿠폰 등)
    _lo_hdrs = {"User-Agent": _UA, "Accept": "application/json",
                "Referer": "https://www.lotteon.com/"}
    pay_discs = []
    _favor_ok = False

    if sl_prc_base and _tr_no:
        from datetime import datetime as _dt
        try:
            _now_str = _dt.now().strftime("%Y%m%d%H%M%S")
            _favor_payload = {
                "spdNo": _spd_no, "sitmNo": sitm_no,
                "trGrpCd": _tr_grp_cd, "trNo": _tr_no,
                "lrtrNo": "", "strCd": "", "ctrtTypCd": _ctrt_typ,
                "slPrc": sl_prc_base, "slQty": 1,
                "scatNo": _scat_no, "brdNo": _brd_no,
                "sfcoPdMrgnRt": _sfco_mrgn, "sfcoPdLwstMrgnRt": 0,
                "afflPdMrgnRt": None, "afflPdLwstMrgnRt": None,
                "pcsLwstMrgnRt": 0, "infwMdiaCd": "PC",
                "chCsfCd": "PA", "chTypCd": "PA08",
                "chNo": "100065", "chDtlNo": "1000030",
                "aplyStdDttm": _now_str,
                "cartDvsCd": "01", "thdyPdYn": "N",
                "dvCst": _dv_cst, "fprdDvPdYn": "N",
                "discountApplyProductList": [],
                "maxPurQty": _max_pur, "stkMgtYn": _stk_mgt,
                "screenType": "PRODUCT", "dmstOvsDvDvsCd": "DMST",
                "dvPdTypCd": "GNRL", "dvCstStdQty": 0,
                "aplyBestPrcChk": "N", "pyMnsExcpLst": ["21"],
                "mallNo": _mall_no,
            }
            _fr = requests.post(
                "https://pbf.lotteon.com/product/v2/extlmsa/promotion/favorBox/benefits",
                json=_favor_payload, timeout=2.5, headers=_lo_hdrs,
            )
            if _fr.status_code == 200:
                _fd = (_fr.json().get("data") or {})
                _tot_amt = _fd.get("totAmt")
                if _tot_amt and 0 < _tot_amt < sl_prc_base:
                    detail["actual_price"] = _tot_amt
                    _favor_ok = True
                # 정상가 = slPrc (base API)
                if sl_prc_base and _tot_amt and sl_prc_base > _tot_amt:
                    detail["origin_price"] = sl_prc_base
                # 추가 할인 (상품할인 prTypCd=PRD_DC는 totAmt에 이미 반영됨 → 제외)
                # 최대혜택가 = totAmt에서 가장 큰 단일 결제할인 1개 적용
                # 「패션」「럭셔리」 등 카테고리 레벨명보다 구체적 카드/페이 명칭 우선
                import re as _re
                _CATEG_PAT = _re.compile(r'^「[^」]*」')  # 「패션」 결제할인 등 제외
                _best_nm, _best_amt = "", 0
                _categ_nm, _categ_amt = "", 0  # 카테고리명 후보 (카드명 없을 때 폴백)
                for _grp in (_fd.get("discountGroups") or []):
                    for _prm in (_grp.get("discountApplyPromotionList") or []):
                        if _prm.get("prTypCd") == "PRD_DC":
                            continue  # 이미 totAmt에 포함된 즉시할인
                        _nm  = (_prm.get("prNm") or "").strip()
                        _amt = _prm.get("dcAmt") or 0
                        if not _nm or not _amt:
                            continue
                        if _CATEG_PAT.match(_nm):
                            # 카테고리명: 더 나은 카드명이 없으면 폴백으로 사용
                            if _amt > _categ_amt:
                                _categ_amt = _amt
                                _categ_nm  = _nm
                        else:
                            if _amt > _best_amt:
                                _best_amt = _amt
                                _best_nm  = _nm
                # 카드/페이 명칭이 없으면 카테고리명 폴백
                if _best_amt == 0 and _categ_amt > 0:
                    _best_nm, _best_amt = _categ_nm, _categ_amt
                if _best_amt > 0:
                    pay_discs.append({"name": _best_nm, "price": _best_amt})
        except Exception:
            pass

    # favorBox 실패 시: 가격 정보 비워둠 (폴백 없음)
    # — timeout/오류 시 naver_price나 slPrc로 채우지 않음
    if not _favor_ok:
        pass  # actual_price, pay_discs 모두 미설정 → price_info 없음

    # price_info
    pi = _build_price_info(
        sell_price=detail.get("actual_price"),
        origin_price=detail.get("origin_price"),
        discount_rate=detail.get("discount_rate"),
        payment_discounts=pay_discs or None,
    )
    if pi: detail["price_info"] = pi

    if not detail.get("seller"): return None
    _DETAIL_CACHE[link] = detail
    return detail

def _scrape_ssg(html):
    # ① "판매자스토어" 접미사 — SSG 마켓플레이스 셀러 식별자 (가장 신뢰 가능)
    #    예: <span>EURO AVENUE 판매자스토어</span>
    m = re.search(r'>\s*([^<>]{2,60}?)\s*판매자스토어\s*<', html)
    if m: return m.group(1).strip()
    # ② <strong class="tit">XXX 판매자스토어 상품</strong>
    m = re.search(r'<strong[^>]*class="tit"[^>]*>\s*([^<]{2,60}?)\s*판매자스토어\s*상품\s*</strong>', html)
    if m: return m.group(1).strip()
    # ③ data-seller-name 속성
    m = re.search(r'data-seller(?:-name)?\s*=\s*["\']([^"\']{2,60})["\']', html)
    if m: return m.group(1).strip()
    # ④ 판매자 레이블 바로 뒤 텍스트 패턴 (SSG 신규 구조)
    m = re.search(r'판매자\s*</[^>]+>\s*<[^>]+>\s*([^<]{2,60}?)\s*<', html)
    if m: return m.group(1).strip()
    # ⑤ class에 seller/vendor/shop 포함하는 요소
    m = re.search(r'class="[^"]*(?:seller|vendor|shop)[^"]*"[^>]*>\s*([^<]{2,60}?)\s*<', html, re.I)
    if m:
        candidate = m.group(1).strip()
        if 2 < len(candidate) < 60 and '판매자스토어' not in candidate:
            return candidate
    # ⑥ JSON 패턴들 — 다양한 키 이름 지원
    for pat in (
        r'"sellerNick"\s*:\s*"([^"]{2,60})"',
        r'"sellerNm"\s*:\s*"([^"]{2,60})"',
        r'"sellerName"\s*:\s*"([^"]{2,60})"',
        r'"sellerShopNm"\s*:\s*"([^"]{2,60})"',
        r'"shopNm"\s*:\s*"([^"]{2,60})"',
        r'"storeName"\s*:\s*"([^"]{2,60})"',
        r'"vendorNm"\s*:\s*"([^"]{2,60})"',
        r'"vendorName"\s*:\s*"([^"]{2,60})"',
        r'"brandNm"\s*:\s*"([^"]{2,60})"',
        r'"partnerName"\s*:\s*"([^"]{2,60})"',
        r'"sellerBrandNm"\s*:\s*"([^"]{2,60})"',
        r'"mallName"\s*:\s*"([^"]{2,60})"',
    ):
        m = re.search(pat, html)
        if m:
            v = m.group(1).strip()
            # 완전히 generic한 SSG 자체몰 이름만 제외 (신세계몰은 유효한 셀러)
            if v and v.upper() not in ('SSG.COM', 'SSG'):
                return v
    # ⑦ siteNo로 SSG 채널 추론 (신세계몰=1006, SSG마켓=6004 등)
    m = re.search(r'siteNo[="\s:]+(\d+)', html)
    if m:
        site_map = {'1006': '신세계몰', '1003': '이마트몰', '6004': 'SSG마켓',
                    '1041': '신세계TV쇼핑', '1048': 'S.I.빌리지'}
        label = site_map.get(m.group(1))
        if label: return label
    return None

_SSG_SESSION = requests.Session()
_SSG_SESSION.headers.update({
    "User-Agent": _UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
})

# SSG rate limiter: 동시 요청 1개 + 최소 0.2초 간격 유지
import threading as _threading
_SSG_RATE_LOCK = _threading.Lock()
_SSG_LAST_REQ_TIME = [0.0]   # list: 스레드 간 공유 가능한 mutable
_SSG_MIN_INTERVAL = 0.4      # 초 단위. 0.2→0.4 (429 차단 방지)

def _ssg_get(url, **kwargs):
    """SSG 전용 rate-limited GET. 최소 0.2초 간격을 강제."""
    import time as _t
    with _SSG_RATE_LOCK:
        elapsed = _t.time() - _SSG_LAST_REQ_TIME[0]
        if elapsed < _SSG_MIN_INTERVAL:
            _t.sleep(_SSG_MIN_INTERVAL - elapsed)
        _SSG_LAST_REQ_TIME[0] = _t.time()
    return _SSG_SESSION.get(url, **kwargs)

# SSG siteNo → 채널명 매핑 (공통 상수)
_SSG_SITE_MAP = {'1006': '신세계몰', '1003': '이마트몰', '6004': 'SSG마켓',
                 '1041': '신세계TV쇼핑', '1048': 'S.I.빌리지'}
_SSG_SUBDOMAIN_MAP = {"shinsegaemall": "신세계몰", "emart.ssg": "이마트몰",
                      "tvshop.ssg": "신세계TV쇼핑", "sivillage": "S.I.빌리지"}

def _ssg_seller_from_url(link):
    """URL만으로 SSG 채널명 추출. HTTP 없음.
    siteNo=6004(SSG마켓)는 오픈마켓이므로 제외 — 실제 판매자를 AJAX/HTML에서 찾아야 함."""
    _ll = link.lower()
    for _sub, _nm in _SSG_SUBDOMAIN_MAP.items():
        if _sub in _ll:
            return _nm
    _m = re.search(r'[?&]siteNo=(\d+)', link)
    if _m and _m.group(1) != "6004":   # 6004=SSG마켓(오픈마켓) 제외
        return _SSG_SITE_MAP.get(_m.group(1))
    return None

def _fetch_ssg_detail(link):
    """SSG 상품 상세정보 수집.
    순서: ① URL fast path (HTTP 없음, 직매입 채널만) → ② AJAX getItemDtlInfo (rate-free) →
          ③ HTML fetch (rate-limited, 필요 시만) → ④ 가격·할인 조합
    반환: dict(seller, shipping_fee, stock, actual_price,
               origin_price, discount_rate, price_info) | None
    """
    if link in _DETAIL_CACHE:
        return _DETAIL_CACHE[link]

    detail = {}
    html   = ""

    # ── ① URL fast path: 서브도메인·직매입 siteNo로 판매자 즉시 확정 (HTTP 0건) ────
    # SSG마켓(6004)은 오픈마켓이므로 제외 → AJAX/HTML에서 실제 판매자 탐색
    _url_seller = _ssg_seller_from_url(link)
    if _url_seller:
        detail["seller"] = _url_seller

    # itemId / siteNo 파라미터 추출 (AJAX에 필요)
    _m_id  = re.search(r'[?&]itemId=(\d+)', link)
    _m_sno = re.search(r'[?&]siteNo=(\d+)', link)

    # ── ② AJAX getItemDtlInfo (rate-limiter 없음, 빠름) ─────────────────────
    # 직매입 채널(신세계몰/이마트몰 등)은 URL에서 이미 확보 → 스킵
    # SSG마켓(오픈마켓) 및 siteNo 없는 항목은 항상 실행
    if _m_id and not detail.get("seller"):
        _item_id  = _m_id.group(1)
        _site_no  = _m_sno.group(1) if _m_sno else "6004"
        _ajax_hdrs = {
            "Referer": link,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, */*; q=0.01",
        }
        try:
            _ar = _SSG_SESSION.get(
                "https://www.ssg.com/item/ajax/getItemDtlInfo.ssg",
                params={"itemId": _item_id, "siteNo": _site_no},
                timeout=3, headers=_ajax_hdrs)
            if _ar.status_code == 200:
                _ad = _ar.json()
                # seller
                if not detail.get("seller"):
                    _sel = _find_first_key(_ad, ("sellerNick","sellerNm","sellerName",
                                                  "storeName","shopNm","vendorNm"))
                    if _sel and _sel.upper() not in ('SSG.COM','SSG'):
                        detail["seller"] = _sel.strip()[:50]
                    if not detail.get("seller"):
                        _sn_v = _find_first_key(_ad, ("siteNo",))
                        if not _sn_v:
                            _sn_m = re.search(r'"siteNo"\s*:\s*"?(\d+)', str(_ad))
                            _sn_v = _sn_m.group(1) if _sn_m else None
                        if _sn_v and str(_sn_v).isdigit():
                            _lbl = _SSG_SITE_MAP.get(str(_sn_v))
                            if _lbl: detail["seller"] = _lbl
                # price from AJAX response
                _ap = _find_int_key(_ad, ("salePrice","sellPrice","finalPrice",
                                          "price","discountedPrice"))
                if _ap: detail["actual_price"] = _ap
                _op = _find_int_key(_ad, ("normalPrice","originPrice","consumerPrice",
                                          "listPrice","orgPrice"))
                if _op: detail["origin_price"] = _op
                _dc = _find_int_key(_ad, ("discountRate","dcRate","discountRatio"))
                if _dc: detail["discount_rate"] = _dc
        except Exception:
            pass

    # ── ③ HTML fetch (rate-limited 0.2s) — seller 또는 actual_price 미확보 시 ──
    need_html = not detail.get("seller") or not detail.get("actual_price")
    if need_html:
        try:
            r = _ssg_get(link, timeout=4, allow_redirects=True,
                         headers={"Referer": "https://search.naver.com/"})
            blocked = (r.status_code == 429 or "g-recaptcha" in (r.text or "")[:500])
            if not blocked and r.status_code == 200:
                html = r.text or ""
        except Exception:
            pass

    if html:
        # 판매자 (HTML 패턴)
        if not detail.get("seller"):
            _hs = _scrape_ssg(html)
            if _hs: detail["seller"] = _hs.strip()[:50]

        # __NEXT_DATA__ JSON
        nd = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>([\s\S]*?)</script>', html)
        if nd:
            try:
                data = json.loads(nd.group(1))
                if not detail.get("actual_price"):
                    sell = _find_int_key(data, ("salePrice","sellPrice","finalPrice",
                                                "price","discountedPrice"))
                    if sell: detail["actual_price"] = sell
                if not detail.get("origin_price"):
                    origin = _find_int_key(data, ("normalPrice","originPrice","consumerPrice",
                                                   "listPrice","orgPrice"))
                    if origin: detail["origin_price"] = origin
                if not detail.get("discount_rate"):
                    dc = _find_int_key(data, ("discountRate","dcRate","discountRatio"))
                    if dc: detail["discount_rate"] = dc
                ship = _find_int_key(data, ("deliveryFee","shippingFee","dlvrAmt"))
                free_s = _find_first_key(data, ("freeShipping","freeDlvrYn","isFreeDlvr","isFreeDelivery"))
                if free_s in ("Y","true","1") or free_s is True or ship == 0:
                    detail["shipping_fee"] = 0
                elif ship: detail["shipping_fee"] = ship
                stock = _find_int_key(data, ("stockCnt","stockCount","stock","quantity","remainQty"))
                if stock is not None: detail["stock"] = stock
                if not detail.get("seller"):
                    sel = _find_first_key(data, (
                        "sellerNick","sellerNm","sellerName","storeName",
                        "sellerShopNm","shopNm","vendorNm","vendorName",
                        "brandNm","partnerName","sellerBrandNm","mallName",
                    ))
                    if sel and sel.upper() not in ('SSG.COM','SSG'):
                        detail["seller"] = sel.strip()[:50]
            except Exception:
                pass

        # regex 폴백
        if not detail.get("actual_price"):
            m = re.search(r'"salePrice"\s*:\s*(\d+)', html)
            if m: detail["actual_price"] = int(m.group(1))
        if not detail.get("origin_price"):
            m = re.search(r'"normalPrice"\s*:\s*(\d+)', html)
            if m: detail["origin_price"] = int(m.group(1))
        if "shipping_fee" not in detail and re.search(r'무료\s*배송', html):
            detail["shipping_fee"] = 0

        # HTML siteNo 폴백 (판매자 최종)
        if not detail.get("seller"):
            _m2 = (re.search(r'"siteNo"\s*:\s*"?(\d+)', html)
                   or re.search(r'siteNo[=:\s"]+(\d+)', html))
            if _m2:
                _lbl = _SSG_SITE_MAP.get(_m2.group(1))
                if _lbl: detail["seller"] = _lbl

    # ── ④ 결제수단별 할인 (HTML 보유 시) + price_info ─────────────────────────
    pay_discs_ssg = _extract_pay_discs_from_html(html, detail.get("actual_price"))
    pi = _build_price_info(
        sell_price=detail.get("actual_price"),
        origin_price=detail.get("origin_price"),
        discount_rate=detail.get("discount_rate"),
        payment_discounts=pay_discs_ssg or None,
    )
    if pi: detail["price_info"] = pi

    # 셀러를 끝내 못 찾은 경우: None 반환
    if not detail.get("seller"):
        return None

    _DETAIL_CACHE[link] = detail
    return detail


def _extract_ss_product_no(link):
    """SmartStore/brand.naver URL에서 products/{no} 파싱."""
    for pat in (
        r'smartstore\.naver\.com/[^/?#]+/products/(\d+)',
        r'brand\.naver\.com/[^/?#]+/products/(\d+)',
    ):
        m = re.search(pat, link or "")
        if m:
            return m.group(1)
    return None

_SS_HDRS = {
    "User-Agent": _UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://smartstore.naver.com/",
}

def _fetch_smartstore_detail(link, naver_price=None, catalog_id=None):
    """스마트스토어 상품 정보 수집.
    접근 우선순위:
      ① SmartStore 내부 JSON API (429 rate-limited 될 수 있음)
      ② Naver Shopping 카탈로그 페이지 __NEXT_DATA__ (search.shopping.naver.com)
      ③ naver_price 폴백 (네이버 검색 API lprice)
    naver_price : 네이버 Open API lprice
    catalog_id  : 네이버 쇼핑 productId (카탈로그 페이지 조회용)
    반환: dict(seller?, actual_price?, origin_price?, price_info?) — seller 없어도 반환 가능
    """
    if link in _DETAIL_CACHE:
        return _DETAIL_CACHE[link]

    detail = {}

    # ① 스토어명 (URL 기반, HTTP 불필요)
    seller = _scrape_smartstore_from_url(link)
    if seller:
        detail["seller"] = seller.strip()[:50]

    product_no = _extract_ss_product_no(link)
    pay_discs = []

    # ② SmartStore 내부 JSON API (v1/v2)
    for _api_path in filter(None, [
        f"https://smartstore.naver.com/i/v1/products/{product_no}" if product_no else None,
        f"https://smartstore.naver.com/i/v2/channels/main/products/{product_no}" if product_no else None,
    ]):
        try:
            _r = requests.get(_api_path, timeout=3, headers=_SS_HDRS)
            if _r.status_code != 200:
                break  # 429/403 이면 이 방식 포기
            _data = _r.json()
            sp = _find_int_key(_data, (
                "salePrice", "discountedSalePrice", "discountedPrice", "sellPrice", "purchasePrice",
            ))
            if sp: detail["actual_price"] = sp
            op = _find_int_key(_data, (
                "originalPrice", "consumerPrice", "basePrice", "listPrice", "regularPrice",
            ))
            if op and (not sp or op > sp): detail["origin_price"] = op
            dc = _find_int_key(_data, ("discountRate", "discountRatio", "dcRate"))
            if dc: detail["discount_rate"] = dc
            if not detail.get("seller"):
                sel = _find_first_key(_data, ("storeName", "channelName", "sellerName", "shopName"))
                if sel: detail["seller"] = str(sel).strip()[:50]
            _coup = _find_int_key(_data, (
                "couponBenefitAmount", "maxCouponBenefitAmount", "couponDiscountAmount", "couponBenefit",
            ))
            if _coup and _coup > 0: pay_discs.append({"name": "쿠폰 혜택", "price": _coup})
            _payp = _find_int_key(_data, (
                "naverPayPointAmount", "accumulationAmount", "pointBenefitAmount",
                "naverPayPoint", "pointAmount", "mileage", "accMileage",
            ))
            if _payp and _payp > 0: pay_discs.append({"name": "네이버페이 포인트", "price": _payp})
            _mem = _find_int_key(_data, (
                "naverMembershipDiscountAmount", "membershipDiscountAmount",
                "naverMembershipBenefitAmount", "membershipBenefitAmount", "membershipBenefit",
            ))
            if _mem and _mem > 0: pay_discs.append({"name": "네이버플러스 멤버십", "price": _mem})
            break
        except Exception:
            break

    # ③ Naver Shopping 카탈로그 페이지 (__NEXT_DATA__)
    # SmartStore API가 429/blocked 이면 여기서 가격/혜택 시도
    if not detail.get("actual_price"):
        for _cid in dict.fromkeys(filter(None, [catalog_id, product_no])):
            try:
                _cat_url = f"https://search.shopping.naver.com/catalog/{_cid}"
                _cr = requests.get(_cat_url, timeout=5, headers={
                    "User-Agent": _UA,
                    "Accept": "text/html,application/xhtml+xml",
                    "Accept-Language": "ko-KR,ko;q=0.9",
                })
                if _cr.status_code != 200:
                    continue
                _nd = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>([\s\S]*?)</script>', _cr.text)
                if not _nd:
                    continue
                _cd = json.loads(_nd.group(1))
                # 판매가
                sp2 = _find_int_key(_cd, (
                    "lowestPrice", "salePrice", "sellPrice", "price",
                    "lowPrice", "discountedPrice",
                ))
                if sp2: detail["actual_price"] = sp2
                # 정상가
                op2 = _find_int_key(_cd, (
                    "normalPrice", "originalPrice", "consumerPrice", "highPrice",
                    "basePrice", "listPrice",
                ))
                if op2 and (not sp2 or op2 > sp2): detail["origin_price"] = op2
                # 할인율
                dc2 = _find_int_key(_cd, ("discountRate", "dcRate", "discountRatio"))
                if dc2: detail["discount_rate"] = dc2
                # 스토어명 보완
                if not detail.get("seller"):
                    sel2 = _find_first_key(_cd, ("mallName", "storeName", "channelName", "sellerName"))
                    if sel2: detail["seller"] = str(sel2).strip()[:50]
                # 쿠폰
                _coup2 = _find_int_key(_cd, (
                    "couponBenefitAmount", "maxCouponBenefitAmount",
                    "couponDiscountAmount", "maxCouponPrice",
                ))
                if _coup2 and _coup2 > 0: pay_discs.append({"name": "쿠폰 혜택", "price": _coup2})
                # 네이버페이 포인트
                _payp2 = _find_int_key(_cd, (
                    "naverPayAccumulationAmount", "naverPayPointAmount",
                    "accumulationAmount", "pointBenefitAmount",
                ))
                if _payp2 and _payp2 > 0: pay_discs.append({"name": "네이버페이 포인트", "price": _payp2})
                # 멤버십
                _mem2 = _find_int_key(_cd, (
                    "naverMembershipDiscountAmount", "membershipDiscountAmount",
                    "naverMembershipBenefitAmount", "membershipBenefitAmount",
                ))
                if _mem2 and _mem2 > 0: pay_discs.append({"name": "네이버플러스 멤버십", "price": _mem2})
                break
            except Exception:
                continue

    # ④ naver_price 폴백 (OpenAPI lprice)
    if not detail.get("actual_price") and naver_price:
        detail["actual_price"] = naver_price

    # ⑤ 최대 단일 혜택 선택 (LotteOn과 동일 방식)
    _best_pay = None
    if pay_discs:
        _best = max(pay_discs, key=lambda x: x["price"])
        _best_pay = [_best]

    pi = _build_price_info(
        sell_price=detail.get("actual_price"),
        origin_price=detail.get("origin_price"),
        discount_rate=detail.get("discount_rate"),
        payment_discounts=_best_pay,
    )
    if pi: detail["price_info"] = pi

    # seller 없어도 price_info 있으면 저장 (seller는 scrape_seller_id에서 폴백 처리)
    if not detail.get("seller") and not detail.get("price_info"):
        return None
    _DETAIL_CACHE[link] = detail
    return detail


def _scrape_lotteon(html):
    for pat in (
        r'"sellerNm"\s*:\s*"([^"]+)"',
        r'"sellerName"\s*:\s*"([^"]+)"',
        r'"sellerNickName"\s*:\s*"([^"]+)"',
        r'class="[^"]*seller[^"]*"[^>]*>\s*([^<\n]{2,40})',
    ):
        m = re.search(pat, html)
        if m: return m.group(1).strip()
    return None

_SS_GENERIC_SEGMENTS = {"main", "home", "search", "window", "category", "my", "n", "products"}

def _scrape_smartstore_from_url(link):
    """스마트스토어 URL의 첫 경로 세그먼트가 실제 스토어명이면 반환, 일반 경로면 None."""
    for pat in (
        r'smartstore\.naver\.com/([^/?#]+)',
        r'brand\.naver\.com/([^/?#]+)',
    ):
        m = re.search(pat, link)
        if m:
            seg = m.group(1)
            if seg and seg.lower() not in _SS_GENERIC_SEGMENTS:
                return seg
    return None

def scrape_seller_id(item):
    plat = item.get("platform")
    link = item.get("link", "") or ""
    mall = (item.get("mallName") or "").strip()
    if not link: return None
    # 머스트잇: product_detail 페이지에서 sellerId + 상세정보 동시 추출
    # _DETAIL_CACHE를 통해 재요청 없이 detail dict 재사용.
    # ※ _SELLER_CACHE early-return을 하지 않는 이유: 새로운 item dict에
    #   _mustit_* 상세 필드를 매번 적용해야 하므로 cache hit 시에도 진입.
    if plat == "머스트잇":
        # product_no는 detail fetch 성패와 무관하게 항상 링크에서 추출 저장.
        # (바로가기 버튼의 naver_session URL 구성에 필요)
        _decoded_link = unquote(link)
        _pd_m = re.search(r'/product_detail/(\d+)', _decoded_link)
        if _pd_m:
            item["_mustit_product_no"] = _pd_m.group(1)

        detail = _DETAIL_CACHE.get(link) or _fetch_mustit_detail(link)
        if detail:
            sid = detail.get("seller", "")
            if sid:
                sid = re.sub(r"\s+", " ", sid).strip()[:50]
                _SELLER_CACHE[link] = sid
                # 상세 필드를 item에 직접 기록 (_mustit_ 접두어)
                for k in ("condition", "used_grade", "auth_status", "shipping_fee",
                          "seller_grade", "stock", "actual_price",
                          "origin_price", "product_no", "options", "price_info"):
                    v = detail.get(k)
                    if v is not None:
                        item[f"_mustit_{k}"] = v
                return sid
        return None
    # ── 공통 detail 필드 저장 헬퍼 ─────────────────────────────
    def _store_det(det):
        """detail dict → item에 _det_* 접두어로 저장."""
        for k in ("condition","shipping_fee","stock","actual_price",
                  "origin_price","discount_rate","options","price_info"):
            v = det.get(k)
            if v is not None:
                item[f"_det_{k}"] = v

    # 트렌비: displaygateway API로 상세정보 전체 수집
    if plat == "트렌비":
        detail = _fetch_trenbe_detail(link)
        if detail:
            sid = detail.get("seller", "")
            if sid:
                sid = re.sub(r"\s+", " ", sid).strip()[:50]
                _SELLER_CACHE[link] = sid
                _store_det(detail)
                return sid
        return None

    # 롯데온: pbf API로 상세정보 전체 수집
    if plat == "롯데온":
        # item["price"] = Naver lprice (search_by_platform에서 price로 저장됨)
        _naver_price = int(item.get("price", 0) or item.get("lprice", 0) or 0)
        detail = _fetch_lotteon_detail(link, naver_price=_naver_price)
        if detail:
            sid = detail.get("seller", "")
            if sid:
                sid = re.sub(r"\s+", " ", sid).strip()[:50]
                _SELLER_CACHE[link] = sid
                _store_det(detail)
                return sid
        return None

    # SSG: SSR HTML + __NEXT_DATA__ + AJAX 파싱
    if plat == "SSG":
        detail = _fetch_ssg_detail(link)
        if detail:
            sid = detail.get("seller", "")
            if sid:
                sid = re.sub(r"\s+", " ", sid).strip()[:50]
                _SELLER_CACHE[link] = sid
                _store_det(detail)
                return sid
        # _fetch_ssg_detail 실패 시 — mallName을 쓰지 않음
        # mallName은 채널명(신세계몰/이마트몰)이지 실제 판매자가 아닐 수 있음
        # URL 서브도메인 기반 채널명만 허용 (이미 _fetch_ssg_detail ④에서 처리됨)
        return None

    # 스마트스토어: HTTP 전면 차단(429/418/490) — HTTP 시도 없이 즉시 처리
    if plat == "스마트스토어":
        if link in _SELLER_CACHE:
            # 캐시 히트: price_info도 이미 item에 있으므로 즉시 반환
            return _SELLER_CACHE[link]
        # naver lprice로 price_info 바로 생성 (HTTP 0건)
        _np = int(item.get("price", 0) or 0)
        if _np > 0:
            _pi = _build_price_info(sell_price=_np)
            if _pi:
                item["_det_actual_price"] = _np
                item["_det_price_info"]   = _pi
        # 판매자: URL → mallName 순
        sid = _scrape_smartstore_from_url(link) or mall or None
        if sid:
            sid = re.sub(r"\s+", " ", sid).strip()[:50]
            _SELLER_CACHE[link] = sid
        return sid

    return None

def enrich_sellers_in_place(items, max_workers=15):
    """items 각각에 대해 scrape_seller_id로 판매자 ID 추정 후 'seller' 필드를 덮어씀.
    실패 시 기존 mallName 기반 seller 값을 유지.
    반환: dict(total_sec, per_platform)"""
    if not items:
        return {"total_sec": 0, "per_platform": {}}
    import time as _t2
    _t_start = _t2.time()
    _plat_times = {}   # plat → list of elapsed seconds
    def _timed_scrape(it):
        _s = _t2.time()
        sid = scrape_seller_id(it)
        _e = _t2.time()
        plat = it.get("platform", "?")
        _plat_times.setdefault(plat, []).append(round(_e - _s, 2))
        return sid
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_timed_scrape, it): it for it in items}
        for f, it in futures.items():
            try:
                sid = f.result(timeout=20)
            except Exception:
                sid = None
            if sid:
                it["seller"] = sid
    total = round(_t2.time() - _t_start, 2)
    summary = {}
    for plat, times in _plat_times.items():
        summary[plat] = {
            "count": len(times),
            "total_sec": round(sum(times), 2),
            "max_sec": round(max(times), 2),
            "avg_sec": round(sum(times)/len(times), 2),
        }
    return {"total_sec": total, "per_platform": summary}


# ── 라우트 ─────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    resp = send_from_directory(BASE_DIR, "index.html")
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

@app.route("/api/ping")
def ping():
    return jsonify({"ok": True, "version": "2026-04-20-v17"})

@app.route("/api/debug/storage")
def debug_storage():
    import os
    return jsonify({
        "data_dir_exists": os.path.isdir("/data"),
        "config_file_path": CONFIG_FILE,
        "config_file_exists": os.path.isfile(CONFIG_FILE),
        "data_dir_contents": os.listdir("/data") if os.path.isdir("/data") else [],
    })

@app.route("/api/myip")
def myip():
    try:
        r = requests.get("https://api.ipify.org?format=json", timeout=5)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/api/debug/trenbe/<int:goodsno>")
def debug_trenbe(goodsno):
    """트렌비 displaygateway API 원본 응답 확인용 (개발 전용)."""
    api_url = f"https://displaygateway.trenbe.com/v3/sdp?goodsno={goodsno}&relatedProductsSize=0"
    try:
        r = requests.get(api_url, timeout=8,
                         headers={"User-Agent": _UA,
                                  "Accept": "application/json",
                                  "Referer": "https://www.trenbe.com/"})
        data = r.json()
        # 결제혜택 관련 키를 먼저 정리해서 상단에 표시
        def collect_pay_keys(obj, path="", depth=0, out=None):
            if out is None: out = []
            if depth > 6 or out and len(out) > 200: return out
            PAY_WORDS = ("benefit","pay","toss","card","dc","discount","coupon","elPay","lpay")
            if isinstance(obj, dict):
                for k, v in obj.items():
                    full = f"{path}.{k}" if path else k
                    if any(w.lower() in k.lower() for w in PAY_WORDS):
                        out.append({"key": full, "value": v})
                    collect_pay_keys(v, full, depth+1, out)
            elif isinstance(obj, list):
                for i, v in enumerate(obj[:5]):
                    collect_pay_keys(v, f"{path}[{i}]", depth+1, out)
            return out
        pay_keys = collect_pay_keys(data)
        return jsonify({"status": r.status_code,
                        "pay_related_keys": pay_keys,
                        "full_data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/debug/lotteon/<sitm_no>")
@app.route("/api/debug/lotteon")
def debug_lotteon(sitm_no=None):
    """롯데온 pbf API 원본 응답 확인용 (개발 전용). /api/debug/lotteon/LO2627707179_2627707180"""
    if not sitm_no:
        sitm_no = request.args.get("sitm", "").strip()
    if not sitm_no:
        return jsonify({"error": "sitm 파라미터 필요 (예: /api/debug/lotteon/LO2627707179_2627707180)"}), 400
    api_url = (f"https://pbf.lotteon.com/product/v2/detail/search/base/sitm/{sitm_no}"
               f"?sitmNo={sitm_no}&ch_no=100065&ch_dtl_no=1000030"
               f"&entryPoint=pcs&srchOnlyThisItm=true&isNotContainOptMapping=true")
    try:
        r = requests.get(api_url, timeout=8,
                         headers={"User-Agent": _UA, "Accept": "application/json",
                                  "Referer": "https://www.lotteon.com/"})
        data = r.json()
        def collect_keys(obj, path="", depth=0, out=None, words=None):
            if out is None: out = []
            if words is None: words = ("card","pay","dc","disc","benefit","opt","stock","qty","option","size","color","prc","price")
            if depth > 7 or len(out) > 300: return out
            if isinstance(obj, dict):
                for k, v in obj.items():
                    full = f"{path}.{k}" if path else k
                    if any(w.lower() in k.lower() for w in words):
                        out.append({"key": full, "value": str(v)[:300]})
                    collect_keys(v, full, depth+1, out, words)
            elif isinstance(obj, list):
                for i, v in enumerate(obj[:10]):
                    collect_keys(v, f"{path}[{i}]", depth+1, out, words)
            return out
        keys = collect_keys(data)
        # base API의 priceInfo 요약
        base_price_info = (data.get("data") or {}).get("priceInfo") or {}
        result = {
            "status": r.status_code,
            "base_price_info": base_price_info,
            "related_keys": keys,
            "full_data": data,
        }

        # 혜택가 관련 API 전체 응답 탐색
        card_api_urls = [
            f"https://pbf.lotteon.com/product/v2/prc/benefit/sitm/{sitm_no}?ch_no=100065&ch_dtl_no=1000030",
            f"https://pbf.lotteon.com/product/v1/prc/display/sitmNoPrc?sitmNo={sitm_no}&ch_no=100065&ch_dtl_no=1000030",
            f"https://pbf.lotteon.com/product/v1/prc/card/sitm?sitmNo={sitm_no}&ch_no=100065&ch_dtl_no=1000030",
            f"https://pbf.lotteon.com/product/v2/detail/sitm/{sitm_no}?ch_no=100065&ch_dtl_no=1000030",
        ]
        card_results = {}
        for curl in card_api_urls:
            try:
                cr = requests.get(curl, timeout=4, headers={"User-Agent": _UA, "Accept": "application/json", "Referer": "https://www.lotteon.com/"})
                try:
                    cjson = cr.json()
                    card_results[curl] = {"status": cr.status_code, "data": cjson}
                except Exception:
                    card_results[curl] = {"status": cr.status_code, "text": cr.text[:600]}
            except Exception as ce:
                card_results[curl] = {"error": str(ce)}
        result["benefit_api_results"] = card_results

        # 파일로도 저장 (JS 차단 우회용)
        out_path = os.path.join(BASE_DIR, "lotteon_debug.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/search")
def api_search():
    """Phase-1: 네이버 API 호출 + 플랫폼 분류만. enrich 없이 즉시 반환 (~0.5-1s).
    클라이언트는 이 결과로 가격 테이블을 먼저 렌더링한 뒤 /api/enrich 를 이어서 호출."""
    query     = request.args.get("query","").strip()
    ref_price = float(request.args.get("ref_price", 0) or 0)
    if not query:
        return jsonify({"error": "query 파라미터 필요"}), 400
    try:
        top10   = search(query, ref_price, 10)
        # skip_enrich=True: 판매자 스크래핑 없이 즉시 반환
        by_plat, _, mustit_ref, anchor_plat = search_by_platform(query, ref_price, 15, skip_enrich=True)
        return jsonify({
            "query": query, "top10": top10,
            "by_platform": by_plat,
            "matching_rows": [],   # Phase-2(/api/enrich) 완료 후 채워짐
            "platforms": list(PLATFORM_MAP.keys()),
            "enriched": False,
            "mustit_ref_price": mustit_ref,   # anchor 최저가 (하한선 기준점)
            "anchor_platform": anchor_plat,   # 기준점이 된 플랫폼명
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"서버 오류: {e}"}), 500


@app.route("/api/enrich")
def api_enrich():
    """Phase-2: 판매자 스크래핑(enrich) + 판매자순 매칭 계산. /api/search 직후 클라이언트가 호출.
    반환: {
      by_platform: {plat: [item, ...]},   # seller/options/price_info 채워진 버전
      matching_rows: [...],
      enriched: True
    }"""
    query     = request.args.get("query","").strip()
    ref_price = float(request.args.get("ref_price", 0) or 0)
    if not query:
        return jsonify({"error": "query 파라미터 필요"}), 400
    try:
        import time as _tm
        _t0 = _tm.time()
        # enrich 포함 전체 실행
        by_plat, enrich_timing, mustit_ref, anchor_plat = search_by_platform(query, ref_price, 15, skip_enrich=False)
        _t1 = _tm.time()
        cfg     = load_config()
        rows    = []
        for label, smap in cfg.items():
            display_label = (smap.get("머스트잇") or "").strip() or label
            row = {"label": display_label, "cells": {}}
            for plat, sid in smap.items():
                sid = (sid or "").strip()
                if not sid: row["cells"][plat] = None; continue

                def _normalize(text):
                    """(주)/주식회사 제거, 소문자, 공백 정리"""
                    t = re.sub(r'\(주\)', '', text or '')
                    t = re.sub(r'주식회사', '', t)
                    t = t.lower().strip()
                    return re.sub(r'\s+', ' ', t)

                def _match_forms(text, _nrm=_normalize):
                    """seller 텍스트의 정규화 가능한 모든 형태 반환.
                    ① 정규화 전체  ② 괄호 제거  ③ 괄호 내용 각각
                    '주식회사 뚜또베네(TUTTO BENE)' →
                    {'뚜또베네(tutto bene)', '뚜또베네', 'tutto bene'}"""
                    t = _nrm(text)
                    forms = {t}
                    paren = re.findall(r'[(（\[]\s*([^)）\]]+?)\s*[)）\]]', t)
                    t_no_p = re.sub(r'[(（\[][^)）\]]*[)）\]]', '', t).strip()
                    t_no_p = re.sub(r'\s+', ' ', t_no_p)
                    if t_no_p:
                        forms.add(t_no_p)
                    for c in paren:
                        c = c.strip()
                        if c:
                            forms.add(c)
                    return forms

                norm_sid = _normalize(sid)
                # 디버그: 해당 플랫폼 아이템들의 seller 목록 출력
                _avail = [it.get("seller","") for it in by_plat.get(plat, [])]
                print(f"[match] plat={plat} sid={sid!r} norm={norm_sid!r} avail={_avail[:5]}")
                def _seller_match(text, _ns=norm_sid, _mf=_match_forms):
                    """정규화된 sid가 seller의 매칭 형태 중 하나와 정확 일치"""
                    return bool(_ns and _ns in _mf(text))
                found = next((it for it in by_plat.get(plat, [])
                              if _seller_match(it.get("seller") or "")
                              or _seller_match(it.get("mallName") or "")), None)
                print(f"[match] → {'FOUND: '+found.get('seller','?') if found else 'NOT FOUND'}")
                row["cells"][plat] = found
            rows.append(row)
        timing = {
            "total_sec": round(_t1 - _t0, 2),
            "enrich": enrich_timing,
        }
        return jsonify({
            "query": query,
            "by_platform": by_plat, "matching_rows": rows,
            "enriched": True,
            "timing": timing,
            "mustit_ref_price": mustit_ref,   # anchor 최저가 (하한선 기준점)
            "anchor_platform": anchor_plat,   # 기준점이 된 플랫폼명
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"서버 오류: {e}"}), 500

@app.route("/api/keys", methods=["GET"])
def get_keys():
    keys = load_keys()
    return jsonify({"client_id": keys.get("client_id",""), "has_secret": bool(keys.get("client_secret",""))})

@app.route("/api/keys", methods=["POST"])
def post_keys():
    data = request.get_json(force=True, silent=True) or {}
    existing = load_keys()
    if "client_id" in data:
        existing["client_id"] = data["client_id"]
    sec = data.get("client_secret","")
    if sec and not sec.startswith("●"):
        existing["client_secret"] = sec
    save_json(KEYS_FILE, existing)
    return jsonify({"ok": True})

@app.route("/api/keys/test")
def test_keys():
    try:
        items = call_api("구찌", max_items=10)
        return jsonify({"ok": True, "message": f"API 연결 성공 (결과 {len(items)}건)"})
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400

@app.route("/api/search/raw")
def api_search_raw():
    """디버그용: Naver API 원본 응답을 간략화해 보여줌.
    쿼리: /api/search/raw?query=구찌 (옵션: max_items=300)
    각 아이템의 mallName/link/ detect_platform() 결과를 함께 표시."""
    query = request.args.get("query", "").strip()
    try:
        max_items = int(request.args.get("max_items", 300))
    except ValueError:
        max_items = 300
    if not query:
        return jsonify({"error": "query 파라미터 필요"}), 400
    try:
        items = call_api(query, max_items=max_items)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    # 플랫폼별 카운트 + 샘플
    by_plat_count = {p: 0 for p in PLATFORM_MAP}
    by_plat_count["(unmatched)"] = 0
    simplified = []
    for it in items:
        plat = detect_platform(it) or "(unmatched)"
        by_plat_count[plat] = by_plat_count.get(plat, 0) + 1
        lnk = it.get("link", "")
        nmid = _extract_naver_nmid(lnk) if plat == "머스트잇" else ""
        pid  = it.get("productId", "")
        simplified.append({
            "platform":   plat,
            "mallName":   it.get("mallName", ""),
            "lprice":     it.get("lprice", ""),
            "title":      re.sub(r"<[^>]+>", "", it.get("title", ""))[:60],
            "link":       lnk,
            # 머스트잇 전용 — 네이버 할인 링크 구성 디버그용
            "productId":  pid,
            "naver_nmid": nmid,
            "proposed_url": (
                f"https://search.shopping.naver.com/product/{nmid}" if nmid else
                (f"https://search.shopping.naver.com/product/{pid}" if pid and plat=="머스트잇" else "")
            ),
        })
    return jsonify({
        "query": query,
        "total": len(items),
        "by_platform_count": by_plat_count,
        "items": simplified,
    })

@app.route("/api/debug/ssg")
def debug_ssg():
    """SSG 판매자·혜택가 추출 디버그. ?url=<SSG_PDP_URL>"""
    url = request.args.get("url","").strip()
    if not url:
        return jsonify({"error": "url 파라미터 필요 (예: /api/debug/ssg?url=https://www.ssg.com/item/itemView.ssg?itemId=XXX)"}), 400
    try:
        r = _ssg_get(url, timeout=8, allow_redirects=True,
                     headers={"Referer": "https://search.naver.com/"})
        html = r.text or ""
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    seller_result = _scrape_ssg(html)

    # __NEXT_DATA__ 파싱
    nd = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>([\s\S]*?)</script>', html)
    next_data_keys = []
    next_data_discount_fields = {}
    seller_from_json = None
    nd_data = {}
    if nd:
        try:
            nd_data = json.loads(nd.group(1))
            seller_from_json = _find_first_key(nd_data, (
                "sellerNick","sellerNm","sellerName","storeName",
                "sellerShopNm","shopNm","vendorNm","vendorName",
                "brandNm","partnerName","sellerBrandNm","mallName",
            ))
            # 할인 관련 키 탐색
            _DISC_KEYS = (
                "dcAmt","dcRate","couponDcAmt","couponAmt","couponPrice",
                "cardDcAmt","cardDcRate","cardBenefitAmt","cardBenefitPrice",
                "ssgMoneyAmt","ssgPointAmt","pointAmt","benefitAmt",
                "maxBenefitAmt","maxBenefitPrice","maxDcAmt",
                "immdDcAmt","immdDcRt","promotionDcAmt","promoDcAmt",
                "addDcAmt","extraDcAmt","specialDcAmt",
                "totDcAmt","finalPrice","benefitPrice",
                "discountGroups","promotionList","benefitList","couponList",
            )
            def _find_disc_fields(obj, result=None, prefix="", depth=0):
                if result is None: result = {}
                if depth > 8: return result
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        full_key = f"{prefix}{k}" if prefix else k
                        if any(dk.lower() in k.lower() for dk in _DISC_KEYS):
                            result[full_key] = v if not isinstance(v, (dict,list)) else f"[{type(v).__name__} len={len(v)}]"
                        _find_disc_fields(v, result, full_key+".", depth+1)
                elif isinstance(obj, list):
                    for i, item in enumerate(obj[:3]):
                        _find_disc_fields(item, result, f"{prefix}[{i}].", depth+1)
                return result
            next_data_discount_fields = _find_disc_fields(nd_data)

            def collect_keys(obj, prefix="", result=None, depth=0):
                if result is None: result = []
                if depth > 4 or len(result) > 150: return result
                if isinstance(obj, dict):
                    for k,v in obj.items():
                        result.append(prefix+k)
                        collect_keys(v, prefix+k+".", result, depth+1)
                elif isinstance(obj, list) and obj:
                    collect_keys(obj[0], prefix+"[].", result, depth+1)
                return result
            next_data_keys = collect_keys(nd_data)
        except Exception as e:
            next_data_keys = [f"parse error: {e}"]

    # 할인 관련 HTML 키워드 컨텍스트
    disc_kw_hits = {}
    for kw in ("즉시할인","최대혜택","최대 혜택","SSG머니","신세계포인트","카드할인",
               "카드 즉시","쿠폰","coupon","dcAmt","benefitAmt","maxBenefit",
               "카드혜택","결제혜택","포인트 적립","포인트적립"):
        idx = html.find(kw)
        if idx >= 0:
            disc_kw_hits[kw] = html[max(0,idx-60):idx+120]

    # 현재 추출 결과
    sell_price = _find_int_key(nd_data, ("salePrice","sellPrice","finalPrice","price","discountedPrice")) if nd_data else None
    pay_discs = _extract_pay_discs_from_html(html, sell_price)

    return jsonify({
        "status": r.status_code,
        "final_url": r.url,
        "html_len": len(html),
        "seller_from_html": seller_result,
        "seller_from_json": seller_from_json,
        "sell_price_from_next_data": sell_price,
        "pay_discs_extracted": pay_discs,
        "next_data_discount_fields": next_data_discount_fields,
        "discount_keyword_hits_in_html": disc_kw_hits,
        "next_data_keys_sample": next_data_keys[:100],
        "html_preview": html[:1000],
    })

@app.route("/api/debug/fetch")
def api_debug_fetch():
    """디버그용: 지정 URL을 서버가 실제로 받아온 HTML 앞부분과
    판매자 관련 키워드 매칭 여부를 반환.
    사용: /api/debug/fetch?url=<PDP_URL>"""
    url = request.args.get("url", "").strip()
    if not url:
        return jsonify({"error": "url 파라미터 필요"}), 400
    try:
        r = requests.get(url, timeout=8, allow_redirects=True,
                         headers={"User-Agent": _UA, "Accept-Language": "ko-KR,ko;q=0.9"})
    except Exception as e:
        return jsonify({"error": f"fetch 실패: {e}"}), 502
    html = r.text or ""
    found = {}
    for kw in ("상호명", "판매자", "sellerName", "sellerNick", "sellerNm", "WISELUX",
              "store@", "사업자", "SellerInfos"):
        idx = html.find(kw)
        found[kw] = idx  # -1이면 없음
    return jsonify({
        "final_url": r.url,
        "status": r.status_code,
        "content_type": r.headers.get("Content-Type", ""),
        "length": len(html),
        "keywords_found_at": found,
        "head_preview": html[:1500],  # 앞 1500자 미리보기
    })

@app.route("/api/debug/ssg_ajax")
def debug_ssg_ajax():
    """SSG AJAX 엔드포인트 전수 테스트. ?item_id=1000818976105&site_no=6004
    혜택/할인 관련 엔드포인트 포함 모두 시도."""
    item_id = request.args.get("item_id", "").strip()
    site_no = request.args.get("site_no", "6004").strip()
    if not item_id:
        return jsonify({"error": "item_id 파라미터 필요"}), 400
    referer = f"https://www.ssg.com/item/itemView.ssg?itemId={item_id}&siteNo={site_no}"
    hdrs = {
        "User-Agent": _UA,
        "Referer": referer,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, */*; q=0.01",
        "Accept-Language": "ko-KR,ko;q=0.9",
    }
    # 시도할 엔드포인트 목록 (GET)
    get_endpoints = [
        "getItemDtlInfo",
        "getItemOption",
        "getItemBenefitInfo",
        "getItemCouponInfo",
        "getItemPromotionInfo",
        "getItemCardBenefit",
        "getCardBenefit",
        "getBenefitInfo",
        "getItemDiscount",
        "getItemPayBenefit",
        "getItemInstantDiscount",
        "getItemPrice",
        "getItemPriceInfo",
        "getItemSaleInfo",
    ]
    results = {}
    # 쿠키만 공유하고 헤더는 요청별로 전달 (_SSG_SESSION 헤더를 오염시키지 않음)
    sess = requests.Session()
    sess.cookies.update(_SSG_SESSION.cookies)
    for ep in get_endpoints:
        try:
            r = sess.get(
                f"https://www.ssg.com/item/ajax/{ep}.ssg",
                params={"itemId": item_id, "siteNo": site_no},
                headers=hdrs, timeout=4)
            try:
                body = r.json()
            except Exception:
                body = r.text[:500]
            # 할인 관련 키가 있는지 체크
            body_str = json.dumps(body, ensure_ascii=False)
            has_disc = any(k in body_str.lower() for k in
                           ("dcamt","discountamt","couponamt","benefitamt",
                            "carddc","ssgmoney","pointamt","maxbenefit","favorbox"))
            results[ep] = {
                "status": r.status_code,
                "has_discount_keys": has_disc,
                "body": body if r.status_code == 200 else body_str[:300],
            }
        except Exception as e:
            results[ep] = {"error": str(e)}
        time.sleep(0.15)  # 엔드포인트 연속 호출 간 간격

    # pbf (LotteOn 방식) 유사한 SSG promotion API도 시도
    promo_endpoints = [
        f"https://www.ssg.com/promotion/ajax/getItemPromotion.ssg",
        f"https://www.ssg.com/item/ajax/getItemFavorBox.ssg",
        f"https://www.ssg.com/item/ajax/getItemBenefit.ssg",
        f"https://api.ssg.com/item/v1/benefit?itemId={item_id}&siteNo={site_no}",
    ]
    for ep_url in promo_endpoints:
        ep_key = ep_url.split("/")[-1].split("?")[0]
        try:
            r = sess.get(ep_url,
                         params={"itemId": item_id, "siteNo": site_no} if "?" not in ep_url else {},
                         headers=hdrs, timeout=4)
            try:
                body = r.json()
            except Exception:
                body = r.text[:500]
            body_str = json.dumps(body, ensure_ascii=False)
            has_disc = any(k in body_str.lower() for k in
                           ("dcamt","discountamt","couponamt","benefitamt",
                            "carddc","ssgmoney","pointamt","maxbenefit"))
            results[f"[alt]{ep_key}"] = {
                "status": r.status_code,
                "has_discount_keys": has_disc,
                "body": body if r.status_code == 200 else body_str[:300],
            }
        except Exception as e:
            results[f"[alt]{ep_key}"] = {"error": str(e)}
        time.sleep(0.15)

    # 200 응답 + 할인키 있는 것만 따로 요약
    hits = {k: v for k, v in results.items()
            if v.get("status") == 200 and v.get("has_discount_keys")}
    return jsonify({"summary_hits": list(hits.keys()), "all": results})

@app.route("/api/debug/smartstore")
def debug_smartstore():
    """스마트스토어 상세정보 추출 디버그. ?url=<스마트스토어_URL>
    _fetch_smartstore_detail 전체 결과 + __NEXT_DATA__ 주요 키 반환."""
    url = request.args.get("url", "").strip()
    if not url:
        return jsonify({"error": "url 파라미터 필요 (예: /api/debug/smartstore?url=https://smartstore.naver.com/...)"}), 400
    html_status = None
    html = ""
    try:
        r = requests.get(url, timeout=6, allow_redirects=True,
                         headers={"User-Agent": _UA, "Accept-Language": "ko-KR,ko;q=0.9"})
        html_status = r.status_code
        if r.status_code == 200:
            html = r.text or ""
    except Exception as e:
        html_status = f"error: {e}"

    # __NEXT_DATA__ 분석 (HTML 가져온 경우만)
    nd_keys = []
    discount_hits = {}
    opt_sample = None
    if html:
        nd = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>([\s\S]*?)</script>', html)
        if nd:
            try:
                data = json.loads(nd.group(1))
                for k in ("immediateDiscountAmount","immediateDiscountPrice","discountAmount",
                          "naverMembershipDiscountAmount","membershipDiscountAmount",
                          "couponBenefitAmount","couponDiscountAmount","naverPayPointAmount",
                          "accumulationAmount","mileage"):
                    v = _find_int_key(data, (k,))
                    if v: discount_hits[k] = v
                for k in ("optionCombinations","combinations","productOptions","optionList","options"):
                    lst = _find_list_key(data, (k,))
                    if lst:
                        opt_sample = {"key": k, "count": len(lst), "first": lst[0] if lst else None}
                        break
                def collect_keys(obj, prefix="", result=[], depth=0):
                    if depth > 4 or len(result) > 100: return result
                    if isinstance(obj, dict):
                        for k,v in obj.items():
                            result.append(prefix+k)
                            collect_keys(v, prefix+k+".", result, depth+1)
                    elif isinstance(obj, list) and obj:
                        collect_keys(obj[0], prefix+"[].", result, depth+1)
                    return result
                nd_keys = collect_keys(data)[:80]
            except Exception as e:
                nd_keys = [f"parse error: {e}"]

    # 전체 detail 추출 결과 (JSON API 사용)
    _DETAIL_CACHE.pop(url, None)
    detail = _fetch_smartstore_detail(url)

    return jsonify({
        "html_status": html_status,
        "html_len": len(html),
        "has_next_data": bool(html and re.search(r'id="__NEXT_DATA__"', html)),
        "discount_hits_in_json": discount_hits,
        "option_sample": opt_sample,
        "nd_keys_sample": nd_keys[:60],
        "full_detail": detail,
    })

@app.route("/api/debug/ss_api")
def debug_ss_api():
    """스마트스토어 API + 카탈로그 페이지 디버그.
    ?url=<smartstore URL>  /  ?product_no=<상품번호>  /  ?catalog_id=<네이버 productId>"""
    url = request.args.get("url", "").strip()
    product_no = request.args.get("product_no", "").strip()
    catalog_id = request.args.get("catalog_id", "").strip()
    if url and not product_no:
        product_no = _extract_ss_product_no(url)
    if not product_no and not catalog_id:
        return jsonify({"error": "url / product_no / catalog_id 파라미터 필요"}), 400

    results = {}
    # SmartStore JSON API
    for _api_path in filter(None, [
        f"https://smartstore.naver.com/i/v1/products/{product_no}" if product_no else None,
        f"https://smartstore.naver.com/i/v2/channels/main/products/{product_no}" if product_no else None,
    ]):
        try:
            _r = requests.get(_api_path, timeout=5, headers=_SS_HDRS)
            try: _body = _r.json()
            except Exception: _body = _r.text[:500]
            results[_api_path] = {"status": _r.status_code, "body": _body}
        except Exception as e:
            results[_api_path] = {"error": str(e)}

    # Naver Shopping 카탈로그 페이지 (search.shopping.naver.com)
    for _cid in dict.fromkeys(filter(None, [catalog_id, product_no])):
        _cat_url = f"https://search.shopping.naver.com/catalog/{_cid}"
        try:
            _cr = requests.get(_cat_url, timeout=6, headers={
                "User-Agent": _UA, "Accept-Language": "ko-KR,ko;q=0.9",
            })
            _nd = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>([\s\S]*?)</script>', _cr.text or "")
            _cat_body = None
            if _nd:
                try: _cat_body = json.loads(_nd.group(1))
                except: _cat_body = "parse_error"
            results[_cat_url] = {
                "status": _cr.status_code,
                "has_next_data": bool(_nd),
                "next_data_snippet": _cat_body,
            }
        except Exception as e:
            results[_cat_url] = {"error": str(e)}

    # 파싱 결과
    parsed = None
    if url:
        _DETAIL_CACHE.pop(url, None)
        parsed = _fetch_smartstore_detail(url, catalog_id=catalog_id or product_no)

    return jsonify({
        "product_no": product_no,
        "api_results": results,
        "parsed_detail": parsed,
    })


@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify(load_config())

@app.route("/api/config", methods=["POST"])
def post_config():
    data = request.get_json(force=True, silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "잘못된 형식"}), 400
    save_json(CONFIG_FILE, data)
    return jsonify({"ok": True})

@app.route("/api/config/cleanup", methods=["GET","POST"])
def cleanup_config():
    """설정에서 특정 값을 가진 셀만 빈 문자열로 초기화."""
    body = request.get_json(force=True, silent=True) or {}
    # GET 파라미터 또는 POST body 모두 지원
    targets = body.get("values") or request.args.getlist("v") or []
    if not targets:
        return jsonify({"error": "values 필요"}), 400
    targets_lower = [v.strip().lower() for v in targets]
    cfg = load_config()
    cleaned = 0
    for label, smap in cfg.items():
        for plat, sid in smap.items():
            if (sid or "").strip().lower() in targets_lower:
                cfg[label][plat] = ""
                cleaned += 1
    save_json(CONFIG_FILE, cfg)
    return jsonify({"ok": True, "cleaned": cleaned})

@app.route("/api/debug/mustit_dump")
def api_debug_mustit_dump():
    """머스트잇 페이지 HTML을 파일로 저장 후 요약 반환. ?pd_id=숫자"""
    pd_id = request.args.get("pd_id", "").strip()
    if not pd_id:
        return jsonify({"error": "pd_id 필요"}), 400
    target = f"https://m.web.mustit.co.kr/v2/m/product/product_detail/{pd_id}"
    try:
        r = requests.get(target, timeout=8, allow_redirects=True,
                         headers={"User-Agent": _UA, "Accept-Language": "ko-KR,ko;q=0.9"})
        html = r.text or ""
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    dump_path = os.path.join(BASE_DIR, f"_debug_mustit_{pd_id}.html")
    with open(dump_path, "w", encoding="utf-8") as f:
        f.write(html)

    # 영문 키 패턴 수집
    eng_keys = {}
    for m in re.finditer(r'"([a-zA-Z][a-zA-Z0-9_]{2,40})"\s*:\s*"?([^"\\,\]\}\n]{0,60})', html):
        k, v = m.group(1), m.group(2).strip().rstrip('\\').strip()
        if k not in eng_keys and v:
            eng_keys[k] = v

    # 한국어 키워드 컨텍스트
    ko_kwds = ["새상품","중고","정품","인증","등급","배송비","재고","정가","원가","할인"]
    ko_ctx = {}
    for kw in ko_kwds:
        idx = html.find(kw)
        if idx >= 0:
            ko_ctx[kw] = html[max(0,idx-60):idx+100]

    return jsonify({
        "saved_to": dump_path,
        "html_length": len(html),
        "eng_keys": eng_keys,
        "ko_contexts": ko_ctx,
    })

@app.route("/api/debug/mustit_html")
def api_debug_mustit_html():
    """머스트잇 페이지 전체 HTML에서 키워드/패턴을 검색해 컨텍스트 반환.
    ?pd_id=<숫자>&kw=키워드1,키워드2,..."""
    pd_id = request.args.get("pd_id", "").strip()
    extra_kw = request.args.get("kw", "").strip()
    if not pd_id:
        return jsonify({"error": "pd_id 필요"}), 400
    target = f"https://m.web.mustit.co.kr/v2/m/product/product_detail/{pd_id}"
    try:
        r = requests.get(target, timeout=8, allow_redirects=True,
                         headers={"User-Agent": _UA, "Accept-Language": "ko-KR,ko;q=0.9"})
        html = r.text or ""
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    # 1) 영문 키 패턴 (RSC escaped + 일반 JSON 모두)
    eng_keys = {}
    for m in re.finditer(r'[\\"]?([a-zA-Z][a-zA-Z0-9_]{2,40})[\\"]?\s*:\s*[\\"]?([^"\\,\]\}\n]{0,80})', html):
        k, v = m.group(1), m.group(2).strip().rstrip('\\').strip('"')
        if k not in eng_keys and v and len(v) < 60:
            eng_keys[k] = v

    # 2) 한국어 키워드 컨텍스트 (새상품, 중고, 정품, 등급 등)
    ko_keywords = ["새상품", "중고", "정품인증", "인증", "등급", "배송비", "재고", "정가", "원가"]
    if extra_kw:
        ko_keywords += [k.strip() for k in extra_kw.split(",")]
    ko_ctx = {}
    for kw in ko_keywords:
        idx = html.find(kw)
        if idx >= 0:
            ko_ctx[kw] = html[max(0, idx-80):idx+120]

    return jsonify({
        "pd_id": pd_id,
        "html_length": len(html),
        "eng_keys_sample": dict(list(eng_keys.items())[:200]),
        "korean_keyword_contexts": ko_ctx,
    })

@app.route("/api/debug/mustit")
def api_debug_mustit():
    """머스트잇 상품 페이지 원본에서 추출 가능한 필드 전체를 반환.
    ?link=<naver_mustit_link> 또는 ?pd_id=<숫자>"""
    link = request.args.get("link", "").strip()
    pd_id = request.args.get("pd_id", "").strip()
    if not link and not pd_id:
        return jsonify({"error": "link 또는 pd_id 파라미터 필요"}), 400
    if pd_id:
        target = f"https://m.web.mustit.co.kr/v2/m/product/product_detail/{pd_id}"
    else:
        decoded = unquote(link)
        m = re.search(r'/product_detail/(\d+)', decoded)
        if not m:
            return jsonify({"error": "product_detail ID를 URL에서 찾을 수 없음", "decoded": decoded}), 400
        pd_id = m.group(1)
        target = f"https://m.web.mustit.co.kr/v2/m/product/product_detail/{pd_id}"
    try:
        r = requests.get(target, timeout=8, allow_redirects=True,
                         headers={"User-Agent": _UA, "Accept-Language": "ko-KR,ko;q=0.9"})
        html = r.text or ""
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    # __NEXT_DATA__ 전체 파싱
    nd_data = None
    nd = re.search(r'<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)</script>', html)
    if nd:
        try:
            nd_data = json.loads(nd.group(1))
        except Exception:
            pass

    # RSC에서 키-값 쌍 전체 추출 (문자열/숫자/bool 모두)
    rsc_pairs = {}
    for m2 in re.finditer(r'\\?"(\w+)\\?"\s*:\s*(?:\\?"([^"\\]{0,200})\\?"|(\d+)|(true|false))', html):
        k, vs, vi, vb = m2.group(1), m2.group(2), m2.group(3), m2.group(4)
        v = vs if vs is not None else (int(vi) if vi else vb)
        if k not in rsc_pairs:
            rsc_pairs[k] = v

    # 현재 _fetch_mustit_detail로 추출한 결과
    extracted = {}
    detail = _fetch_mustit_detail(link or f"https://mustit.co.kr/product_detail/{pd_id}")
    if detail:
        extracted = detail

    return jsonify({
        "pd_id": pd_id,
        "target_url": target,
        "http_status": r.status_code,
        "html_length": len(html),
        "has_next_data": nd_data is not None,
        "rsc_key_value_pairs": dict(sorted(rsc_pairs.items())),
        "extracted_by_current_code": extracted,
    })

@app.route("/api/debug/mustit_search")
def api_debug_mustit_search():
    """머스트잇 mobile_search_v2 RSC 데이터 깊이 파싱.
    사용: /api/debug/mustit_search?keyword=르메르+크루아상백"""
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"error": "keyword 파라미터 필요"}), 400

    kw_enc = requests.utils.quote(keyword)
    url = f"https://m.web.mustit.co.kr/v2/m/search/product?keyword={kw_enc}&sortType=LOW_PRICE&page=1&limit=20"
    _hdrs = {"User-Agent": _UA, "Accept": "text/html", "Accept-Language": "ko-KR,ko;q=0.9",
             "Referer": "https://m.web.mustit.co.kr/"}
    try:
        r = requests.get(url, timeout=10, allow_redirects=True, headers=_hdrs)
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    html = r.text or ""

    # ① 모든 숫자 6~8자리 패턴 (가격대 30만~300만 범위로 필터)
    all_nums = [int(x) for x in re.findall(r'\b(\d{5,8})\b', html)]
    price_range = [x for x in all_nums if 10000 <= x <= 50000000]
    price_range_uniq = sorted(set(price_range))[:50]

    # ② RSC 플라이트 데이터에서 sellPrice / price 패턴
    rsc_prices = re.findall(r'"(?:sellPrice|salePrice|price|lowestPrice|normalPrice|originPrice)"\s*:\s*(\d{4,9})', html)
    rsc_prices = [int(x) for x in rsc_prices if 10000 <= int(x) <= 50000000]

    # ③ __NEXT_DATA__ 확인
    nd_match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    nd_prices = []
    nd_keys = []
    if nd_match:
        try:
            nd = json.loads(nd_match.group(1))
            nd_str = json.dumps(nd)
            nd_prices = [int(x) for x in re.findall(r'"(?:price|sellPrice|salePrice)"\s*:\s*(\d{4,9})', nd_str) if 10000 <= int(x) <= 50000000]
            nd_keys = list(nd.keys())[:20]
        except Exception:
            pass

    # ④ RSC 청크 앞부분 (self.__next_f.push)
    rsc_chunks = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html[:100000])

    result = {
        "status": r.status_code,
        "html_len": len(html),
        "has_next_data": nd_match is not None,
        "nd_keys": nd_keys,
        "nd_prices": nd_prices[:20],
        "rsc_prices_all": sorted(set(rsc_prices))[:30],
        "numeric_range_10k_50m": price_range_uniq[:30],
        "rsc_chunks_count": len(rsc_chunks),
        "rsc_chunk_sample": [c[:200] for c in rsc_chunks[:3]],
        "html_snippet_with_price": "",
    }

    # ⑤ 가격처럼 보이는 숫자 주변 컨텍스트
    for px in sorted(set(rsc_prices))[:3]:
        idx = html.find(f':{px}')
        if idx > 0:
            result["html_snippet_with_price"] += html[max(0,idx-80):idx+80] + "\n---\n"

    _out = os.path.join(BASE_DIR, "mustit_search_debug.json")
    with open(_out, "w", encoding="utf-8") as _f:
        json.dump(result, _f, ensure_ascii=False, indent=2)
    return jsonify(result)


@app.route("/api/debug/mustit_live")
def api_debug_mustit_live():
    """Railway 환경 진단 (Playwright + curl_cffi 모두 테스트). ?pd_id=숫자"""
    pd_id = request.args.get("pd_id", "121340554").strip()
    result = {
        "pd_id": pd_id,
        "has_cffi": _HAS_CFFI,
        "has_playwright": _HAS_PLAYWRIGHT,
        "pw_browser_alive": _PW_BROWSER is not None,
    }

    # ── Playwright 테스트 ──────────────────────────────────────────────
    # 브라우저 먼저 기동 시도하여 에러 캡처
    pw_launch_error = None
    try:
        _get_pw_browser()
    except Exception as e:
        pw_launch_error = str(e)
    result["pw_browser_alive"] = _PW_BROWSER is not None
    result["pw_launch_error"] = pw_launch_error
    result["pw_ctx_alive"] = _PW_CTX is not None

    # Playwright raw 테스트 — 공유 컨텍스트(쿠키 유지) + CF 챌린지 대기
    try:
        ctx = _get_pw_context()
        if ctx:
            url = f"https://m.web.mustit.co.kr/v2/m/product/product_detail/{pd_id}"
            page = ctx.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=40000)
                _pw_wait_challenge(page, timeout_ms=30000)
                page.wait_for_timeout(1500)
                pw_html = page.content()
                final_pw_url = page.url
            except Exception as ge:
                pw_html = ""
                final_pw_url = f"ERROR: {ge}"
            page.close()
            cf_names = [c["name"] for c in ctx.cookies() if "cf_" in c["name"].lower()]
            seller_idx = pw_html.find('sellerId')
            result["playwright"] = {
                "final_url": final_pw_url,
                "html_length": len(pw_html),
                "has_sellerId": seller_idx >= 0,
                "sellerId_context": pw_html[max(0,seller_idx-20):seller_idx+60] if seller_idx >= 0 else None,
                "html_head_300": pw_html[:300],
                "has_verification": "Human Verification" in pw_html,
                "has_next_f": "__next_f" in pw_html,
                "cookie_count": len(ctx.cookies()),
                "cf_cookies": cf_names,
            }
        else:
            result["playwright"] = {"error": "context is None"}
    except Exception as e:
        result["playwright"] = {"error": str(e)}

    # ── curl_cffi / requests 테스트 ────────────────────────────────────
    target = f"https://m.web.mustit.co.kr/v2/m/product/product_detail/{pd_id}"
    try:
        sess = _get_mustit_session()
        hdrs = {"User-Agent": _UA, "Accept-Language": "ko-KR,ko;q=0.9",
                "Referer": "https://m.web.mustit.co.kr/"}
        r = sess.get(target, timeout=10, headers=hdrs, allow_redirects=True) if sess \
            else requests.get(target, timeout=8, headers=hdrs, allow_redirects=True)
        html = r.text or ""
        seller_idx = html.find('sellerId')
        result["curl_cffi"] = {
            "status_code": r.status_code,
            "final_url": str(getattr(r, 'url', target)),
            "html_length": len(html),
            "has_sellerId": seller_idx >= 0,
            "html_head_200": html[:200],
        }
    except Exception as e:
        result["curl_cffi"] = {"error": str(e)}

    # ── Mustit 내부 REST API 엔드포인트 탐색 ──────────────────────────
    # 앱/Next.js가 호출하는 JSON API는 CF 보호가 다를 수 있음
    api_candidates = [
        f"https://mustit.co.kr/api/v1/goods/{pd_id}",
        f"https://mustit.co.kr/api/v2/goods/{pd_id}",
        f"https://mustit.co.kr/api/goods/{pd_id}",
        f"https://m.web.mustit.co.kr/api/v1/goods/{pd_id}",
        f"https://m.web.mustit.co.kr/api/v2/goods/{pd_id}",
        f"https://m.web.mustit.co.kr/api/product/{pd_id}",
        # RSC 전용 요청 (Next.js App Router)
        f"https://m.web.mustit.co.kr/v2/m/product/product_detail/{pd_id}",
    ]
    api_hdrs_base = {
        "User-Agent": _UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Referer": "https://m.web.mustit.co.kr/",
        "X-Requested-With": "XMLHttpRequest",
    }
    # RSC 요청용 헤더 변형
    rsc_hdrs = {**api_hdrs_base, "RSC": "1", "Accept": "text/x-component"}

    api_results = []
    sess2 = _get_mustit_session()
    for i, url in enumerate(api_candidates):
        try:
            h = rsc_hdrs if i == len(api_candidates) - 1 else api_hdrs_base
            fn = sess2.get if sess2 else requests.get
            r2 = fn(url, timeout=8, headers=h, allow_redirects=False)
            body = r2.text or ""
            api_results.append({
                "url": url,
                "status": r2.status_code,
                "len": len(body),
                "has_sellerId": "sellerId" in body,
                "head_150": body[:150],
            })
        except Exception as ex:
            api_results.append({"url": url, "error": str(ex)})
    result["api_probe"] = api_results

    return jsonify(result)


@app.route("/api/mustit_exposure")
def api_mustit_exposure():
    """머스트잇 노출순위 조회: 가격순 상위 10개 머스트잇 상품의 네이버 랭킹순 노출순위 반환."""
    query = request.args.get("query", "").strip()
    if not query:
        return jsonify({"error": "query 파라미터가 필요합니다."}), 400
    try:
        # 1. 네이버랭킹순(sim) 호출 → 링크별 노출순위 맵
        sim_items = call_api(query, max_items=200, sort="sim")
        rank_map = {}
        for idx, it in enumerate(sim_items):
            link = (it.get("link") or "").strip()
            if link and link not in rank_map:
                rank_map[link] = idx + 1

        # 2. 가격순(asc) 호출 → 머스트잇만 필터, 상위 10개
        asc_items = call_api(query, max_items=300, sort="asc")
        results = []
        seen = set()
        for item in asc_items:
            if detect_platform(item) != "머스트잇":
                continue
            link = (item.get("link") or "").strip()
            if not link or link in seen:
                continue
            seen.add(link)
            # 품번: mustit URL에서 product_detail ID 추출
            pd_m = re.search(r'/product[_-]?detail/(\d+)', link)
            if not pd_m:
                pd_m = re.search(r'/(\d{5,})', link)
            product_no = pd_m.group(1) if pd_m else item.get("mallProductId", "")
            price_str = item.get("lprice", "0")
            price = int(price_str) if price_str.isdigit() else 0
            if price == 0:
                continue
            naver_rank = rank_map.get(link)
            rc_str = item.get("reviewCount", "0") or "0"
            review_count = int(rc_str) if str(rc_str).isdigit() else 0
            results.append({
                "product_no": product_no,
                "name": strip_html(item.get("title", "")),
                "price": price,
                "review_count": review_count,
                "naver_rank": naver_rank,
                "link": link,
            })
            if len(results) >= 10:
                break
        return jsonify({
            "query": query,
            "items": results,
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"조회 실패: {str(e)}"}), 500


@app.route("/api/restart", methods=["POST", "GET"])
def api_restart():
    """새 python 프로세스를 띄운 뒤 현재 프로세스를 종료.
    Windows에서 os.execv는 프로세스 교체가 안 되므로
    subprocess.Popen으로 새 프로세스를 먼저 시작 후 sys.exit."""
    import threading
    def _do_restart():
        import time, sys, subprocess, os
        time.sleep(0.4)   # 응답 전송 여유
        subprocess.Popen(
            [sys.executable] + sys.argv,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0),
        )
        os._exit(0)       # 현재 프로세스 강제 종료
    threading.Thread(target=_do_restart, daemon=True).start()
    return jsonify({"ok": True, "message": "재시작 중..."})

# ── 접근 비밀번호 (환경변수 ACCESS_PASSWORD 설정 시 활성화) ──────────────────────
_ACCESS_PW = os.environ.get("ACCESS_PASSWORD", "").strip()

@app.before_request
def check_auth():
    if not _ACCESS_PW:
        return   # 비밀번호 미설정 시 인증 없이 허용 (로컬 개발)
    if request.path == "/health":
        return   # Railway 헬스체크는 인증 제외
    import base64
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Basic "):
        try:
            _, pw = base64.b64decode(auth[6:]).decode().split(":", 1)
            if pw == _ACCESS_PW:
                return
        except Exception:
            pass
    resp = app.make_response("")
    resp.status_code = 401
    resp.headers["WWW-Authenticate"] = 'Basic realm="Mustit"'
    return resp

@app.route("/health")
def health():
    return "ok", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    host = "0.0.0.0" if os.environ.get("PORT") else "127.0.0.1"
    app.run(host=host, port=port, debug=False, use_reloader=False)
