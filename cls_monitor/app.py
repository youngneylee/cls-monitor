import os
import json
import time
import hashlib
import threading
import sqlite3
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse

# 清理环境代理，避免系统代理干扰
for key in [
    "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
    "ALL_PROXY", "all_proxy", "NO_PROXY", "no_proxy"
]:
    os.environ.pop(key, None)

app = FastAPI()

URL = "https://m.cls.cn/telegraph"
DB = "cls_news.db"

FETCH_INTERVAL = 60
QUOTE_INTERVAL = 45

# ✅ 只推送这个飞书机器人
FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/36d76c0a-d013-455a-9017-c13f259c7b5e"

# 更稳的 requests Session
session = requests.Session()
session.trust_env = False

retry = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
session.mount("http://", adapter)
session.mount("https://", adapter)

COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Connection": "close",
}

EM_HEADERS = {
    **COMMON_HEADERS,
    "Referer": "https://quote.eastmoney.com/",
    "Origin": "https://quote.eastmoney.com",
}

QUOTE_CACHE = {}

# 股票库（可按需添加）
STOCKS = [
    {"code": "600875", "name": "东方电气", "aliases": ["东方电气"], "concepts": ["风电", "核电", "发电设备"]},
    {"code": "605196", "name": "华通线缆", "aliases": ["华通线缆"], "concepts": ["电缆", "海缆"]},
    {"code": "002837", "name": "英维克", "aliases": ["英维克"], "concepts": ["液冷", "算力", "数据中心"]},
    {"code": "002056", "name": "横店东磁", "aliases": ["横店东磁"], "concepts": ["光伏", "储能", "电池片"]},
    {"code": "688256", "name": "寒武纪", "aliases": ["寒武纪"], "concepts": ["AI芯片", "算力", "服务器"]},
    {"code": "300308", "name": "中际旭创", "aliases": ["中际旭创"], "concepts": ["光模块", "CPO", "AI算力"]},
    {"code": "002281", "name": "光迅科技", "aliases": ["光迅科技"], "concepts": ["光模块", "CPO"]},
    {"code": "300502", "name": "新易盛", "aliases": ["新易盛"], "concepts": ["光模块", "AI算力"]},
]

HOT = [
    "涨停", "大涨", "算力", "AI", "机器人",
    "光模块", "风电", "光伏", "储能",
    "中标", "签约", "订单", "合作", "重组"
]

# 龙头股判断
def is_leading_stock(name: str) -> bool:
    return "龙头" in name or "领先" in name


def now_full():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def fp(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def get_conn():
    return sqlite3.connect(DB, check_same_thread=False)


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS news(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        content TEXT,
        hash TEXT UNIQUE,
        related_stocks TEXT,
        pushed INTEGER DEFAULT 0,
        time TEXT
    )
    """)
    conn.commit()
    conn.close()


# —— 行情抓取 —— #

def update_quotes():
    global QUOTE_CACHE

    url = "https://82.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "5000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": "f2,f3,f4,f12,f14",
    }

    while True:
        try:
            resp = session.get(url, params=params, headers=EM_HEADERS, timeout=(8, 15))
            resp.raise_for_status()
            data = resp.json()

            diff = data.get("data", {}).get("diff", [])
            cache = {}

            for row in diff:
                code = str(row.get("f12", "")).zfill(6)
                if not code:
                    continue

                cache[code] = {
                    "price": row.get("f2"),
                    "pct": row.get("f3"),
                    "change": row.get("f4"),
                    "name": row.get("f14"),
                }

            if cache:
                QUOTE_CACHE = cache
                print(f"[{now_full()}] 行情更新成功: {len(cache)}")
            else:
                print(f"[{now_full()}] 行情返回为空")

        except Exception as e:
            print(f"[{now_full()}] 行情失败:", repr(e))

        time.sleep(QUOTE_INTERVAL)


def get_quote(code: str):
    return QUOTE_CACHE.get(code)


# —— 财联社抓取 —— #

def fetch():
    resp = session.get(URL, headers=COMMON_HEADERS, timeout=(8, 15))
    resp.encoding = "utf-8"

    soup = BeautifulSoup(resp.text, "html.parser")
    res = []
    for i in soup.select("div#tele span"):
        t = i.get_text(" ", strip=True)
        if "财联社" in t and len(t) > 20:
            res.append(t)

    uniq = []
    seen = set()
    for i in res:
        h = fp(i)
        if h not in seen:
            seen.add(h)
            uniq.append(i)

    return uniq


def match(text: str):
    res = []
    for s in STOCKS:
        matched = None
        for a in s["aliases"]:
            if a in text:
                matched = {**s, "match_type": "alias", "hit": a}
                break
        if not matched:
            for c in s["concepts"]:
                if c in text:
                    matched = {**s, "match_type": "concept", "hit": c}
                    break
        if matched:
            res.append(matched)

    # 龙头放前面
    leading = [s for s in res if is_leading_stock(s["name"])]
    others = [s for s in res if not is_leading_stock(s["name"])]
    return leading + others


def send(msg: str):
    data = {
        "msg_type": "text",
        "content": {"text": msg},
    }
    try:
        resp = session.post(FEISHU_WEBHOOK, json=data, headers=COMMON_HEADERS, timeout=(8, 12))
        print(f"[{now_full()}] 飞书推送: {resp.status_code}")
    except Exception as e:
        print(f"[{now_full()}] 飞书推送失败:", repr(e))


def build_push_message(line: str, stocks: list):
    msg = "【财联社监控】\n\n"
    msg += line + "\n\n"
    msg += "关联股票：\n"
    for s in stocks[:5]:
        q = get_quote(s["code"])
        if q:
            msg += (f"- {s['name']}({s['code']}) "
                    f"{q['price']} {q['pct']}%\n")
        else:
            msg += f"- {s['name']}({s['code']})\n"
    msg += "\n仅供信息整理参考，不构成投资建议。"
    return msg


def save(lines):
    conn = get_conn()
    cur = conn.cursor()
    new_count = 0
    pushed_count = 0

    for line in lines:
        h = fp(line)
        stocks = match(line)
        try:
            cur.execute(
                "INSERT INTO news(content,hash,related_stocks,pushed,time) VALUES(?,?,?,?,?)",
                (line, h, json.dumps(stocks, ensure_ascii=False), 0, now_full())
            )
            news_id = cur.lastrowid
            new_count += 1

            if stocks:
                msg = build_push_message(line, stocks)
                send(msg)
                cur.execute("UPDATE news SET pushed=1 WHERE id=?", (news_id,))
                pushed_count += 1

        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    return new_count, pushed_count


def loop():
    while True:
        try:
            lines = fetch()
            new_count, pushed_count = save(lines)
            print(f"[{now_full()}] 本轮新增 {new_count} 条，推送 {pushed_count} 条")
        except Exception as e:
            print(f"[{now_full()}] 抓取失败:", repr(e))
        time.sleep(FETCH_INTERVAL)


@app.on_event("startup")
def startup():
    init_db()
    threading.Thread(target=loop, daemon=True).start()
    threading.Thread(target=update_quotes, daemon=True).start()


@app.get("/", response_class=HTMLResponse)
def home():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT content,time,pushed FROM news ORDER BY id DESC LIMIT 200")
    rows = cur.fetchall()
    conn.close()

    html = "<html><head><meta charset='utf-8'></head><body>"
    html += "<h3>财联社监控</h3><a href='/refresh'>手动抓取</a><hr>"

    for content, t, pushed in rows:
        html += f"<div>{t} {'[已推送]' if pushed else ''}<br>{content}</div><hr>"

    html += "</body></html>"
    return html


@app.get("/refresh")
def refresh():
    lines = fetch()
    new_count, pushed_count = save(lines)
    return RedirectResponse("/", status_code=302)