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


for key in [
    "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
    "ALL_PROXY", "all_proxy", "NO_PROXY", "no_proxy"
]:
    os.environ.pop(key, None)

app = FastAPI()

DB = "cls_news.db"
FETCH_INTERVAL = 60
QUOTE_INTERVAL = 45

CLS_URL = "https://m.cls.cn/telegraph"
EM_KX_URL = "https://kuaixun.eastmoney.com/"
THS_KX_URL = "https://stock.10jqka.com.cn/kx/"

# 多群 webhook
FEISHU_WEBHOOKS = {
    "ai": "https://open.feishu.cn/open-apis/bot/v2/hook/364ad825-1f90-4d88-9948-b69b8c98b4ce",
    "energy": "https://open.feishu.cn/open-apis/bot/v2/hook/79ac4918-6a47-42e7-95f1-d5c1a0e23db1",
    "finance": "https://open.feishu.cn/open-apis/bot/v2/hook/1fe92053-ceb6-46c6-aa6a-05235d4ba6a8",
}

QUOTE_CACHE = {}

STOCKS = [
    {"code": "600875", "name": "东方电气", "aliases": ["东方电气"], "concepts": ["风电", "核电", "发电设备"]},
    {"code": "605196", "name": "华通线缆", "aliases": ["华通线缆"], "concepts": ["电缆", "海缆"]},
    {"code": "002837", "name": "英维克", "aliases": ["英维克"], "concepts": ["液冷", "算力", "数据中心"]},
    {"code": "002056", "name": "横店东磁", "aliases": ["横店东磁"], "concepts": ["光伏", "储能", "电池片"]},
    {"code": "688256", "name": "寒武纪", "aliases": ["寒武纪"], "concepts": ["AI芯片", "算力", "服务器"]},
    {"code": "300308", "name": "中际旭创", "aliases": ["中际旭创"], "concepts": ["光模块", "CPO", "AI算力"]},
    {"code": "002281", "name": "光迅科技", "aliases": ["光迅科技"], "concepts": ["光模块", "CPO"]},
    {"code": "300502", "name": "新易盛", "aliases": ["新易盛"], "concepts": ["光模块", "AI算力"]},
    {"code": "000063", "name": "中兴通讯", "aliases": ["中兴通讯", "中兴"], "concepts": ["5G", "算力", "服务器"]},
    {"code": "600941", "name": "中国移动", "aliases": ["中国移动"], "concepts": ["算力", "数据中心", "云计算"]},
    {"code": "600050", "name": "中国联通", "aliases": ["中国联通"], "concepts": ["算力", "数据中心", "云计算"]},
    {"code": "002230", "name": "科大讯飞", "aliases": ["科大讯飞", "讯飞"], "concepts": ["人工智能", "AI", "大模型"]},
    {"code": "601318", "name": "中国平安", "aliases": ["中国平安"], "concepts": ["保险", "金融"]},
    {"code": "600030", "name": "中信证券", "aliases": ["中信证券"], "concepts": ["券商", "金融"]},
    {"code": "601398", "name": "工商银行", "aliases": ["工商银行"], "concepts": ["银行", "金融"]},
]

THEME_KEYWORDS = {
    "算力": ["算力", "AI服务器", "GPU", "数据中心", "液冷", "大模型", "人工智能", "光模块", "CPO", "AI芯片"],
    "新能源": ["风电", "光伏", "储能", "核电", "锂电", "电池片", "海上风电", "机组"],
    "金融": ["银行", "券商", "保险", "降准", "降息", "MLF", "LPR", "货币政策", "金融", "逆回购"],
}

POSITIVE_WORDS_STRONG = [
    "全球第一", "居全球第一", "持续增长", "领先", "新高", "大增",
    "中标", "签约", "获批", "订单", "开工", "投产", "量产", "扩产", "核准"
]
POSITIVE_WORDS_MEDIUM = [
    "推进", "合作", "建设", "落地", "发布", "加快", "启动", "回暖"
]
NEGATIVE_WORDS = [
    "下滑", "停工", "事故", "亏损", "减持", "处罚", "诉讼", "终止", "风险"
]

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


def now_full() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def md5_text(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def get_conn():
    return sqlite3.connect(DB, check_same_thread=False)


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS news(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        content TEXT,
        hash TEXT UNIQUE,
        related_stocks TEXT,
        analysis TEXT,
        channels TEXT,
        pushed INTEGER DEFAULT 0,
        time TEXT
    )
    """)
    conn.commit()
    conn.close()


def get_quote(code: str):
    return QUOTE_CACHE.get(code)


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
                print(f"[{now_full()}] 行情更新 {len(cache)}")
            else:
                print(f"[{now_full()}] 行情返回为空")

        except Exception as e:
            print(f"[{now_full()}] 行情失败 {repr(e)}")

        time.sleep(QUOTE_INTERVAL)


def match_stocks(text: str):
    results = []

    for s in STOCKS:
        matched = None

        for a in s["aliases"]:
            if a and a in text:
                matched = {
                    "code": s["code"],
                    "name": s["name"],
                    "match_type": "alias",
                    "hit": a,
                }
                break

        if not matched:
            for c in s["concepts"]:
                if c and c in text:
                    matched = {
                        "code": s["code"],
                        "name": s["name"],
                        "match_type": "concept",
                        "hit": c,
                    }
                    break

        if matched:
            results.append(matched)

    uniq = {}
    for item in results:
        if item["code"] not in uniq:
            uniq[item["code"]] = item

    return list(uniq.values())


def detect_theme(text: str) -> str:
    for theme, kws in THEME_KEYWORDS.items():
        for kw in kws:
            if kw in text:
                return theme
    return "综合"


def score_sentiment(text: str):
    score = 0
    reasons = []

    for w in POSITIVE_WORDS_STRONG:
        if w in text:
            score += 2
            reasons.append(w)

    for w in POSITIVE_WORDS_MEDIUM:
        if w in text:
            score += 1
            reasons.append(w)

    for w in NEGATIVE_WORDS:
        if w in text:
            score -= 2
            reasons.append(w)

    if score >= 4:
        direction = "强利好"
        stars = "★★★★★"
    elif score >= 2:
        direction = "中度利好"
        stars = "★★★★☆"
    elif score >= 1:
        direction = "轻度利好"
        stars = "★★★☆☆"
    elif score <= -2:
        direction = "利空"
        stars = "★☆☆☆☆"
    else:
        direction = "中性"
        stars = "★★☆☆☆"

    return direction, stars, reasons


def estimate_move(direction: str, theme: str, stocks: list):
    if not stocks:
        return "暂无明确个股，无法预估"

    if direction == "强利好":
        base = "+3%~+6%"
    elif direction == "中度利好":
        base = "+2%~+4%"
    elif direction == "轻度利好":
        base = "+1%~+3%"
    elif direction == "利空":
        base = "-1%~-4%"
    else:
        base = "区间震荡"

    if theme == "算力" and "利好" in direction:
        return f"{base}，高弹性题材可能更强"
    if theme == "新能源" and "利好" in direction:
        return f"{base}，偏板块中期催化"
    if theme == "金融" and "利好" in direction:
        return f"{base}，更偏指数与权重联动"
    return base


def analyze_news(text: str, stocks: list):
    theme = detect_theme(text)
    direction, stars, reasons = score_sentiment(text)
    est = estimate_move(direction, theme, stocks)

    logic = []
    if theme != "综合":
        logic.append(f"主题识别为{theme}")
    if reasons:
        logic.append("触发词：" + "、".join(reasons[:4]))
    if stocks:
        logic.append(f"命中{len(stocks)}只关联股票")
    else:
        logic.append("未命中明确关联股票")

    return {
        "theme": theme,
        "direction": direction,
        "stars": stars,
        "estimate": est,
        "logic": logic,
    }


def classify_channels(analysis: dict):
    theme = analysis.get("theme", "综合")

    channels = []

    if theme == "算力":
        channels.append("ai")
    elif theme == "新能源":
        channels.append("energy")
    elif theme == "金融":
        channels.append("finance")

    return channels


def send_to_channel(channel: str, msg: str):
    webhook = FEISHU_WEBHOOKS.get(channel)
    if not webhook:
        print(f"[{now_full()}] 未找到 channel={channel} 的 webhook")
        return

    data = {
        "msg_type": "text",
        "content": {"text": msg},
    }

    try:
        resp = session.post(webhook, json=data, headers=COMMON_HEADERS, timeout=(8, 12))
        print(f"[{now_full()}] 推送到 {channel} 状态 {resp.status_code}")
    except Exception as e:
        print(f"[{now_full()}] 推送到 {channel} 失败 {repr(e)}")


def build_message(source: str, line: str, stocks: list, analysis: dict) -> str:
    msg = f"【{source}】\n\n"
    msg += line + "\n\n"
    msg += f"事件方向：{analysis['direction']}\n"
    msg += f"事件强度：{analysis['stars']}\n"
    msg += f"主题板块：{analysis['theme']}\n"
    msg += f"预计影响：{analysis['estimate']}\n"

    if analysis["logic"]:
        msg += "逻辑分析：\n"
        for item in analysis["logic"]:
            msg += f"- {item}\n"

    msg += "\n关联股票：\n"
    if stocks:
        for s in stocks[:5]:
            q = get_quote(s["code"])
            if q:
                msg += f"- {s['name']}({s['code']}) / {q['price']} / {q['pct']}%\n"
            else:
                msg += f"- {s['name']}({s['code']})\n"
    else:
        msg += "- 未匹配股票\n"

    msg += "\n仅供信息整理参考，不构成投资建议。"
    return msg


def fetch_cls():
    resp = session.get(CLS_URL, headers=COMMON_HEADERS, timeout=(8, 15))
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    items = []
    for node in soup.select("div#tele span"):
        text = node.get_text(" ", strip=True)
        if "财联社" in text and len(text) > 20:
            items.append(("财联社", text))
    return items


def fetch_eastmoney():
    resp = session.get(EM_KX_URL, headers=COMMON_HEADERS, timeout=(8, 15))
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    items = []
    selectors = [".media-body", ".news-item", ".item", ".newsList li", ".list-item"]

    seen = set()
    for sel in selectors:
        for node in soup.select(sel):
            text = node.get_text(" ", strip=True)
            text = " ".join(text.split())
            if len(text) >= 18 and text not in seen:
                seen.add(text)
                items.append(("东方财富", text))

    return items[:100]


def fetch_10jqka():
    resp = session.get(THS_KX_URL, headers=COMMON_HEADERS, timeout=(8, 15))
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    items = []
    selectors = [".arc-cont", ".list-con li", ".news_list li", ".J-contents li", ".m-pager-list li"]

    seen = set()
    for sel in selectors:
        for node in soup.select(sel):
            text = node.get_text(" ", strip=True)
            text = " ".join(text.split())
            if len(text) >= 18 and text not in seen:
                seen.add(text)
                items.append(("同花顺", text))

    return items[:100]


def fetch_all():
    data = []

    try:
        data.extend(fetch_cls())
    except Exception as e:
        print(f"[{now_full()}] 财联社抓取失败 {repr(e)}")

    try:
        data.extend(fetch_eastmoney())
    except Exception as e:
        print(f"[{now_full()}] 东方财富抓取失败 {repr(e)}")

    try:
        data.extend(fetch_10jqka())
    except Exception as e:
        print(f"[{now_full()}] 同花顺抓取失败 {repr(e)}")

    uniq = []
    seen = set()
    for source, text in data:
        h = md5_text(f"{source}|{text}")
        if h not in seen:
            seen.add(h)
            uniq.append((source, text))

    return uniq


def save(items):
    conn = get_conn()
    cur = conn.cursor()

    new_count = 0
    pushed_count = 0

    for source, line in items:
        h = md5_text(f"{source}|{line}")
        stocks = match_stocks(line)
        analysis = analyze_news(line, stocks)
        channels = classify_channels(analysis)

        try:
            cur.execute(
                "INSERT INTO news(source,content,hash,related_stocks,analysis,channels,pushed,time) VALUES(?,?,?,?,?,?,?,?)",
                (
                    source,
                    line,
                    h,
                    json.dumps(stocks, ensure_ascii=False),
                    json.dumps(analysis, ensure_ascii=False),
                    json.dumps(channels, ensure_ascii=False),
                    0,
                    now_full(),
                )
            )
            news_id = cur.lastrowid
            new_count += 1

            if channels:
                msg = build_message(source, line, stocks, analysis)
                for ch in channels:
                    send_to_channel(ch, msg)
                cur.execute("UPDATE news SET pushed=1 WHERE id=?", (news_id,))
                pushed_count += 1
            else:
                print(f"[{now_full()}] 未分类到群，不推送: {line[:40]}")

        except sqlite3.IntegrityError:
            pass
        except Exception as e:
            print(f"[{now_full()}] 保存失败 {repr(e)}")

    conn.commit()
    conn.close()
    return new_count, pushed_count


def loop():
    while True:
        try:
            items = fetch_all()
            new_count, pushed_count = save(items)
            print(f"[{now_full()}] 新增 {new_count} 推送 {pushed_count}")
        except Exception as e:
            print(f"[{now_full()}] 主循环失败 {repr(e)}")

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
    cur.execute("SELECT source,content,time,pushed,related_stocks,analysis,channels FROM news ORDER BY id DESC LIMIT 200")
    rows = cur.fetchall()
    conn.close()

    html = """
    <html>
    <head>
      <meta charset="utf-8">
      <title>多群分类监控</title>
      <style>
        body { font-family: Arial, sans-serif; max-width: 1100px; margin: 20px auto; }
        .card { border: 1px solid #ddd; border-radius: 8px; padding: 12px; margin-bottom: 12px; }
        .time { color: #666; font-size: 13px; margin-bottom: 8px; }
        .stock { display: inline-block; padding: 3px 8px; border-radius: 12px; background: #f2f2f2; margin-right: 6px; margin-top: 6px; font-size: 13px; }
        .pushed { display: inline-block; padding: 2px 8px; border-radius: 10px; background: #d9f7be; font-size: 12px; margin-left: 8px; }
      </style>
    </head>
    <body>
      <h2>多群分类监控</h2>
      <p><a href="/refresh">手动抓取</a> | <a href="/test_push">测试三群</a></p>
      <hr>
    """

    for source, content, t, pushed, related_stocks, analysis_json, channels_json in rows:
        try:
            stock_list = json.loads(related_stocks) if related_stocks else []
        except Exception:
            stock_list = []

        try:
            analysis = json.loads(analysis_json) if analysis_json else {}
        except Exception:
            analysis = {}

        try:
            channels = json.loads(channels_json) if channels_json else []
        except Exception:
            channels = []

        status = '<span class="pushed">已推送</span>' if pushed == 1 else ""

        html += '<div class="card">'
        html += f'<div class="time">【{source}】 {t} {status} / 群: {",".join(channels) if channels else "无"}</div>'
        html += f"<div>{content}</div>"

        if analysis:
            html += f"<div style='margin-top:8px;'>方向：{analysis.get('direction','')} / 强度：{analysis.get('stars','')} / 主题：{analysis.get('theme','')} / 预计：{analysis.get('estimate','')}</div>"

        if stock_list:
            html += '<div style="margin-top:10px;">'
            for s in stock_list:
                q = get_quote(s["code"])
                if q:
                    html += f'<span class="stock">{s["name"]}({s["code"]}) / {q["price"]} / {q["pct"]}%</span>'
                else:
                    html += f'<span class="stock">{s["name"]}({s["code"]})</span>'
            html += "</div>"

        html += "</div>"

    html += "</body></html>"
    return html


@app.get("/refresh")
def refresh():
    items = fetch_all()
    new_count, pushed_count = save(items)
    print(f"[{now_full()}] 手动抓取 新增 {new_count} 推送 {pushed_count}")
    return RedirectResponse("/", status_code=302)


@app.get("/test_push")
def test_push():
    send_to_channel("ai", "【测试推送】AI算力群测试")
    send_to_channel("energy", "【测试推送】新能源群测试")
    send_to_channel("finance", "【测试推送】金融群测试")
    return {"ok": True}
