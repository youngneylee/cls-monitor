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

URL = "https://m.cls.cn/telegraph"
DB = "cls_news.db"

FETCH_INTERVAL = 60
QUOTE_INTERVAL = 45

FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/36d76c0a-d013-455a-9017-c13f259c7b5e"


session = requests.Session()
session.trust_env = False

retry = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=1,
)

adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)


COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

QUOTE_CACHE = {}

STOCKS = [
    {"code": "600875", "name": "东方电气", "aliases": ["东方电气"], "concepts": ["风电","核电"]},
    {"code": "605196", "name": "华通线缆", "aliases": ["华通线缆"], "concepts": ["电缆"]},
    {"code": "002837", "name": "英维克", "aliases": ["英维克"], "concepts": ["算力","液冷"]},
    {"code": "002056", "name": "横店东磁", "aliases": ["横店东磁"], "concepts": ["光伏","储能"]},
]


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def fp(text):
    return hashlib.md5(text.encode()).hexdigest()


def conn():
    return sqlite3.connect(DB, check_same_thread=False)


def init_db():
    c = conn()
    cur = c.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS news(
    id INTEGER PRIMARY KEY,
    content TEXT,
    hash TEXT UNIQUE,
    related TEXT,
    pushed INTEGER,
    time TEXT)
    """)

    c.commit()
    c.close()


# 行情
def update_quotes():
    global QUOTE_CACHE

    url = "https://82.push2.eastmoney.com/api/qt/clist/get"

    params = {
        "pn":"1",
        "pz":"5000",
        "po":"1",
        "np":"1",
        "ut":"bd1d9ddb04089700cf9c27f6f7426281",
        "fltt":"2",
        "invt":"2",
        "fid":"f12",
        "fs":"m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23",
        "fields":"f2,f3,f12,f14"
    }

    while True:
        try:
            r = session.get(url,params=params,timeout=10)
            data = r.json()

            cache = {}

            for i in data["data"]["diff"]:
                code=i["f12"]
                cache[code]={
                    "price":i["f2"],
                    "pct":i["f3"],
                    "name":i["f14"]
                }

            QUOTE_CACHE = cache
            print(now(),"行情更新",len(cache))

        except Exception as e:
            print("行情失败",e)

        time.sleep(QUOTE_INTERVAL)


def get_quote(code):
    return QUOTE_CACHE.get(code)


# 抓财联社
def fetch():

    r=session.get(URL,headers=COMMON_HEADERS)
    r.encoding="utf-8"

    soup=BeautifulSoup(r.text,"html.parser")

    res=[]

    for i in soup.select("#tele span"):
        t=i.get_text(strip=True)
        if "财联社" in t:
            res.append(t)

    return res


def match(text):

    res=[]

    for s in STOCKS:

        for a in s["aliases"]:
            if a in text:
                res.append(s)

        for c in s["concepts"]:
            if c in text:
                res.append(s)

    return res


def send(msg):

    data={
        "msg_type":"text",
        "content":{"text":msg}
    }

    try:
        session.post(FEISHU_WEBHOOK,json=data,timeout=10)
    except:
        pass


def build(line,stocks):

    msg="【财联社监控】\n\n"
    msg+=line+"\n\n"

    if stocks:

        msg+="关联股票：\n"

        for s in stocks:

            q=get_quote(s["code"])

            if q:
                msg+=f"{s['name']} {q['pct']}%\n"
            else:
                msg+=f"{s['name']}\n"

    else:
        msg+="未匹配股票\n"

    return msg


def save(lines):

    c=conn()
    cur=c.cursor()

    new=0
    push=0

    for l in lines:

        h=fp(l)
        stocks=match(l)

        try:

            cur.execute(
            "insert into news values(null,?,?,?,?)",
            (l,h,json.dumps(stocks),0,now())
            )

            new+=1

            # 全部推送
            msg=build(l,stocks)
            send(msg)

            push+=1

        except:
            pass

    c.commit()
    c.close()

    return new,push


def loop():

    while True:

        try:
            lines=fetch()
            n,p=save(lines)

            print(now(),"新增",n,"推送",p)

        except Exception as e:
            print("错误",e)

        time.sleep(FETCH_INTERVAL)


@app.on_event("startup")
def start():

    init_db()

    threading.Thread(target=loop).start()
    threading.Thread(target=update_quotes).start()


@app.get("/",response_class=HTMLResponse)
def home():

    c=conn()
    cur=c.cursor()

    cur.execute("select content,time,pushed from news order by id desc limit 200")
    rows=cur.fetchall()

    c.close()

    html="<html><body>"
    html+="<h2>财联社监控</h2><hr>"

    for r in rows:

        t=r[1]
        txt=r[0]

        html+=f"<div><b>{t}</b><br>{txt}</div><hr>"

    html+="</body></html>"

    return html


@app.get("/refresh")
def refresh():

    lines=fetch()
    save(lines)

    return {"ok":True}
