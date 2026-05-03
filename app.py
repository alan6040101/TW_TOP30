"""
台股成交金額 TOP 30
資料來源: FinMind API — TaiwanStockPrice (全市場當日成交)
"""

import streamlit as st
import pandas as pd
import requests
import re
import time
from datetime import datetime, timedelta, timezone

st.set_page_config(
    page_title="台股成交金額 TOP 30",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans+TC:wght@400;500;700&display=swap');

*, *::before, *::after { box-sizing:border-box; margin:0; padding:0; }
html, body, [data-testid="stAppViewContainer"] {
    background:#060b10 !important; color:#c8d6e5 !important;
    font-family:'IBM Plex Sans TC', sans-serif;
}
[data-testid="stHeader"]  { background:transparent !important; }
[data-testid="stSidebar"] { background:#0a1520 !important; border-right:1px solid #1a2940; }
section[data-testid="stSidebarContent"] * { color:#c8d6e5 !important; }
#MainMenu, footer, [data-testid="stToolbar"] { visibility:hidden; }
.block-container { padding:1.5rem 2rem 4rem !important; max-width:1440px; }

.topbar { display:flex; align-items:center; justify-content:space-between;
    padding:10px 0 18px; border-bottom:1px solid #1a2940; margin-bottom:20px; }
.topbar-logo { font-family:'IBM Plex Mono',monospace; font-size:20px; font-weight:600;
    color:#4fc3f7; letter-spacing:2px; }
.topbar-sub { font-size:11px; color:#4a6080; margin-top:2px; letter-spacing:1px; }
.status-pill { display:inline-flex; align-items:center; gap:7px;
    background:#0a1e12; border:1px solid #1a5c28; border-radius:4px;
    padding:5px 14px; font-family:'IBM Plex Mono',monospace; font-size:12px; color:#2ecc71; }
.status-pill.closed { background:#12131a; border-color:#2a3040; color:#5a6a80; }
.blink { animation:blink 1.2s step-start infinite; }
@keyframes blink { 50%{opacity:0;} }

.kpi-strip { display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-bottom:20px; }
.kpi { background:#0a1520; border:1px solid #1a2940; border-radius:4px; padding:14px 18px; }
.kpi-label { font-size:10px; color:#4a6080; letter-spacing:1.5px; text-transform:uppercase; margin-bottom:8px; }
.kpi-value { font-family:'IBM Plex Mono',monospace; font-size:24px; font-weight:600; }
.ratio-wrap { display:flex; align-items:baseline; gap:8px; margin-top:4px; }
.ratio-bar { height:6px; border-radius:3px; margin-top:8px;
    background: linear-gradient(to right,
        #e74c3c 0% var(--up), #2a3a50 var(--up) calc(100% - var(--dn)),
        #2ecc71 calc(100% - var(--dn)) 100%); }
.ratio-labels { display:flex; justify-content:space-between; font-size:10px;
    margin-top:4px; color:#4a6080; font-family:'IBM Plex Mono',monospace; }
.legend-strip { display:flex; align-items:center; gap:20px; flex-wrap:wrap;
    background:#0a1520; border:1px solid #1a2940; border-radius:4px;
    padding:8px 16px; font-size:11px; color:#4a6080; margin-bottom:14px; }
.leg { display:flex; align-items:center; gap:6px; }
.dot { width:8px; height:8px; border-radius:1px; }
.refresh-info { font-family:'IBM Plex Mono',monospace; font-size:11px; color:#2a3a50;
    text-align:right; margin-bottom:10px; }
.section-title { font-family:'IBM Plex Mono',monospace; font-size:12px; color:#4a6080;
    letter-spacing:2px; text-transform:uppercase; margin:24px 0 12px;
    display:flex; align-items:center; gap:10px; }
.section-title::after { content:''; flex:1; height:1px; background:#1a2940; }

div[data-testid="stRadio"] > div { display:flex !important; flex-direction:row !important;
    gap:0 !important; background:#0a1520; border:1px solid #1a2940; border-radius:4px;
    padding:2px; width:fit-content; }
div[data-testid="stRadio"] label { font-family:'IBM Plex Mono',monospace !important;
    font-size:12px !important; padding:6px 16px !important; border-radius:3px !important;
    cursor:pointer; color:#4a6080 !important; }
div[data-testid="stRadio"] label[data-checked="true"] { background:#1a2940 !important; color:#4fc3f7 !important; }
[data-testid="stDataFrame"] { border:1px solid #1a2940 !important; border-radius:4px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# TOKEN — 多種讀取方式確保相容
# ─────────────────────────────────────────────────────────────────────────────
def _read_token() -> str:
    """
    從 Streamlit secrets 讀取 FinMind token。
    相容以下 secrets.toml 格式:
      finmind_token = "xxx"          → st.secrets["finmind_token"]
      FINMIND_TOKEN = "xxx"          → st.secrets["FINMIND_TOKEN"]
      [finmind]                      → st.secrets["finmind"]["token"]
      token = "xxx"
    """
    keys = ["finmind_token", "FINMIND_TOKEN", "finmind_api_token",
            "FinMind_Token", "finmindtoken"]
    for k in keys:
        try:
            v = st.secrets.get(k, "")
            if v: return str(v).strip()
        except:
            pass
    # nested: [finmind] token = "xxx"
    try:
        v = st.secrets.get("finmind", {}).get("token", "")
        if v: return str(v).strip()
    except:
        pass
    return ""

TOKEN = _read_token()

# ─────────────────────────────────────────────────────────────────────────────
# TIME  — 全部用台灣時間 UTC+8
# ─────────────────────────────────────────────────────────────────────────────
TW_TZ = timezone(timedelta(hours=8))

def tw_now() -> datetime:
    return datetime.now(tz=TW_TZ).replace(tzinfo=None)

def last_trade_date() -> str:
    """最近一個盤後資料已完整的交易日（台灣時間）"""
    tw = tw_now()
    # 盤後資料 14:30 後才完整
    if tw.hour < 14 or (tw.hour == 14 and tw.minute < 30):
        tw -= timedelta(days=1)
    while tw.weekday() >= 5:   # 跳週末
        tw -= timedelta(days=1)
    return tw.strftime("%Y-%m-%d")

def is_market_open() -> bool:
    tw = tw_now()
    if tw.weekday() >= 5: return False
    t = tw.hour * 60 + tw.minute
    return 9 * 60 <= t <= 13 * 60 + 30

# ─────────────────────────────────────────────────────────────────────────────
# FINMIND API
# ─────────────────────────────────────────────────────────────────────────────
FINMIND_BASE = "https://api.finmindtrade.com/api/v4/data"

def _fm(dataset: str, extra: dict = None):
    """
    FinMind GET。回傳 data list 或 None。
    出錯時回傳 (None, error_string) 以便顯示。
    """
    if not TOKEN:
        return None, "TOKEN_NOT_SET"
    params = {"dataset": dataset, "token": TOKEN}
    if extra:
        params.update(extra)
    try:
        r   = requests.get(FINMIND_BASE, params=params, timeout=30)
        raw = r.json()
        # status 可能是 int 200 或字串 "200"
        status = int(str(raw.get("status", 0)))
        if status == 200:
            return raw.get("data") or [], None
        else:
            return None, f"status={status} msg={raw.get('msg','(無訊息)')}"
    except Exception as e:
        return None, str(e)

# ─────────────────────────────────────────────────────────────────────────────
# STOCK NAMES
# ─────────────────────────────────────────────────────────────────────────────
BUILT_IN_NAMES = {
    "2330":"台積電","2317":"鴻海","2454":"聯發科","3008":"大立光",
    "2382":"廣達","2308":"台達電","2303":"聯電","6505":"台塑化",
    "1301":"台塑","2002":"中鋼","2412":"中華電","2881":"富邦金",
    "2882":"國泰金","2886":"兆豐金","2891":"中信金","2884":"玉山金",
    "2885":"元大金","2890":"永豐金","2880":"華南金","2892":"第一金",
    "6669":"緯穎","3231":"緯創","2379":"瑞昱","2395":"研華",
    "2408":"南亞科","3034":"聯詠","2344":"華邦電","2357":"華碩",
    "4904":"遠傳","3045":"台灣大","2301":"光寶科","3481":"群創",
    "2345":"智邦","5876":"上海商銀","2603":"長榮","2609":"陽明",
    "2615":"萬海","5871":"中租-KY","3702":"大聯大","1303":"南亞",
    "2207":"和泰車","1102":"亞泥","1101":"台泥","2801":"彰銀",
    "6446":"藥華藥","2449":"京元電子","2376":"技嘉","2353":"宏碁",
    "2823":"中壽","2838":"聯邦銀","3673":"TPK-KY","2325":"矽品",
}

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_name_map() -> dict:
    data, _ = _fm("TaiwanStockInfo")
    if data:
        df   = pd.DataFrame(data)
        id_c = next((c for c in df.columns if "stock_id"   in c.lower()), None)
        nm_c = next((c for c in df.columns if "stock_name" in c.lower()), None)
        if id_c and nm_c:
            api_map = dict(zip(df[id_c].astype(str), df[nm_c].astype(str)))
            return {**BUILT_IN_NAMES, **api_map}
    return BUILT_IN_NAMES

# ─────────────────────────────────────────────────────────────────────────────
# CB 可轉債
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_cb_stocks() -> set:
    codes = set()
    data, _ = _fm("TaiwanStockConvertibleBond")
    if data:
        df   = pd.DataFrame(data)
        id_c = next((c for c in df.columns if "stock_id" in c.lower()), None)
        if id_c:
            for v in df[id_c].astype(str):
                if re.match(r"^\d{4}$", v.strip()):
                    codes.add(v.strip())
    return codes

# ─────────────────────────────────────────────────────────────────────────────
# TOP 30 成交金額  — 只用 FinMind TaiwanStockPrice
# ─────────────────────────────────────────────────────────────────────────────
def _n(v, d=0.0):
    try:
        s = str(v).replace(",","").replace("+","").strip()
        return float(s) if s not in ("","--","-","N/A","nan","None","X","null") else d
    except: return d


@st.cache_data(ttl=170, show_spinner=False)  # 略小於 3 分鐘，確保按時更新
def fetch_top30(trade_date: str) -> tuple:
    """
    回傳 (df, error_msg)
    df 欄位: rank, code, name, trade_value(億), change_pct
    """
    name_map = fetch_name_map()

    data, err = _fm("TaiwanStockPrice", {"date": trade_date})

    if data is None:
        return pd.DataFrame(), err

    if len(data) == 0:
        # 可能是假日或資料尚未更新，試前一個交易日
        tw = datetime.strptime(trade_date, "%Y-%m-%d")
        tw -= timedelta(days=1)
        while tw.weekday() >= 5:
            tw -= timedelta(days=1)
        prev_date = tw.strftime("%Y-%m-%d")
        data, err2 = _fm("TaiwanStockPrice", {"date": prev_date})
        if not data:
            return pd.DataFrame(), f"今日({trade_date})與前日({prev_date})均無資料: {err2}"
        trade_date = prev_date  # 更新為實際使用日期

    raw = pd.DataFrame(data)

    # 確認欄位
    need = {"stock_id", "Trading_money", "close", "spread"}
    if not need.issubset(raw.columns):
        missing = need - set(raw.columns)
        return pd.DataFrame(), f"FinMind 欄位不符，缺少: {missing}，實際欄位: {list(raw.columns)}"

    raw["code"]   = raw["stock_id"].astype(str).str.strip()
    raw["tv"]     = raw["Trading_money"].apply(_n) / 1e8
    raw["close_n"]= raw["close"].apply(_n)
    raw["spd"]    = raw["spread"].apply(_n)
    raw["prev"]   = raw["close_n"] - raw["spd"]
    raw["chg"]    = raw.apply(
        lambda r: round(r["spd"] / r["prev"] * 100, 2) if r["prev"] > 0 else 0.0, axis=1)
    raw["name"]   = raw["code"].map(name_map).fillna(raw["code"])

    # 只保留 4 碼股票代號 & 有成交金額
    df = raw[raw["code"].str.match(r"^\d{4}$") & (raw["tv"] > 0)].copy()
    df = df.sort_values("tv", ascending=False).head(30).reset_index(drop=True)
    df["rank"] = range(1, len(df) + 1)

    return df.rename(columns={"tv":"trade_value","chg":"change_pct"})[
        ["rank","code","name","trade_value","change_pct"]], None

# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────────────────
GOOGLE_SHEETS_ID = "1lTRxbT9iv3uRIrQ1wUFPV0LZbo8p5Qoh4a1FMGa-iU4"

@st.cache_resource(show_spinner=False)
def gs_client():
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        cfg = st.secrets.get("gcp_service_account")
        if cfg:
            creds = Credentials.from_service_account_info(
                dict(cfg), scopes=["https://www.googleapis.com/auth/spreadsheets",
                                   "https://www.googleapis.com/auth/drive"])
            return gspread.authorize(creds)
    except: pass
    return None

def save_today(client, df, date_key):
    if not client or len(df) == 0: return
    try:
        import gspread
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        try:    ws = sh.worksheet(date_key); ws.clear()
        except gspread.WorksheetNotFound: ws = sh.add_worksheet(date_key, 35, 5)
        out = df[["code","name","trade_value","change_pct"]].copy()
        out.columns = ["代號","名稱","成交金額(億)","漲跌幅(%)"]
        out["成交金額(億)"] = out["成交金額(億)"].round(2)
        out["漲跌幅(%)"]   = out["漲跌幅(%)"].round(2)
        ws.update([out.columns.tolist()] + out.values.tolist())
    except: pass

def load_prev_codes(client, today_key) -> set:
    if not client: return set()
    try:
        sh    = client.open_by_key(GOOGLE_SHEETS_ID)
        valid = sorted(ws.title for ws in sh.worksheets()
                       if re.match(r"^\d{4}-\d{2}-\d{2}$", ws.title) and ws.title < today_key)
        if not valid: return set()
        recs = sh.worksheet(valid[-1]).get_all_records()
        return {str(r.get("代號","")).strip() for r in recs if r.get("代號")}
    except: return set()

def load_history(client) -> dict:
    if not client: return {}
    out = {}
    try:
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        for ws in sorted(
            [w for w in sh.worksheets() if re.match(r"^\d{4}-\d{2}-\d{2}$", w.title)],
            key=lambda x: x.title, reverse=True)[:60]:
            try:
                recs = ws.get_all_records()
                if recs: out[ws.title] = pd.DataFrame(recs)
            except: pass
    except: pass
    return out

# ─────────────────────────────────────────────────────────────────────────────
# TABLE  — 只顯示 排行 / 股票名稱 / 漲跌幅 / 成交金額(億)
# ─────────────────────────────────────────────────────────────────────────────
def build_table(df, prev_codes, cb_codes, extra=None):
    rows, pcts, news = [], [], []
    for _, r in df.iterrows():
        code   = str(r["code"]).strip()
        pct    = float(r.get("change_pct") or 0)
        is_new = bool(prev_codes) and (code not in prev_codes)
        has_cb = code in cb_codes
        tags   = (["★新"] if is_new else []) + (["CB"] if has_cb else [])
        name   = str(r["name"]) + ("  " + "  ".join(tags) if tags else "")
        pstr   = (f"▲ {pct:.2f}%" if pct > 0 else f"▼ {abs(pct):.2f}%" if pct < 0 else "─")
        row_d  = {"排行": int(r.get("rank", _+1)), "股票名稱": name,
                  "漲跌幅": pstr, "成交金額(億)": float(r.get("trade_value", 0))}
        if extra:
            for s, d2 in extra: row_d[d2] = r.get(s, "")
        rows.append(row_d); pcts.append(pct); news.append(is_new)

    disp = pd.DataFrame(rows)
    disp["__p"] = pcts
    disp["__n"] = news

    def rbg(row):
        bg = ("#191000" if row["__n"] else
              "#1a0808" if row["__p"] > 0 else
              "#041008" if row["__p"] < 0 else "#0a1520")
        return [bg] * len(row)
    def cpct(col):
        return ["color:#e74c3c;font-weight:600" if v > 0
                else ("color:#2ecc71;font-weight:600" if v < 0 else "color:#5a6a80")
                for v in disp["__p"]]
    def cname(col):
        s = []
        for i, v in enumerate(disp["股票名稱"]):
            n = disp["__n"].iloc[i]; cb = "CB" in str(v)
            s.append("color:#f39c12;font-weight:700" if n
                     else "color:#a78bfa" if cb else "color:#c8d6e5")
        return s
    def crank(col):
        return ["color:#4fc3f7;font-weight:700" if v <= 3 else "color:#4a6080"
                for v in disp["排行"]]

    fmt = {"成交金額(億)": "{:,.1f}"}
    if extra:
        for _, d2 in extra:
            if "億" in d2: fmt[d2] = "{:,.1f}"

    return (
        disp.style
        .apply(rbg, axis=1).apply(cpct, subset=["漲跌幅"])
        .apply(cname, subset=["股票名稱"]).apply(crank, subset=["排行"])
        .format(fmt)
        .set_properties(**{"font-family":"'IBM Plex Mono',monospace","font-size":"13px","border":"none"})
        .set_properties(subset=["排行"],                       **{"text-align":"center"})
        .set_properties(subset=["股票名稱"],                    **{"text-align":"left"})
        .set_properties(subset=["漲跌幅","成交金額(億)"],       **{"text-align":"right"})
        .set_table_styles([
            {"selector":"thead th","props":[
                ("background-color","#0a1520"),("color","#4a6080"),
                ("font-family","'IBM Plex Mono',monospace"),("font-size","11px"),
                ("letter-spacing","1.5px"),("text-transform","uppercase"),
                ("border-bottom","1px solid #1a2940"),("padding","10px 14px")]},
            {"selector":"tbody td","props":[
                ("padding","10px 14px"),("border-bottom","1px solid #0d1a28")]},
            {"selector":"tbody tr:hover td","props":[("filter","brightness(1.3)")]},
            {"selector":"table","props":[("width","100%"),("border-collapse","collapse")]},
        ])
        .hide(axis="index")
        .hide(subset=["__p","__n"], axis="columns")
    )

# ─────────────────────────────────────────────────────────────────────────────
# SHARED UI
# ─────────────────────────────────────────────────────────────────────────────
def render_kpi(df, prev_codes, cb_codes):
    up  = int((df["change_pct"] > 0).sum())
    dn  = int((df["change_pct"] < 0).sum())
    nc  = len(df) - up - dn
    new_c = sum(1 for c in df["code"].astype(str) if prev_codes and c not in prev_codes)
    cb_c  = sum(1 for c in df["code"].astype(str) if c in cb_codes)
    tot   = len(df) or 1
    up_p  = round(up/tot*100); dn_p = round(dn/tot*100); nc_p = 100 - up_p - dn_p
    st.markdown(f"""
    <div class="kpi-strip">
        <div class="kpi"><div class="kpi-label">上榜股數</div>
            <div class="kpi-value" style="color:#4fc3f7">{tot}</div></div>
        <div class="kpi"><div class="kpi-label">漲 / 平 / 跌</div>
            <div class="ratio-wrap">
                <span class="kpi-value" style="color:#e74c3c">{up}</span>
                <span style="color:#4a6080;font-size:20px">/</span>
                <span class="kpi-value" style="color:#5a6a80;font-size:20px">{nc}</span>
                <span style="color:#4a6080;font-size:20px">/</span>
                <span class="kpi-value" style="color:#2ecc71">{dn}</span>
            </div>
            <div class="ratio-bar" style="--up:{up_p}%;--dn:{dn_p}%"></div>
            <div class="ratio-labels">
                <span style="color:#e74c3c">▲ {up_p}%</span>
                <span>─ {nc_p}%</span>
                <span style="color:#2ecc71">▼ {dn_p}%</span>
            </div></div>
        <div class="kpi"><div class="kpi-label">新上榜 ★</div>
            <div class="kpi-value" style="color:#f39c12">{new_c}</div></div>
        <div class="kpi"><div class="kpi-label">CB 上榜</div>
            <div class="kpi-value" style="color:#a78bfa">{cb_c}</div></div>
    </div>""", unsafe_allow_html=True)

def render_legend():
    st.markdown("""
    <div class="legend-strip">
        <span style="color:#4a6080;font-size:10px;letter-spacing:1px;text-transform:uppercase">圖例</span>
        <div class="leg"><div class="dot" style="background:#4a1212"></div><span style="color:#e74c3c">上漲</span></div>
        <div class="leg"><div class="dot" style="background:#0a3018"></div><span style="color:#2ecc71">下跌</span></div>
        <div class="leg"><div class="dot" style="background:#3a2500"></div>
            <span style="color:#f39c12">★新 = 今日新上榜</span></div>
        <div class="leg" style="color:#a78bfa">CB = 已發行可轉債</div>
    </div>""", unsafe_allow_html=True)

def _prep_hist(raw):
    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    rm = {}
    for c in df.columns:
        if c in ("代號","code","stock_id"):         rm[c] = "code"
        elif c in ("名稱","name","stock_name"):      rm[c] = "name"
        elif "成交金額" in c or c == "trade_value":  rm[c] = "trade_value"
        elif "漲跌幅"   in c or c == "change_pct":  rm[c] = "change_pct"
    df = df.rename(columns=rm)
    for c in ["code","name","trade_value","change_pct"]:
        if c not in df.columns:
            df[c] = "" if c in ["code","name"] else 0.0
    df["trade_value"] = pd.to_numeric(df["trade_value"], errors="coerce").fillna(0)
    df["change_pct"]  = pd.to_numeric(df["change_pct"],  errors="coerce").fillna(0)
    df["rank"] = range(1, len(df)+1)
    return df

# ─────────────────────────────────────────────────────────────────────────────
# PAGE 1 – 即時排行
# ─────────────────────────────────────────────────────────────────────────────
def page_realtime():
    tw       = tw_now()
    open_    = is_market_open()
    trade_d  = last_trade_date()
    today_k  = tw.strftime("%Y-%m-%d")

    pill = "status-pill" if open_ else "status-pill closed"
    plbl = '<span class="blink">●</span> 盤中即時' if open_ else "● 非交易時段"
    st.markdown(f"""
    <div class="topbar">
        <div><div class="topbar-logo">台股成交金額 TOP 30</div>
             <div class="topbar-sub">TWSE · DAILY VOLUME LEADERS</div></div>
        <div style="text-align:right">
            <div class="{pill}">{plbl}</div>
            <div style="font-family:'IBM Plex Mono',monospace;font-size:11px;color:#2a3a50;margin-top:6px">
                {tw.strftime("%Y/%m/%d &nbsp; %H:%M:%S")} (台灣時間)</div>
        </div>
    </div>""", unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### ⚙️ 控制面板")
        if st.button("⟳ 立即刷新", use_container_width=True):
            st.cache_data.clear(); st.rerun()
        st.markdown("---")
        st.markdown(f"**台灣時間**  \n`{tw.strftime('%H:%M:%S')}`")
        st.markdown(f"**查詢日期**  \n`{trade_d}`")
        st.markdown(f"**Token**  \n{'✅ 已設定' if TOKEN else '❌ 未設定'}")
        if TOKEN:
            st.caption(f"Token 前8碼: `{TOKEN[:8]}...`")

    # 載入資料
    with st.spinner("載入成交資料…"):
        df, err = fetch_top30(trade_d)

    if df is None or len(df) == 0:
        st.error(f"❌ 資料載入失敗")
        st.code(f"Token 狀態: {'已設定 (' + TOKEN[:8] + '...)' if TOKEN else '未設定'}\n"
                f"查詢日期: {trade_d}\n"
                f"錯誤訊息: {err}", language=None)
        st.info(
            "**請確認 Streamlit Cloud secrets.toml 格式：**\n\n"
            "```toml\n"
            "finmind_token = \"你的token\"\n"
            "```\n\n"
            "Token 在 [finmindtrade.com](https://finmindtrade.com) → 個人資料 → API Token"
        )
        return

    cb_codes   = fetch_cb_stocks()
    client     = gs_client()
    prev_codes = load_prev_codes(client, today_k)

    # 每天收盤後儲存一次
    if tw.hour >= 14:
        save_today(client, df, today_k)

    render_kpi(df, prev_codes, cb_codes)
    render_legend()
    st.markdown(
        f'<div class="refresh-info">資料日期: {trade_d} &nbsp;·&nbsp; '
        f'更新時間: {tw.strftime("%H:%M:%S")} &nbsp;·&nbsp; 每 3 分鐘自動刷新</div>',
        unsafe_allow_html=True)

    st.dataframe(build_table(df, prev_codes, cb_codes),
                 use_container_width=True, height=980, hide_index=True)

    if not open_:
        st.info(f"📌 非交易時段，顯示 {trade_d} 收盤資料。")

    # 每 3 分鐘自動刷新
    time.sleep(180)
    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 2 – 歷史排行
# ─────────────────────────────────────────────────────────────────────────────
def page_history():
    st.markdown("""<div class="topbar"><div>
        <div class="topbar-logo">歷史成交金額排行</div>
        <div class="topbar-sub">HISTORICAL VOLUME LEADERS</div>
    </div></div>""", unsafe_allow_html=True)

    client   = gs_client()
    history  = load_history(client)
    cb_codes = fetch_cb_stocks()

    if not history:
        st.warning("尚無歷史資料。請確認 Google Sheets API 已設定，且系統曾在交易日 14:00 後完成儲存。")
        return

    dates      = sorted(history.keys(), reverse=True)
    all_sorted = sorted(history.keys())

    c1, c2 = st.columns([3, 1])
    with c1:
        selected = st.multiselect("選擇日期（可多選）", dates,
                                  default=dates[:7] if len(dates) >= 7 else dates)
    with c2:
        mode = st.radio("模式", ["每日明細","彙總排行"], horizontal=True)

    if not selected:
        st.info("請選擇日期"); return

    if mode == "每日明細":
        for date in sorted(selected, reverse=True):
            raw = history.get(date)
            if raw is None or len(raw) == 0: continue
            df     = _prep_hist(raw)
            prev_c = set()
            idx    = all_sorted.index(date) if date in all_sorted else -1
            if idx > 0:
                try: prev_c = set(_prep_hist(history[all_sorted[idx-1]])["code"].astype(str))
                except: pass
            with st.expander(f"📅 {date}", expanded=(date == sorted(selected, reverse=True)[0])):
                render_kpi(df, prev_c, cb_codes)
                render_legend()
                st.dataframe(build_table(df, prev_c, cb_codes),
                             use_container_width=True, height=600, hide_index=True)

    else:  # 彙總排行
        all_rows = []
        for d in selected:
            raw = history.get(d)
            if raw is None: continue
            tmp = _prep_hist(raw); tmp["_d"] = d; all_rows.append(tmp)
        if not all_rows:
            st.warning("無有效資料"); return

        combined = pd.concat(all_rows, ignore_index=True)
        agg = (combined.groupby(["code","name"])
               .agg(avg_val=("trade_value","mean"), total_val=("trade_value","sum"),
                    days=("trade_value","count"),   avg_pct=("change_pct","mean"))
               .reset_index()
               .sort_values("total_val", ascending=False).head(30).reset_index(drop=True))
        agg["rank"]        = range(1, len(agg)+1)
        agg["trade_value"] = agg["avg_val"]
        agg["change_pct"]  = agg["avg_pct"].round(2)

        sel_s  = sorted(selected)
        prev_c = set()
        if sel_s[0] in all_sorted:
            i = all_sorted.index(sel_s[0])
            if i > 0:
                try: prev_c = set(_prep_hist(history[all_sorted[i-1]])["code"].astype(str))
                except: pass

        period = f"{sel_s[0]} ~ {sel_s[-1]}" if len(sel_s) > 1 else sel_s[0]
        st.markdown(f'<div class="section-title">彙總 · {period} · {len(sel_s)} 交易日</div>',
                    unsafe_allow_html=True)
        render_legend()
        extra = [("avg_val","平均成交(億)"),("total_val","累積成交(億)"),("days","上榜天數")]
        st.dataframe(build_table(agg, prev_c, cb_codes, extra=extra),
                     use_container_width=True, height=700, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    page = st.radio("nav", ["📈  即時排行", "📊  歷史排行"],
                    horizontal=True, label_visibility="collapsed")
    if "即時" in page: page_realtime()
    else:              page_history()

if __name__ == "__main__":
    main()
