"""
台股成交金額 TOP 30 追蹤系統

主要資料源: FinMind API (token 必填)
  - TaiwanStockPrice  全市場當日成交
  - TaiwanStockConvertibleBond  CB 可轉債
  - TaiwanStockInfo  股票名稱

備援: TWSE STOCK_DAY_ALL / MI_INDEX20 / Yahoo Finance

重要: Streamlit Cloud 使用 UTC 時區，所有時間判斷皆轉成台灣時間 (UTC+8)
"""

import streamlit as st
import pandas as pd
import requests
import time
import re
from datetime import datetime, timedelta, timezone

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
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
        #e74c3c 0% var(--up),
        #2a3a50 var(--up) calc(100% - var(--dn)),
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
.src-tag { display:inline-block; background:#0d1e30; border:1px solid #1a4060;
    border-radius:3px; padding:2px 8px; font-size:10px; color:#4a8ab8;
    font-family:'IBM Plex Mono',monospace; letter-spacing:1px; margin-left:8px; }
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
div[data-testid="stRadio"] label[data-checked="true"] {
    background:#1a2940 !important; color:#4fc3f7 !important; }
[data-testid="stDataFrame"] { border:1px solid #1a2940 !important; border-radius:4px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG & CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
GOOGLE_SHEETS_ID = "1lTRxbT9iv3uRIrQ1wUFPV0LZbo8p5Qoh4a1FMGa-iU4"
FINMIND_BASE     = "https://api.finmindtrade.com/api/v4/data"
FINMIND_TOKEN    = st.secrets.get("finmind_token", "")

TW_TZ   = timezone(timedelta(hours=8))
TWSE_H  = {"User-Agent":"Mozilla/5.0","Referer":"https://www.twse.com.tw/","Accept":"application/json"}
MIS_H   = {"User-Agent":"Mozilla/5.0","Referer":"https://mis.twse.com.tw/","Accept":"application/json"}
YAHOO_H = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36","Accept":"application/json"}

# ─────────────────────────────────────────────────────────────────────────────
# TIME HELPERS  — 全程用台灣時間 UTC+8
# ─────────────────────────────────────────────────────────────────────────────
def tw_now() -> datetime:
    """台灣當前時間 (UTC+8)，相容各種環境"""
    return datetime.now(tz=TW_TZ).replace(tzinfo=None)


def last_trade_date() -> str:
    """
    最近一個盤後資料已完整的交易日 (台灣時間)。
    盤後資料 14:30 後才完整，14:30 前使用前一個交易日。
    """
    tw = tw_now()
    # 14:30 前仍用前一日
    if tw.hour < 14 or (tw.hour == 14 and tw.minute < 30):
        tw -= timedelta(days=1)
    # 跳過週末（台灣股市週六日休市）
    while tw.weekday() >= 5:   # 5=Sat, 6=Sun
        tw -= timedelta(days=1)
    return tw.strftime("%Y-%m-%d")


def is_market_open() -> bool:
    """判斷目前是否在盤中 09:00–13:30 台灣時間"""
    tw = tw_now()
    if tw.weekday() >= 5:
        return False
    t = tw.hour * 60 + tw.minute
    return 9 * 60 <= t <= 13 * 60 + 30


# ─────────────────────────────────────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────────────────────────────────────
def _n(val, d=0.0):
    """Safe numeric parse"""
    try:
        s = str(val).replace(",","").replace("+","").strip()
        return float(s) if s not in ("","--","-","N/A","nan","None","X","null") else d
    except:
        return d


def _fm_get(dataset: str, extra: dict = None, log_errors: bool = True):
    """FinMind API GET。回傳 data list 或 None"""
    if not FINMIND_TOKEN:
        return None
    params = {"dataset": dataset, "token": FINMIND_TOKEN}
    if extra:
        params.update({k: v for k, v in extra.items() if v is not None})
    try:
        r   = requests.get(FINMIND_BASE, params=params, timeout=25)
        raw = r.json()
        # FinMind status 可能是 int 200 或字串 "200"
        status = int(raw.get("status", 0))
        if status == 200:
            return raw.get("data") or []
        else:
            if log_errors:
                _err_log(f"FinMind {dataset}: HTTP={r.status_code} status={status} msg={raw.get('msg','')}")
            return None
    except Exception as e:
        if log_errors:
            _err_log(f"FinMind {dataset} exception: {e}")
        return None


# 全域錯誤日誌（在頁面上顯示）
if "api_errors" not in st.session_state:
    st.session_state.api_errors = []

def _err_log(msg: str):
    st.session_state.api_errors.append(f"[{tw_now().strftime('%H:%M:%S')}] {msg}")
    if len(st.session_state.api_errors) > 20:
        st.session_state.api_errors = st.session_state.api_errors[-20:]


# ─────────────────────────────────────────────────────────────────────────────
# STOCK NAME MAP
# ─────────────────────────────────────────────────────────────────────────────
FALLBACK_NAMES = {
    "2330":"台積電","2317":"鴻海","2454":"聯發科","3008":"大立光",
    "2382":"廣達","2308":"台達電","2303":"聯電","6505":"台塑化",
    "1301":"台塑","2002":"中鋼","2412":"中華電","2881":"富邦金",
    "2882":"國泰金","2886":"兆豐金","2891":"中信金","2884":"玉山金",
    "2885":"元大金","2890":"永豐金","2880":"華南金","2892":"第一金",
    "6669":"緯穎","3231":"緯創","2379":"瑞昱","2395":"研華",
    "2408":"南亞科","3034":"聯詠","2344":"華邦電","2357":"華碩",
    "4904":"遠傳","3045":"台灣大","2301":"光寶科","3481":"群創",
    "2345":"智邦","5876":"上海商銀","2325":"矽品","2353":"宏碁",
    "2376":"技嘉","2603":"長榮","2609":"陽明","2615":"萬海",
    "5871":"中租-KY","3673":"TPK-KY","3702":"大聯大","1303":"南亞",
    "2207":"和泰車","1102":"亞泥","1101":"台泥","2801":"彰銀",
    "2823":"中壽","6446":"藥華藥","2408":"南亞科","2449":"京元電子",
}

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_name_map() -> dict:
    data = _fm_get("TaiwanStockInfo", log_errors=False)
    if data:
        df   = pd.DataFrame(data)
        id_c = next((c for c in df.columns if "stock_id"   in c.lower()), None)
        nm_c = next((c for c in df.columns if "stock_name" in c.lower()), None)
        if id_c and nm_c:
            m = dict(zip(df[id_c].astype(str), df[nm_c].astype(str)))
            return {**FALLBACK_NAMES, **m}
    return FALLBACK_NAMES


# ─────────────────────────────────────────────────────────────────────────────
# CB 可轉債
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_cb_stocks() -> set:
    codes = set()

    # FinMind
    data = _fm_get("TaiwanStockConvertibleBond", log_errors=False)
    if data:
        df   = pd.DataFrame(data)
        id_c = next((c for c in df.columns if "stock_id" in c.lower()), None)
        if id_c:
            for v in df[id_c].astype(str):
                if re.match(r"^\d{4}$", v.strip()):
                    codes.add(v.strip())
        if codes:
            return codes

    # TWSE fallback
    for url in ["https://www.twse.com.tw/rwd/zh/cbInfo/CB_OVERVIEW",
                "https://www.twse.com.tw/rwd/zh/cbInfo/CB_BOND_INFO"]:
        try:
            r = requests.get(url, headers=TWSE_H, timeout=12)
            d = r.json()
            if d.get("stat") == "OK":
                for row in d.get("data",[]):
                    for cell in row:
                        s = str(cell).strip()
                        if re.match(r"^\d{4}$", s):
                            codes.add(s)
                if codes:
                    return codes
        except:
            pass
    return codes


# ─────────────────────────────────────────────────────────────────────────────
# TOP 30 成交金額  — 5 層 fallback
# ─────────────────────────────────────────────────────────────────────────────
def _to_top30(raw: pd.DataFrame, code_c, tv_c, close_c, spread_c,
              name_c=None, tv_unit=1.0, name_map=None) -> pd.DataFrame:
    df             = raw.copy()
    df["code"]     = df[code_c].astype(str).str.strip()
    df["tv"]       = df[tv_c].apply(_n) * tv_unit
    close          = df[close_c].apply(_n)  if close_c  else pd.Series(0, index=df.index)
    spread         = df[spread_c].apply(_n) if spread_c else pd.Series(0, index=df.index)
    prev           = close - spread
    df["chg_pct"]  = (spread / prev.replace(0, float("nan")) * 100).round(2).fillna(0)
    df["name"]     = (df[name_c].astype(str) if name_c
                      else df["code"].map(name_map or {}).fillna(df["code"]))
    if name_map:
        df["name"] = df["code"].map(name_map).fillna(df["name"])
    df = df[df["code"].str.match(r"^\d{4}$") & (df["tv"] > 0)]
    df = df.sort_values("tv", ascending=False).head(30).reset_index(drop=True)
    df["rank"] = range(1, len(df)+1)
    return df.rename(columns={"tv":"trade_value","chg_pct":"change_pct"})[
        ["rank","code","name","trade_value","change_pct"]]


@st.cache_data(ttl=55, show_spinner=False)
def fetch_top30(trade_date: str) -> tuple:
    """回傳 (df, source_label)。trade_date: YYYY-MM-DD"""
    name_map = fetch_name_map()
    errors   = []

    # ══ 1. FinMind TaiwanStockPrice ══
    if FINMIND_TOKEN:
        data = _fm_get("TaiwanStockPrice", {"date": trade_date})
        if data:
            raw = pd.DataFrame(data)
            # 預期欄位: stock_id, Trading_money, close, spread
            need = {"stock_id","Trading_money","close","spread"}
            if need.issubset(raw.columns):
                raw["tv_yi"] = raw["Trading_money"].apply(_n) / 1e8
                df = _to_top30(raw,"stock_id","tv_yi","close","spread",name_map=name_map)
                if len(df) >= 10:
                    return df, "FinMind TaiwanStockPrice"
                else:
                    errors.append(f"FinMind: 只有 {len(df)} 筆 (期望>=10)")
            else:
                missing = need - set(raw.columns)
                errors.append(f"FinMind 缺欄: {missing}, 現有: {list(raw.columns)[:8]}")
        else:
            errors.append(f"FinMind TaiwanStockPrice 回傳 None (date={trade_date})")

    # ══ 2. TWSE STOCK_DAY_ALL ══
    try:
        r = requests.get("https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL",
                         headers=TWSE_H, timeout=15)
        d = r.json()
        if d.get("stat")=="OK" and d.get("data"):
            rows  = d["data"]; ncols = len(rows[0])
            base  = ["code","name","vol","txn","tv_raw","open","high","low","close","sign","chg","pe"]
            cols  = base[:ncols] if ncols<=len(base) else base+[f"x{i}" for i in range(ncols-len(base))]
            raw   = pd.DataFrame(rows, columns=cols)
            raw["tv_yi"]  = raw["tv_raw"].apply(_n)/1e8
            raw["sign_n"] = raw["sign"].astype(str).str.strip().map({"+":1,"-":-1}).fillna(0)
            raw["chg_n"]  = raw["chg"].apply(_n)*raw["sign_n"]
            raw["prev"]   = raw["close"].apply(_n)-raw["chg_n"]
            raw["cpct"]   = raw.apply(lambda r: round(r["chg_n"]/r["prev"]*100,2) if r["prev"]>0 else 0, axis=1)
            raw["name2"]  = raw["code"].astype(str).map(name_map).fillna(raw["name"])
            df = raw[raw["code"].str.match(r"^\d{4}$")&(raw["tv_yi"]>0)].copy()
            df = df.sort_values("tv_yi",ascending=False).head(30).reset_index(drop=True)
            df["rank"]=range(1,len(df)+1)
            df=df.rename(columns={"tv_yi":"trade_value","cpct":"change_pct","name2":"name"})
            if len(df)>=10:
                return df[["rank","code","name","trade_value","change_pct"]], "TWSE STOCK_DAY_ALL"
        else:
            errors.append(f"TWSE STOCK_DAY_ALL: stat={d.get('stat')} rows={len(d.get('data',[]))}")
    except Exception as e:
        errors.append(f"TWSE STOCK_DAY_ALL exception: {e}")

    # ══ 3. TWSE MI_INDEX20 ══
    try:
        ymd = trade_date.replace("-","")
        r   = requests.get(
            f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX20?date={ymd}&selectType=ALL",
            headers=TWSE_H, timeout=15)
        d   = r.json()
        if d.get("stat")=="OK" and d.get("data"):
            rows  = d["data"]; ncols=len(rows[0])
            base  = ["code","name","vol","tv_raw","open","high","low","close","chg","cpct_raw"]
            cols  = base[:ncols] if ncols<=len(base) else base+[f"x{i}" for i in range(ncols-len(base))]
            raw   = pd.DataFrame(rows,columns=cols)
            raw["tv_yi"] = raw["tv_raw"].apply(_n)/1e8
            raw["cpct"]  = raw["cpct_raw"].apply(lambda v: _n(str(v).replace("%","").replace("+","")))
            raw["name2"] = raw["code"].astype(str).map(name_map).fillna(raw["name"])
            df = raw[raw["code"].str.match(r"^\d{4}$")&(raw["tv_yi"]>0)].copy()
            df = df.sort_values("tv_yi",ascending=False).head(30).reset_index(drop=True)
            df["rank"]=range(1,len(df)+1)
            df=df.rename(columns={"tv_yi":"trade_value","cpct":"change_pct","name2":"name"})
            if len(df)>=10:
                return df[["rank","code","name","trade_value","change_pct"]], "TWSE MI_INDEX20"
        else:
            errors.append(f"TWSE MI_INDEX20: stat={d.get('stat')}")
    except Exception as e:
        errors.append(f"TWSE MI_INDEX20: {e}")

    # ══ 4. Yahoo Screener ══
    try:
        url = "https://query1.finance.yahoo.com/v1/finance/screener?formatted=false&lang=zh-TW&region=TW"
        payload = {"size":30,"offset":0,"sortField":"intradaytradingvalue","sortType":"DESC",
                   "quoteType":"EQUITY","topOperator":"AND",
                   "query":{"operator":"AND","operands":[{"operator":"eq","operands":["region","tw"]}]},
                   "userId":"","userIdType":"guid"}
        r     = requests.post(url, json=payload, headers=YAHOO_H, timeout=15)
        qs    = r.json()["finance"]["result"][0]["quotes"]
        rows  = []
        for q in qs:
            sym  = str(q.get("symbol","")).replace(".TW","").replace(".TWO","")
            px   = _n(q.get("regularMarketPrice",0))
            vol  = _n(q.get("regularMarketVolume",0))
            tv   = round(px*vol/1e8,1)
            cpct = round(_n(q.get("regularMarketChangePercent",0)),2)
            nm   = name_map.get(sym, q.get("shortName",sym))
            if re.match(r"^\d{4}$",sym) and tv>0:
                rows.append({"code":sym,"name":nm,"trade_value":tv,"change_pct":cpct})
        if rows:
            df=pd.DataFrame(rows).sort_values("trade_value",ascending=False).head(30).reset_index(drop=True)
            df["rank"]=range(1,len(df)+1)
            return df, "Yahoo Finance Screener"
        errors.append("Yahoo Screener: 0 筆符合條件")
    except Exception as e:
        errors.append(f"Yahoo Screener: {e}")

    # ══ 5. Yahoo Quote 大型股清單 ══
    try:
        codes = list(name_map.keys())[:60]
        syms  = ",".join(f"{c}.TW" for c in codes)
        r     = requests.get(f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={syms}",
                             headers=YAHOO_H, timeout=15)
        rows  = []
        for q in r.json().get("quoteResponse",{}).get("result",[]):
            sym  = str(q.get("symbol","")).replace(".TW","")
            px   = _n(q.get("regularMarketPrice",0))
            vol  = _n(q.get("regularMarketVolume",0))
            tv   = round(px*vol/1e8,1)
            cpct = round(_n(q.get("regularMarketChangePercent",0)),2)
            nm   = name_map.get(sym, sym)
            if re.match(r"^\d{4}$",sym) and tv>0:
                rows.append({"code":sym,"name":nm,"trade_value":tv,"change_pct":cpct})
        if rows:
            df=pd.DataFrame(rows).sort_values("trade_value",ascending=False).head(30).reset_index(drop=True)
            df["rank"]=range(1,len(df)+1)
            return df, "Yahoo Finance Quote"
        errors.append("Yahoo Quote: 0 筆")
    except Exception as e:
        errors.append(f"Yahoo Quote: {e}")

    # 所有來源失敗，把 error 記到 session
    for e in errors:
        _err_log(e)
    return pd.DataFrame(), "all_failed"


# ─────────────────────────────────────────────────────────────────────────────
# REALTIME CHANGE PCT
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=28, show_spinner=False)
def fetch_realtime_pct(codes: list) -> dict:
    result = {}
    today  = tw_now().strftime("%Y-%m-%d")

    # FinMind TaiwanStockPriceMinute
    if FINMIND_TOKEN:
        for code in codes:
            data = _fm_get("TaiwanStockPriceMinute", {"data_id":code,"date":today}, log_errors=False)
            if data and len(data) >= 2:
                c = _n(data[-1].get("close",0))
                o = _n(data[0].get("open",0))
                if o>0:
                    result[code] = round((c-o)/o*100,2)
            time.sleep(0.02)
        if result:
            return result

    # TWSE mis
    try:
        sess = requests.Session()
        sess.get("https://mis.twse.com.tw/stock/index.jsp",headers=MIS_H,timeout=6)
        for i in range(0,len(codes),30):
            q = "|".join(f"tse_{c}.tw" for c in codes[i:i+30])
            r = sess.get(f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={q}&json=1&delay=0",
                         headers=MIS_H, timeout=8)
            for item in r.json().get("msgArray",[]):
                c=item.get("c",""); z=_n(item.get("z") or item.get("o") or 0); y=_n(item.get("y") or 0)
                if y>0 and z>0: result[c]=round((z-y)/y*100,2)
            time.sleep(0.05)
        if result: return result
    except: pass

    # Yahoo
    try:
        for i in range(0,len(codes),20):
            syms=",".join(f"{c}.TW" for c in codes[i:i+20])
            r=requests.get(f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={syms}&fields=regularMarketChangePercent",
                           headers=YAHOO_H,timeout=10)
            for q in r.json().get("quoteResponse",{}).get("result",[]):
                c=str(q.get("symbol","")).replace(".TW","")
                result[c]=round(_n(q.get("regularMarketChangePercent",0)),2)
            time.sleep(0.05)
    except: pass

    return result


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────────────────
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
    except Exception as e:
        _err_log(f"Google Sheets 連線失敗: {e}")
    return None


def save_today(client, df, date_key):
    if not client or len(df)==0: return
    try:
        import gspread
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        try:    ws=sh.worksheet(date_key); ws.clear()
        except gspread.WorksheetNotFound: ws=sh.add_worksheet(date_key,35,5)
        out=df[["code","name","trade_value","change_pct"]].copy()
        out.columns=["代號","名稱","成交金額(億)","漲跌幅(%)"]
        out["成交金額(億)"]=out["成交金額(億)"].round(2)
        out["漲跌幅(%)"]=out["漲跌幅(%)"].round(2)
        ws.update([out.columns.tolist()]+out.values.tolist())
    except Exception as e:
        _err_log(f"Sheets 寫入失敗: {e}")


def load_prev_codes(client, today_key):
    if not client: return set()
    try:
        sh    = client.open_by_key(GOOGLE_SHEETS_ID)
        valid = sorted(ws.title for ws in sh.worksheets()
                       if re.match(r"^\d{4}-\d{2}-\d{2}$",ws.title) and ws.title<today_key)
        if not valid: return set()
        recs=sh.worksheet(valid[-1]).get_all_records()
        return {str(r.get("代號","")).strip() for r in recs if r.get("代號")}
    except: return set()


def load_history(client):
    if not client: return {}
    out={}
    try:
        sh=client.open_by_key(GOOGLE_SHEETS_ID)
        for ws in sorted([w for w in sh.worksheets() if re.match(r"^\d{4}-\d{2}-\d{2}$",w.title)],
                         key=lambda x:x.title,reverse=True)[:60]:
            try:
                recs=ws.get_all_records()
                if recs: out[ws.title]=pd.DataFrame(recs)
            except: pass
    except: pass
    return out


# ─────────────────────────────────────────────────────────────────────────────
# TABLE
# ─────────────────────────────────────────────────────────────────────────────
def build_table(df, prev_codes, cb_codes, extra=None):
    rows, pcts, news = [], [], []
    for _, r in df.iterrows():
        code   = str(r["code"]).strip()
        pct    = float(r.get("change_pct") or 0)
        is_new = bool(prev_codes) and (code not in prev_codes)
        has_cb = code in cb_codes
        tags   = (["★新"] if is_new else [])+(["CB"] if has_cb else [])
        name   = str(r["name"])+("  "+"  ".join(tags) if tags else "")
        pstr   = (f"▲ {pct:.2f}%" if pct>0 else f"▼ {abs(pct):.2f}%" if pct<0 else "─")
        row_d  = {"排行":int(r.get("rank",_+1)),"股票名稱":name,"漲跌幅":pstr,
                  "成交金額(億)":float(r.get("trade_value",0))}
        if extra:
            for s,d2 in extra: row_d[d2]=r.get(s,"")
        rows.append(row_d); pcts.append(pct); news.append(is_new)

    disp=pd.DataFrame(rows)
    disp["__p"]=pcts; disp["__n"]=news

    def rbg(row):
        bg=("#191000" if row["__n"] else "#1a0808" if row["__p"]>0 else "#041008" if row["__p"]<0 else "#0a1520")
        return [bg]*len(row)
    def cpct(col):
        return ["color:#e74c3c;font-weight:600" if v>0
                else ("color:#2ecc71;font-weight:600" if v<0 else "color:#5a6a80") for v in disp["__p"]]
    def cname(col):
        s=[]
        for i,v in enumerate(disp["股票名稱"]):
            n,cb=disp["__n"].iloc[i],"CB" in str(v)
            s.append("color:#f39c12;font-weight:700" if n else "color:#a78bfa" if cb else "color:#c8d6e5")
        return s
    def crank(col):
        return ["color:#4fc3f7;font-weight:700" if v<=3 else "color:#4a6080" for v in disp["排行"]]

    fmt={"成交金額(億)":"{:,.1f}"}
    if extra:
        for _,d2 in extra:
            if "億" in d2: fmt[d2]="{:,.1f}"

    return (disp.style
        .apply(rbg,axis=1).apply(cpct,subset=["漲跌幅"])
        .apply(cname,subset=["股票名稱"]).apply(crank,subset=["排行"])
        .format(fmt)
        .set_properties(**{"font-family":"'IBM Plex Mono',monospace","font-size":"13px","border":"none"})
        .set_properties(subset=["排行"],**{"text-align":"center"})
        .set_properties(subset=["股票名稱"],**{"text-align":"left"})
        .set_properties(subset=["漲跌幅","成交金額(億)"],**{"text-align":"right"})
        .set_table_styles([
            {"selector":"thead th","props":[("background-color","#0a1520"),("color","#4a6080"),
                ("font-family","'IBM Plex Mono',monospace"),("font-size","11px"),
                ("letter-spacing","1.5px"),("text-transform","uppercase"),
                ("border-bottom","1px solid #1a2940"),("padding","10px 14px")]},
            {"selector":"tbody td","props":[("padding","10px 14px"),("border-bottom","1px solid #0d1a28")]},
            {"selector":"tbody tr:hover td","props":[("filter","brightness(1.3)")]},
            {"selector":"table","props":[("width","100%"),("border-collapse","collapse")]},
        ])
        .hide(axis="index")
        .hide(subset=["__p","__n"],axis="columns"))


# ─────────────────────────────────────────────────────────────────────────────
# SHARED UI
# ─────────────────────────────────────────────────────────────────────────────
def render_kpi(df, prev_codes, cb_codes):
    up=int((df["change_pct"]>0).sum()); dn=int((df["change_pct"]<0).sum()); nc=len(df)-up-dn
    new_c=sum(1 for c in df["code"].astype(str) if prev_codes and c not in prev_codes)
    cb_c =sum(1 for c in df["code"].astype(str) if c in cb_codes)
    tot=len(df) or 1; up_p=round(up/tot*100); dn_p=round(dn/tot*100); nc_p=100-up_p-dn_p
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


def _prep(raw):
    df=raw.copy(); df.columns=[str(c).strip() for c in df.columns]
    rm={}
    for c in df.columns:
        lc=c.lower()
        if c in ("代號","code") or lc=="stock_id":          rm[c]="code"
        elif c in ("名稱","name") or lc=="stock_name":       rm[c]="name"
        elif "成交金額" in c or c=="trade_value":             rm[c]="trade_value"
        elif "漲跌幅" in c or c in ("change_pct","漲跌%"):   rm[c]="change_pct"
    df=df.rename(columns=rm)
    for c in ["code","name","trade_value","change_pct"]:
        if c not in df.columns: df[c]="" if c in ["code","name"] else 0.0
    df["trade_value"]=pd.to_numeric(df["trade_value"],errors="coerce").fillna(0)
    df["change_pct"] =pd.to_numeric(df["change_pct"], errors="coerce").fillna(0)
    df["rank"]=range(1,len(df)+1)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 1 – REALTIME
# ─────────────────────────────────────────────────────────────────────────────
def page_realtime():
    tw      = tw_now()
    open_   = is_market_open()
    trade_d = last_trade_date()
    today_k = tw.strftime("%Y-%m-%d")

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
        auto = st.toggle("自動刷新", value=open_)
        ivl  = st.select_slider("刷新間隔(秒)",[15,30,60,120],value=60)
        if st.button("⟳ 立即刷新", use_container_width=True):
            st.cache_data.clear(); st.rerun()
        st.markdown("---")
        st.metric("台灣時間", tw.strftime("%H:%M:%S"))
        st.metric("查詢交易日", trade_d)
        if FINMIND_TOKEN:
            st.success("✅ FinMind Token 已設定")
        else:
            st.error("❌ 未設定 finmind_token")
        st.caption("資料來源順序:\n① FinMind\n② TWSE ALL\n③ TWSE TOP20\n④ Yahoo Screener\n⑤ Yahoo Quote")

    with st.spinner("載入成交資料…"):
        df, src = fetch_top30(trade_d)

    # 若所有來源失敗，顯示詳細錯誤
    if len(df) == 0:
        st.error("❌ 所有資料來源均失敗")
        if st.session_state.api_errors:
            st.markdown("**詳細錯誤訊息：**")
            for e in st.session_state.api_errors[-10:]:
                st.code(e, language=None)
        st.info(
            "**排查步驟：**\n"
            "1. 確認 Streamlit Cloud secrets 有設定 `finmind_token`\n"
            "2. 切換到 🔧 診斷頁面執行測試\n"
            "3. 確認 FinMind token 是否有效（到 finmindtrade.com 檢查）"
        )
        return

    if open_:
        with st.spinner("更新即時漲跌幅…"):
            rt = fetch_realtime_pct(df["code"].tolist())
        for idx, row in df.iterrows():
            p = rt.get(str(row["code"]))
            if p is not None:
                df.at[idx,"change_pct"] = p

    cb_codes   = fetch_cb_stocks()
    client     = gs_client()
    prev_codes = load_prev_codes(client, today_k)
    if open_ or tw.hour >= 14:
        save_today(client, df, today_k)

    render_kpi(df, prev_codes, cb_codes)
    render_legend()
    st.markdown(
        f'<div class="refresh-info">更新 {tw.strftime("%H:%M:%S")} '
        f'<span class="src-tag">{src}</span>&nbsp;查詢日期: {trade_d}</div>',
        unsafe_allow_html=True)
    st.dataframe(build_table(df,prev_codes,cb_codes), use_container_width=True, height=980, hide_index=True)

    if not open_:
        st.info(f"📌 非交易時段，顯示 {trade_d} 收盤資料。")
    if auto and open_:
        time.sleep(ivl); st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 2 – HISTORY
# ─────────────────────────────────────────────────────────────────────────────
def page_history():
    st.markdown("""<div class="topbar"><div>
        <div class="topbar-logo">歷史成交金額排行</div>
        <div class="topbar-sub">HISTORICAL VOLUME LEADERS</div>
    </div></div>""", unsafe_allow_html=True)

    client=gs_client(); history=load_history(client); cb_codes=fetch_cb_stocks()
    if not history:
        st.warning("尚無歷史資料。請確認 Google Sheets API 已設定並已完成至少一日儲存。")
        return

    dates=sorted(history.keys(),reverse=True); all_sorted=sorted(history.keys())
    c1,c2=st.columns([3,1])
    with c1: selected=st.multiselect("選擇日期",dates,default=dates[:7] if len(dates)>=7 else dates)
    with c2: mode=st.radio("模式",["每日明細","彙總排行"],horizontal=True)
    if not selected: st.info("請選擇日期"); return

    if mode=="每日明細":
        for date in sorted(selected,reverse=True):
            raw=history.get(date)
            if raw is None or len(raw)==0: continue
            df=_prep(raw); prev_c=set()
            idx=all_sorted.index(date) if date in all_sorted else -1
            if idx>0:
                try: prev_c=set(_prep(history[all_sorted[idx-1]])["code"].astype(str))
                except: pass
            with st.expander(f"📅 {date}",expanded=(date==sorted(selected,reverse=True)[0])):
                render_kpi(df,prev_c,cb_codes); render_legend()
                st.dataframe(build_table(df,prev_c,cb_codes),use_container_width=True,height=600,hide_index=True)
    else:
        all_rows=[]
        for d in selected:
            raw=history.get(d)
            if raw is None: continue
            tmp=_prep(raw); tmp["_d"]=d; all_rows.append(tmp)
        if not all_rows: st.warning("無有效資料"); return
        combined=pd.concat(all_rows,ignore_index=True)
        agg=(combined.groupby(["code","name"])
             .agg(avg_val=("trade_value","mean"),total_val=("trade_value","sum"),
                  days=("trade_value","count"),avg_pct=("change_pct","mean"))
             .reset_index().sort_values("total_val",ascending=False).head(30).reset_index(drop=True))
        agg["rank"]=range(1,len(agg)+1); agg["trade_value"]=agg["avg_val"]; agg["change_pct"]=agg["avg_pct"].round(2)
        sel_s=sorted(selected); prev_c=set()
        if sel_s[0] in all_sorted:
            i=all_sorted.index(sel_s[0])
            if i>0:
                try: prev_c=set(_prep(history[all_sorted[i-1]])["code"].astype(str))
                except: pass
        period=f"{sel_s[0]} ~ {sel_s[-1]}" if len(sel_s)>1 else sel_s[0]
        st.markdown(f'<div class="section-title">彙總 · {period} · {len(sel_s)} 交易日</div>',unsafe_allow_html=True)
        render_legend()
        extra=[("avg_val","平均成交(億)"),("total_val","累積成交(億)"),("days","上榜天數")]
        st.dataframe(build_table(agg,prev_c,cb_codes,extra=extra),use_container_width=True,height=700,hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 3 – DIAGNOSE
# ─────────────────────────────────────────────────────────────────────────────
def page_debug():
    tw = tw_now()
    st.markdown("## 🔧 API 診斷")
    col1,col2,col3 = st.columns(3)
    col1.metric("台灣時間 (UTC+8)", tw.strftime("%Y-%m-%d %H:%M:%S"))
    col2.metric("查詢交易日", last_trade_date())
    col3.metric("FinMind Token", "✅ 已設定" if FINMIND_TOKEN else "❌ 未設定")

    if st.session_state.api_errors:
        st.error("**最近錯誤記錄：**\n\n" + "\n\n".join(st.session_state.api_errors[-10:]))

    if not st.button("🧪 執行診斷測試", type="primary"):
        st.info("點擊上方按鈕開始測試所有 API 來源")
        return

    trade_d = last_trade_date()
    st.markdown("---")

    # ── Test 1: FinMind TaiwanStockPrice ──
    with st.expander("① FinMind TaiwanStockPrice", expanded=True):
        if not FINMIND_TOKEN:
            st.error("Token 未設定！請在 Streamlit secrets 加入 finmind_token")
        else:
            try:
                params = {"dataset":"TaiwanStockPrice","date":trade_d,"token":FINMIND_TOKEN}
                r = requests.get(FINMIND_BASE, params=params, timeout=25)
                d = r.json()
                st.write(f"**HTTP狀態:** {r.status_code}  |  **status:** {d.get('status')} (型別:{type(d.get('status')).__name__})  |  **msg:** {d.get('msg','')}")
                if d.get("data"):
                    df = pd.DataFrame(d["data"])
                    st.success(f"✅ 取得 {len(df)} 筆資料")
                    st.write(f"**欄位:** {list(df.columns)}")
                    st.dataframe(df.head(5))
                else:
                    st.error(f"❌ 無資料。完整回傳: {str(d)[:500]}")
            except Exception as e:
                st.error(f"❌ Exception: {e}")

    # ── Test 2: FinMind TaiwanStockInfo ──
    with st.expander("② FinMind TaiwanStockInfo"):
        if not FINMIND_TOKEN:
            st.warning("Token 未設定")
        else:
            try:
                params = {"dataset":"TaiwanStockInfo","token":FINMIND_TOKEN}
                r = requests.get(FINMIND_BASE, params=params, timeout=15)
                d = r.json()
                st.write(f"HTTP:{r.status_code} status:{d.get('status')} msg:{d.get('msg','')}")
                if d.get("data"):
                    df = pd.DataFrame(d["data"])
                    st.success(f"✅ {len(df)} 筆  欄位: {list(df.columns)}")
                    st.dataframe(df.head(3))
                else:
                    st.error(str(d)[:300])
            except Exception as e:
                st.error(f"❌ {e}")

    # ── Test 3: FinMind CB ──
    with st.expander("③ FinMind TaiwanStockConvertibleBond"):
        if not FINMIND_TOKEN:
            st.warning("Token 未設定")
        else:
            try:
                params = {"dataset":"TaiwanStockConvertibleBond","token":FINMIND_TOKEN}
                r = requests.get(FINMIND_BASE, params=params, timeout=15)
                d = r.json()
                st.write(f"HTTP:{r.status_code} status:{d.get('status')} msg:{d.get('msg','')}")
                if d.get("data"):
                    df = pd.DataFrame(d["data"])
                    st.success(f"✅ {len(df)} 筆  欄位: {list(df.columns)}")
                    st.dataframe(df.head(3))
                else:
                    st.error(str(d)[:500])
            except Exception as e:
                st.error(f"❌ {e}")

    # ── Test 4: TWSE ──
    with st.expander("④ TWSE STOCK_DAY_ALL"):
        try:
            r = requests.get("https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL",
                             headers=TWSE_H, timeout=15)
            d = r.json()
            st.write(f"HTTP:{r.status_code} stat:{d.get('stat')} 筆數:{len(d.get('data',[]))}")
            if d.get("data"):
                st.success(f"✅ {len(d['data'])} 筆")
                st.write(f"第一筆: {d['data'][0]}")
            else:
                st.error(str(d)[:300])
        except Exception as e:
            st.error(f"❌ {e}")

    # ── Test 5: Yahoo ──
    with st.expander("⑤ Yahoo Finance (台積電)"):
        try:
            r = requests.get("https://query1.finance.yahoo.com/v7/finance/quote?symbols=2330.TW",
                             headers=YAHOO_H, timeout=10)
            qs = r.json().get("quoteResponse",{}).get("result",[])
            if qs:
                q = qs[0]
                st.success(f"✅ HTTP:{r.status_code}")
                st.json({k:v for k,v in q.items() if k in
                         ["symbol","shortName","regularMarketPrice","regularMarketVolume","regularMarketChangePercent"]})
            else:
                st.error(f"無結果: {str(r.json())[:300]}")
        except Exception as e:
            st.error(f"❌ {e}")

    st.success("診斷完成！請查看各來源結果。")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    page = st.radio("nav", ["📈  即時排行","📊  歷史排行","🔧  診斷"],
                    horizontal=True, label_visibility="collapsed")
    if "即時" in page:   page_realtime()
    elif "歷史" in page: page_history()
    else:                page_debug()

if __name__ == "__main__":
    main()
