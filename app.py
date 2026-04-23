"""
台股成交金額 TOP 30 追蹤系統

資料來源策略：
  主要（盤後/每日）: FinMind API - TaiwanStockPrice
    https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockPrice&date=YYYY-MM-DD&token=

  即時（盤中）: FinMind - TaiwanStockPriceMinute (需 token)
               或 TWSE mis API (免費)

  CB 可轉債: FinMind - TaiwanStockConvertibleBond
             或 TWSE CB_OVERVIEW

  備援: TWSE STOCK_DAY_ALL / Yahoo Finance quote
"""

import streamlit as st
import pandas as pd
import requests
import time
import re
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="台股成交金額 TOP 30",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────
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

.topbar {
    display:flex; align-items:center; justify-content:space-between;
    padding:10px 0 18px; border-bottom:1px solid #1a2940; margin-bottom:20px;
}
.topbar-logo { font-family:'IBM Plex Mono',monospace; font-size:20px; font-weight:600;
    color:#4fc3f7; letter-spacing:2px; }
.topbar-sub  { font-size:11px; color:#4a6080; margin-top:2px; letter-spacing:1px; }

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
.section-title { font-family:'IBM Plex Mono',monospace; font-size:12px; color:#4a6080;
    letter-spacing:2px; text-transform:uppercase; margin:24px 0 12px;
    display:flex; align-items:center; gap:10px; }
.section-title::after { content:''; flex:1; height:1px; background:#1a2940; }
.src-tag { display:inline-block; background:#0d1e30; border:1px solid #1a4060;
    border-radius:3px; padding:2px 8px; font-size:10px; color:#4a8ab8;
    font-family:'IBM Plex Mono',monospace; letter-spacing:1px; margin-left:8px; }

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
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
GOOGLE_SHEETS_ID = "1lTRxbT9iv3uRIrQ1wUFPV0LZbo8p5Qoh4a1FMGa-iU4"

# FinMind token（可選，有 token 才能取即時；無 token 也能取盤後）
# 設定方式：在 .streamlit/secrets.toml 加 finmind_token = "xxx"
FINMIND_TOKEN = st.secrets.get("finmind_token", "")

FINMIND_BASE  = "https://api.finmindtrade.com/api/v4/data"
TWSE_HEADERS  = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer":    "https://www.twse.com.tw/",
    "Accept":     "application/json, text/plain, */*",
}
MIS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer":    "https://mis.twse.com.tw/",
    "Accept":     "application/json",
}
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json, text/plain, */*",
}


def _clean(val, default=0.0):
    try:
        s = str(val).replace(",","").replace("+","").strip()
        return float(s) if s not in ("","--","-","N/A","nan","None") else default
    except Exception:
        return default


def _last_trading_date() -> str:
    """回傳最近的交易日（跳過週末）"""
    d = datetime.now()
    # 若尚未 14:30，用前一日
    if d.hour < 14 and d.minute < 30:
        d -= timedelta(days=1)
    # 跳週末
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")


# ─────────────────────────────────────────────────────────────────────────────
# CB 可轉債  — FinMind TaiwanStockConvertibleBond + TWSE 備援
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_cb_stocks() -> set:
    codes = set()

    # ── 來源 1: FinMind TaiwanStockConvertibleBond ──
    try:
        params = {"dataset": "TaiwanStockConvertibleBond"}
        if FINMIND_TOKEN:
            params["token"] = FINMIND_TOKEN
        r = requests.get(FINMIND_BASE, params=params, timeout=15)
        data = r.json()
        if data.get("status") == 200 and data.get("data"):
            df = pd.DataFrame(data["data"])
            # 欄位可能是 stock_id 或 StockID
            col = next((c for c in df.columns if "stock_id" in c.lower() or "stockid" in c.lower()), None)
            if col:
                for v in df[col].astype(str):
                    if re.match(r"^\d{4}$", v.strip()):
                        codes.add(v.strip())
        if codes:
            return codes
    except Exception:
        pass

    # ── 來源 2: TWSE CB_OVERVIEW ──
    try:
        r = requests.get("https://www.twse.com.tw/rwd/zh/cbInfo/CB_OVERVIEW",
                         headers=TWSE_HEADERS, timeout=12)
        data = r.json()
        if data.get("stat") == "OK":
            for row in data.get("data", []):
                for cell in row:
                    s = str(cell).strip()
                    if re.match(r"^\d{4}$", s):
                        codes.add(s)
        if codes:
            return codes
    except Exception:
        pass

    # ── 來源 3: TWSE CB_BOND_INFO ──
    try:
        r = requests.get("https://www.twse.com.tw/rwd/zh/cbInfo/CB_BOND_INFO",
                         headers=TWSE_HEADERS, timeout=12)
        data = r.json()
        if data.get("stat") == "OK":
            for row in data.get("data", []):
                for cell in row:
                    s = str(cell).strip()
                    if re.match(r"^\d{4}$", s):
                        codes.add(s)
    except Exception:
        pass

    return codes


# ─────────────────────────────────────────────────────────────────────────────
# TOP 30 成交金額  — 四層 fallback
# ─────────────────────────────────────────────────────────────────────────────
def _make_top30(df: pd.DataFrame, code_col: str, name_col: str,
                tv_col: str, pct_col: str, tv_unit: float = 1.0) -> pd.DataFrame:
    """統一格式化為 TOP30 DataFrame"""
    df = df.copy()
    df["code"]        = df[code_col].astype(str).str.strip()
    df["name"]        = df[name_col].astype(str).str.strip() if name_col else df[code_col].astype(str)
    df["trade_value"] = df[tv_col].apply(_clean) * tv_unit
    df["change_pct"]  = df[pct_col].apply(_clean) if pct_col else 0.0

    df = df[df["code"].str.match(r"^\d{4}$") & (df["trade_value"] > 0)]
    df = df.sort_values("trade_value", ascending=False).head(30).reset_index(drop=True)
    df["rank"] = range(1, len(df)+1)
    return df[["rank","code","name","trade_value","change_pct"]]


@st.cache_data(ttl=55, show_spinner=False)
def fetch_top30(date_str: str) -> tuple:
    """
    回傳 (df, source_label)
    date_str: YYYY-MM-DD
    """

    # ══════════════════════════════════════════════════════
    # 來源 1: FinMind TaiwanStockPrice（每日盤後成交）
    # ══════════════════════════════════════════════════════
    try:
        params = {
            "dataset":   "TaiwanStockPrice",
            "date":      date_str,
            "data_id":   "",       # 空 = 全市場
        }
        if FINMIND_TOKEN:
            params["token"] = FINMIND_TOKEN
        r = requests.get(FINMIND_BASE, params=params, timeout=20)
        data = r.json()
        if data.get("status") == 200 and data.get("data"):
            raw = pd.DataFrame(data["data"])
            # FinMind 欄位: date, stock_id, Trading_Volume, Trading_money,
            #               open, max, min, close, spread, Trading_turnover
            # Trading_money = 成交金額(元)
            if "Trading_money" in raw.columns and "stock_id" in raw.columns:
                raw["tv_yi"] = raw["Trading_money"].apply(_clean) / 1e8
                # 計算漲跌幅: spread / (close - spread)
                raw["close_f"]  = raw["close"].apply(_clean)
                raw["spread_f"] = raw["spread"].apply(_clean)
                raw["prev_c"]   = raw["close_f"] - raw["spread_f"]
                raw["chg_pct"]  = raw.apply(
                    lambda r: round(r["spread_f"]/r["prev_c"]*100, 2) if r["prev_c"]>0 else 0, axis=1)
                df = _make_top30(raw, "stock_id", "stock_id", "tv_yi", "chg_pct")
                # 取股票名稱（FinMind 這個 dataset 沒有名稱，用另一個 API 補）
                df = _enrich_names_finmind(df)
                if len(df) >= 10:
                    return df, "FinMind TaiwanStockPrice"
    except Exception:
        pass

    # ══════════════════════════════════════════════════════
    # 來源 2: TWSE STOCK_DAY_ALL（當日全部股票）
    # ══════════════════════════════════════════════════════
    try:
        r = requests.get(
            "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL",
            headers=TWSE_HEADERS, timeout=15)
        data = r.json()
        if data.get("stat") == "OK" and data.get("data"):
            rows = data["data"]
            ncols = len(rows[0])
            base = ["code","name","vol","txn","trade_value_raw",
                    "open","high","low","close","sign","change_raw","pe"]
            cols = base[:ncols] if ncols<=len(base) else base+[f"x{i}" for i in range(ncols-len(base))]
            raw = pd.DataFrame(rows, columns=cols)
            raw["tv_yi"] = raw["trade_value_raw"].apply(_clean) / 1e8

            # 漲跌幅
            raw["close_f"]  = raw["close"].apply(_clean)
            raw["sign_n"]   = raw["sign"].astype(str).str.strip().map({"+":1,"-":-1}).fillna(0)
            raw["chg_abs"]  = raw["change_raw"].apply(_clean) * raw["sign_n"]
            raw["prev_c"]   = raw["close_f"] - raw["chg_abs"]
            raw["chg_pct"]  = raw.apply(
                lambda r: round(r["chg_abs"]/r["prev_c"]*100, 2) if r["prev_c"]>0 else 0, axis=1)

            df = _make_top30(raw, "code", "name", "tv_yi", "chg_pct")
            if len(df) >= 10:
                return df, "TWSE STOCK_DAY_ALL"
    except Exception:
        pass

    # ══════════════════════════════════════════════════════
    # 來源 3: TWSE MI_INDEX20（大成交量個股）
    # ══════════════════════════════════════════════════════
    try:
        ymd = date_str.replace("-","")
        r = requests.get(
            f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX20?date={ymd}&selectType=ALL",
            headers=TWSE_HEADERS, timeout=15)
        data = r.json()
        if data.get("stat") == "OK" and data.get("data"):
            rows  = data["data"]
            ncols = len(rows[0])
            base  = ["code","name","vol","trade_value_raw",
                     "open","high","low","close","change","chg_pct_raw"]
            cols  = base[:ncols] if ncols<=len(base) else base+[f"x{i}" for i in range(ncols-len(base))]
            raw   = pd.DataFrame(rows, columns=cols)
            raw["tv_yi"]    = raw["trade_value_raw"].apply(_clean) / 1e8
            raw["chg_pct2"] = raw["chg_pct_raw"].apply(
                lambda v: _clean(str(v).replace("%","").replace("+","")))
            df = _make_top30(raw, "code", "name", "tv_yi", "chg_pct2")
            if len(df) >= 10:
                return df, "TWSE MI_INDEX20"
    except Exception:
        pass

    # ══════════════════════════════════════════════════════
    # 來源 4: Yahoo Finance Screener（台股依成交量排序）
    # ══════════════════════════════════════════════════════
    try:
        url = ("https://query1.finance.yahoo.com/v1/finance/screener"
               "?formatted=false&lang=zh-TW&region=TW&corsDomain=finance.yahoo.com")
        payload = {
            "size": 30, "offset": 0,
            "sortField": "intradaytradingvalue", "sortType": "DESC",
            "quoteType": "EQUITY", "topOperator": "AND",
            "query": {"operator":"AND","operands":[
                {"operator":"eq","operands":["region","tw"]},
            ]},
            "userId":"","userIdType":"guid",
        }
        r = requests.post(url, json=payload, headers=YAHOO_HEADERS, timeout=15)
        quotes = r.json()["finance"]["result"][0]["quotes"]
        rows = []
        for i, q in enumerate(quotes):
            sym   = str(q.get("symbol","")).replace(".TW","").replace(".TWO","")
            name  = q.get("shortName") or q.get("longName") or sym
            price = _clean(q.get("regularMarketPrice", 0))
            vol   = _clean(q.get("regularMarketVolume", 0))
            tv    = round(price * vol / 1e8, 1)
            cpct  = round(_clean(q.get("regularMarketChangePercent", 0)), 2)
            if re.match(r"^\d{4}$", sym) and tv > 0:
                rows.append({"code":sym,"name":name,"trade_value":tv,"change_pct":cpct})
        if rows:
            df = pd.DataFrame(rows).sort_values("trade_value",ascending=False).head(30).reset_index(drop=True)
            df["rank"] = range(1,len(df)+1)
            return df, "Yahoo Finance Screener"
    except Exception:
        pass

    # ══════════════════════════════════════════════════════
    # 來源 5: Yahoo Finance quote（固定台股大型股清單）
    # ══════════════════════════════════════════════════════
    try:
        # 取台灣50成分股 + 其他大型股
        base_codes = [
            "2330","2317","2454","2382","2308","2303","6505","1301","2002","2412",
            "2881","2882","2886","2891","2884","2885","2890","2880","2892","6669",
            "3231","2379","2395","2408","3034","2344","2357","4904","3045","2886",
            "2301","2303","2308","2317","2325","2330","2344","2345","2353","2356",
            "2357","2376","2379","2382","2385","2392","2395","2408","2412","2441",
            "3008","3034","3481","3673","3702","6176","6269","6278","6285","6443",
        ]
        syms = ",".join(f"{c}.TW" for c in base_codes[:50])
        r = requests.get(
            f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={syms}&fields="
            f"shortName,regularMarketPrice,regularMarketVolume,regularMarketChangePercent",
            headers=YAHOO_HEADERS, timeout=15)
        quotes = r.json().get("quoteResponse",{}).get("result",[])
        rows = []
        for q in quotes:
            sym   = str(q.get("symbol","")).replace(".TW","")
            name  = q.get("shortName") or sym
            price = _clean(q.get("regularMarketPrice",0))
            vol   = _clean(q.get("regularMarketVolume",0))
            tv    = round(price * vol / 1e8, 1)
            cpct  = round(_clean(q.get("regularMarketChangePercent",0)), 2)
            if re.match(r"^\d{4}$", sym) and tv > 0:
                rows.append({"code":sym,"name":name,"trade_value":tv,"change_pct":cpct})
        if rows:
            df = pd.DataFrame(rows).sort_values("trade_value",ascending=False).head(30).reset_index(drop=True)
            df["rank"] = range(1,len(df)+1)
            return df, "Yahoo Finance Quote"
    except Exception:
        pass

    return pd.DataFrame(), "all_failed"


@st.cache_data(ttl=28, show_spinner=False)
def fetch_realtime_changes(codes: list) -> dict:
    """
    即時漲跌幅更新。回傳 {code: change_pct}
    來源 1: FinMind TaiwanStockPriceMinute
    來源 2: TWSE mis
    來源 3: Yahoo quote
    """
    result = {}

    # ── FinMind 即時（需 token）──
    if FINMIND_TOKEN:
        try:
            for code in codes[:30]:
                params = {
                    "dataset":  "TaiwanStockPriceMinute",
                    "data_id":  code,
                    "date":     datetime.now().strftime("%Y-%m-%d"),
                    "token":    FINMIND_TOKEN,
                }
                r = requests.get(FINMIND_BASE, params=params, timeout=8)
                data = r.json()
                if data.get("status")==200 and data.get("data"):
                    last = data["data"][-1]
                    close = _clean(last.get("close",0))
                    open_ = _clean(last.get("open",0))
                    if open_ > 0:
                        result[code] = round((close-open_)/open_*100, 2)
        except Exception:
            pass
        if result:
            return result

    # ── TWSE mis ──
    try:
        sess = requests.Session()
        sess.get("https://mis.twse.com.tw/stock/index.jsp", headers=MIS_HEADERS, timeout=6)
        bsz = 30
        for i in range(0, len(codes), bsz):
            batch = codes[i:i+bsz]
            q = "|".join(f"tse_{c}.tw" for c in batch)
            r = sess.get(
                f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={q}&json=1&delay=0",
                headers=MIS_HEADERS, timeout=8)
            for item in r.json().get("msgArray",[]):
                code = item.get("c","")
                z = _clean(item.get("z") or item.get("o") or 0)
                y = _clean(item.get("y") or 0)
                if y > 0 and z > 0:
                    result[code] = round((z-y)/y*100, 2)
            time.sleep(0.05)
        if result:
            return result
    except Exception:
        pass

    # ── Yahoo Finance quote ──
    try:
        bsz = 20
        for i in range(0, len(codes), bsz):
            batch = codes[i:i+bsz]
            syms  = ",".join(f"{c}.TW" for c in batch)
            r = requests.get(
                f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={syms}"
                f"&fields=regularMarketChangePercent",
                headers=YAHOO_HEADERS, timeout=10)
            for q in r.json().get("quoteResponse",{}).get("result",[]):
                code = str(q.get("symbol","")).replace(".TW","")
                result[code] = round(_clean(q.get("regularMarketChangePercent",0)), 2)
            time.sleep(0.05)
    except Exception:
        pass

    return result


@st.cache_data(ttl=600, show_spinner=False)
def _enrich_names_finmind(df: pd.DataFrame) -> pd.DataFrame:
    """用 FinMind TaiwanStockInfo 補股票名稱"""
    try:
        params = {"dataset": "TaiwanStockInfo"}
        if FINMIND_TOKEN:
            params["token"] = FINMIND_TOKEN
        r = requests.get(FINMIND_BASE, params=params, timeout=15)
        data = r.json()
        if data.get("status") == 200 and data.get("data"):
            info = pd.DataFrame(data["data"])
            # 欄位: stock_id, stock_name, industry_category, type
            id_col   = next((c for c in info.columns if "stock_id" in c.lower()), None)
            name_col = next((c for c in info.columns if "stock_name" in c.lower() or "name" in c.lower()), None)
            if id_col and name_col:
                name_map = dict(zip(info[id_col].astype(str), info[name_col].astype(str)))
                df["name"] = df["code"].map(name_map).fillna(df["name"])
    except Exception:
        pass
    return df


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
                dict(cfg),
                scopes=["https://www.googleapis.com/auth/spreadsheets",
                        "https://www.googleapis.com/auth/drive"])
            return gspread.authorize(creds)
    except Exception as e:
        st.sidebar.caption(f"ℹ️ Sheets: {e}")
    return None


def save_today(client, df: pd.DataFrame, date_key: str):
    if not client or len(df)==0: return
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
    except Exception as e:
        st.sidebar.caption(f"⚠️ Sheets 寫入: {e}")


def load_prev_codes(client, today_key: str) -> set:
    if not client: return set()
    try:
        sh     = client.open_by_key(GOOGLE_SHEETS_ID)
        valid  = sorted(ws.title for ws in sh.worksheets()
                        if re.match(r"^\d{4}-\d{2}-\d{2}$", ws.title) and ws.title < today_key)
        if not valid: return set()
        recs = sh.worksheet(valid[-1]).get_all_records()
        return {str(r.get("代號","")).strip() for r in recs if r.get("代號")}
    except Exception:
        return set()


def load_history(client) -> dict:
    if not client: return {}
    out = {}
    try:
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        ws_list = [ws for ws in sh.worksheets()
                   if re.match(r"^\d{4}-\d{2}-\d{2}$", ws.title)]
        for ws in sorted(ws_list, key=lambda x: x.title, reverse=True)[:60]:
            try:
                recs = ws.get_all_records()
                if recs: out[ws.title] = pd.DataFrame(recs)
            except Exception:
                pass
    except Exception:
        pass
    return out


# ─────────────────────────────────────────────────────────────────────────────
# TABLE – 只顯示 排行 / 股票名稱 / 漲跌幅 / 成交金額(億)
# ─────────────────────────────────────────────────────────────────────────────
def build_styled_table(df: pd.DataFrame, prev_codes: set, cb_codes: set,
                       extra: list = None) -> object:
    rows, pcts, news = [], [], []

    for _, r in df.iterrows():
        code   = str(r["code"]).strip()
        pct    = float(r.get("change_pct") or 0)
        is_new = bool(prev_codes) and (code not in prev_codes)
        has_cb = code in cb_codes

        tags = []
        if is_new: tags.append("★新")
        if has_cb: tags.append("CB")
        name_cell = str(r["name"]) + ("  "+"  ".join(tags) if tags else "")
        pct_str   = (f"▲ {pct:.2f}%" if pct>0 else f"▼ {abs(pct):.2f}%" if pct<0 else "─")

        row_d = {
            "排行":        int(r.get("rank", _+1)),
            "股票名稱":    name_cell,
            "漲跌幅":      pct_str,
            "成交金額(億)": float(r.get("trade_value", 0)),
        }
        if extra:
            for src, dst in extra:
                row_d[dst] = r.get(src, "")

        rows.append(row_d); pcts.append(pct); news.append(is_new)

    disp = pd.DataFrame(rows)
    disp["__p"] = pcts
    disp["__n"] = news

    vis = ["排行","股票名稱","漲跌幅","成交金額(億)"]
    if extra: vis += [dst for _,dst in extra]

    def row_bg(row):
        bg = ("#191000" if row["__n"] else
              "#1a0808" if row["__p"]>0 else
              "#041008" if row["__p"]<0 else "#0a1520")
        return [bg]*len(row)

    def col_pct(col):
        return ["color:#e74c3c;font-weight:600" if v>0
                else ("color:#2ecc71;font-weight:600" if v<0 else "color:#5a6a80")
                for v in disp["__p"]]

    def col_name(col):
        styles = []
        for i, val in enumerate(disp["股票名稱"]):
            n, cb = disp["__n"].iloc[i], "CB" in str(val)
            styles.append("color:#f39c12;font-weight:700" if n
                          else "color:#a78bfa" if cb else "color:#c8d6e5")
        return styles

    def col_rank(col):
        return ["color:#4fc3f7;font-weight:700" if v<=3 else "color:#4a6080"
                for v in disp["排行"]]

    fmt = {"成交金額(億)":"{:,.1f}"}
    if extra:
        for _,dst in extra:
            if "億" in dst: fmt[dst]="{:,.1f}"

    styled = (
        disp.style
        .apply(row_bg,   axis=1)
        .apply(col_pct,  subset=["漲跌幅"])
        .apply(col_name, subset=["股票名稱"])
        .apply(col_rank, subset=["排行"])
        .format(fmt)
        .set_properties(**{"font-family":"'IBM Plex Mono',monospace","font-size":"13px","border":"none"})
        .set_properties(subset=["排行"],                      **{"text-align":"center"})
        .set_properties(subset=["股票名稱"],                   **{"text-align":"left"})
        .set_properties(subset=["漲跌幅","成交金額(億)"],      **{"text-align":"right"})
        .set_table_styles([
            {"selector":"thead th","props":[
                ("background-color","#0a1520"),("color","#4a6080"),
                ("font-family","'IBM Plex Mono',monospace"),("font-size","11px"),
                ("letter-spacing","1.5px"),("text-transform","uppercase"),
                ("border-bottom","1px solid #1a2940"),("padding","10px 14px"),
            ]},
            {"selector":"tbody td","props":[
                ("padding","10px 14px"),("border-bottom","1px solid #0d1a28"),
            ]},
            {"selector":"tbody tr:hover td","props":[("filter","brightness(1.3)")]},
            {"selector":"table","props":[("width","100%"),("border-collapse","collapse")]},
        ])
        .hide(axis="index")
        .hide(subset=["__p","__n"], axis="columns")
    )
    return styled


# ─────────────────────────────────────────────────────────────────────────────
# SHARED UI
# ─────────────────────────────────────────────────────────────────────────────
def render_kpi(df: pd.DataFrame, prev_codes: set, cb_codes: set):
    up      = int((df["change_pct"]>0).sum())
    dn      = int((df["change_pct"]<0).sum())
    nc      = len(df)-up-dn
    new_cnt = sum(1 for c in df["code"].astype(str) if prev_codes and c not in prev_codes)
    cb_cnt  = sum(1 for c in df["code"].astype(str) if c in cb_codes)
    total   = len(df) or 1
    up_pct  = round(up/total*100)
    dn_pct  = round(dn/total*100)
    nc_pct  = 100-up_pct-dn_pct

    st.markdown(f"""
    <div class="kpi-strip">
        <div class="kpi">
            <div class="kpi-label">上榜股數</div>
            <div class="kpi-value" style="color:#4fc3f7">{total}</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">漲 / 平 / 跌 比例</div>
            <div class="ratio-wrap">
                <span class="kpi-value" style="color:#e74c3c">{up}</span>
                <span style="color:#4a6080;font-size:20px">/</span>
                <span class="kpi-value" style="color:#5a6a80;font-size:20px">{nc}</span>
                <span style="color:#4a6080;font-size:20px">/</span>
                <span class="kpi-value" style="color:#2ecc71">{dn}</span>
            </div>
            <div class="ratio-bar" style="--up:{up_pct}%;--dn:{dn_pct}%"></div>
            <div class="ratio-labels">
                <span style="color:#e74c3c">▲ {up_pct}%</span>
                <span>─ {nc_pct}%</span>
                <span style="color:#2ecc71">▼ {dn_pct}%</span>
            </div>
        </div>
        <div class="kpi">
            <div class="kpi-label">新上榜 ★</div>
            <div class="kpi-value" style="color:#f39c12">{new_cnt}</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">CB 上榜</div>
            <div class="kpi-value" style="color:#a78bfa">{cb_cnt}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_legend():
    st.markdown("""
    <div class="legend-strip">
        <span style="color:#4a6080;font-size:10px;letter-spacing:1px;text-transform:uppercase">圖例</span>
        <div class="leg"><div class="dot" style="background:#4a1212"></div><span style="color:#e74c3c">上漲</span></div>
        <div class="leg"><div class="dot" style="background:#0a3018"></div><span style="color:#2ecc71">下跌</span></div>
        <div class="leg"><div class="dot" style="background:#3a2500"></div>
            <span style="color:#f39c12">★新 = 今日新上榜（與前日比較）</span></div>
        <div class="leg" style="color:#a78bfa">CB = 已發行可轉債</div>
    </div>
    """, unsafe_allow_html=True)


def _prep_hist_df(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    rename = {}
    for col in df.columns:
        lc = col.lower()
        if col in ("代號","code") or lc=="stock_id":           rename[col]="code"
        elif col in ("名稱","name") or lc=="stock_name":        rename[col]="name"
        elif "成交金額" in col or col=="trade_value":            rename[col]="trade_value"
        elif "漲跌幅" in col or col in ("change_pct","漲跌%"):  rename[col]="change_pct"
    df = df.rename(columns=rename)
    for c in ["code","name","trade_value","change_pct"]:
        if c not in df.columns:
            df[c] = ("" if c in ["code","name"] else 0.0)
    df["trade_value"] = pd.to_numeric(df["trade_value"], errors="coerce").fillna(0)
    df["change_pct"]  = pd.to_numeric(df["change_pct"],  errors="coerce").fillna(0)
    df["rank"]        = range(1,len(df)+1)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 1 – REAL-TIME
# ─────────────────────────────────────────────────────────────────────────────
def page_realtime():
    now     = datetime.now()
    is_open = (
        now.weekday() < 5
        and now.replace(hour=9,  minute=0, second=0, microsecond=0) <= now
        <= now.replace(hour=13, minute=30, second=0, microsecond=0)
    )

    pill_cls   = "status-pill" if is_open else "status-pill closed"
    pill_label = '<span class="blink">●</span> 盤中即時' if is_open else "● 非交易時段"
    st.markdown(f"""
    <div class="topbar">
        <div>
            <div class="topbar-logo">台股成交金額 TOP 30</div>
            <div class="topbar-sub">TWSE · DAILY VOLUME LEADERS</div>
        </div>
        <div style="text-align:right">
            <div class="{pill_cls}">{pill_label}</div>
            <div style="font-family:'IBM Plex Mono',monospace;font-size:11px;color:#2a3a50;margin-top:6px">
                {now.strftime("%Y/%m/%d &nbsp; %H:%M:%S")}
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### ⚙️ 控制面板")
        auto = st.toggle("自動刷新", value=is_open)
        ivl  = st.select_slider("刷新間隔(秒)", [15,30,60,120], value=60)
        if st.button("⟳ 立即刷新", use_container_width=True):
            st.cache_data.clear(); st.rerun()
        st.markdown("---")
        st.caption(
            "📡 資料來源優先順序\n\n"
            "① FinMind TaiwanStockPrice\n"
            "② TWSE STOCK_DAY_ALL\n"
            "③ TWSE MI_INDEX20\n"
            "④ Yahoo Finance Screener\n"
            "⑤ Yahoo Finance Quote\n\n"
            "即時漲跌:\n"
            "① FinMind (需token)\n"
            "② TWSE mis\n"
            "③ Yahoo quote\n\n"
            "CB: FinMind / TWSE"
        )
        if FINMIND_TOKEN:
            st.success("✅ FinMind token 已設定")
        else:
            st.info("💡 設定 finmind_token 可啟用即時資料")

    today_key = now.strftime("%Y-%m-%d")
    trade_date = _last_trading_date()

    with st.spinner("載入成交資料中…"):
        df, src = fetch_top30(trade_date)

    if len(df) == 0:
        st.error(
            "❌ 所有資料來源均無法取得資料。\n\n"
            "可能原因：\n"
            "- 今日非交易日（假日/休市）\n"
            "- 部署環境網路限制（Streamlit Cloud 可能封鎖部分 IP）\n"
            "- 請嘗試設定 FinMind token（免費註冊）\n\n"
            "解決方法：\n"
            "1. 前往 https://finmindtrade.com 免費註冊取得 token\n"
            "2. 在 `.streamlit/secrets.toml` 加入 `finmind_token = 'your_token'`"
        )
        return

    # 盤中更新即時漲跌
    if is_open and len(df) > 0:
        with st.spinner("取得即時行情…"):
            rt = fetch_realtime_changes(df["code"].tolist())
        for idx, row in df.iterrows():
            p = rt.get(str(row["code"]))
            if p is not None:
                df.at[idx, "change_pct"] = p

    cb_codes   = fetch_cb_stocks()
    client     = gs_client()
    prev_codes = load_prev_codes(client, today_key)

    if is_open or now.hour >= 14:
        save_today(client, df, today_key)

    render_kpi(df, prev_codes, cb_codes)
    render_legend()

    st.markdown(
        f'<div class="refresh-info">'
        f'最後更新 {now.strftime("%H:%M:%S")} &nbsp;'
        f'<span class="src-tag">{src}</span>'
        f'</div>',
        unsafe_allow_html=True)

    styled = build_styled_table(df, prev_codes, cb_codes)
    st.dataframe(styled, use_container_width=True, height=980, hide_index=True)

    if not is_open:
        st.info(f"📌 非交易時段，顯示 {trade_date} 收盤資料。")

    if auto and is_open:
        time.sleep(ivl); st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 2 – HISTORY
# ─────────────────────────────────────────────────────────────────────────────
def page_history():
    st.markdown("""
    <div class="topbar">
        <div>
            <div class="topbar-logo">歷史成交金額排行</div>
            <div class="topbar-sub">HISTORICAL VOLUME LEADERS</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    client   = gs_client()
    history  = load_history(client)
    cb_codes = fetch_cb_stocks()

    if not history:
        st.warning(
            "尚無歷史資料。請確認：\n"
            "1. Google Sheets API 已設定\n"
            "2. 服務帳號已加入 Sheet 共用編輯者\n"
            "3. 系統曾在交易日 14:30 後成功儲存資料"
        )
        return

    dates      = sorted(history.keys(), reverse=True)
    all_sorted = sorted(history.keys())

    c1, c2 = st.columns([3,1])
    with c1:
        selected = st.multiselect("選擇日期（可多選）", dates,
                                   default=dates[:7] if len(dates)>=7 else dates)
    with c2:
        mode = st.radio("顯示模式", ["每日明細","彙總排行"], horizontal=True)

    if not selected:
        st.info("請選擇至少一個日期"); return

    if mode == "每日明細":
        for date in sorted(selected, reverse=True):
            raw = history.get(date)
            if raw is None or len(raw)==0: continue
            df = _prep_hist_df(raw)

            prev_c = set()
            idx = all_sorted.index(date) if date in all_sorted else -1
            if idx > 0:
                try: prev_c = set(_prep_hist_df(history[all_sorted[idx-1]])["code"].astype(str))
                except: pass

            with st.expander(f"📅 {date}", expanded=(date==sorted(selected,reverse=True)[0])):
                render_kpi(df, prev_c, cb_codes)
                render_legend()
                st.dataframe(build_styled_table(df, prev_c, cb_codes),
                             use_container_width=True, height=600, hide_index=True)

    else:
        all_rows = []
        for d in selected:
            raw = history.get(d)
            if raw is None: continue
            tmp = _prep_hist_df(raw); tmp["_date"]=d; all_rows.append(tmp)

        if not all_rows:
            st.warning("選擇的日期無有效資料"); return

        combined = pd.concat(all_rows, ignore_index=True)
        agg = (combined.groupby(["code","name"])
               .agg(avg_val=("trade_value","mean"), total_val=("trade_value","sum"),
                    days=("trade_value","count"),   avg_pct=("change_pct","mean"))
               .reset_index()
               .sort_values("total_val",ascending=False).head(30).reset_index(drop=True))
        agg["rank"]        = range(1,len(agg)+1)
        agg["trade_value"] = agg["avg_val"]
        agg["change_pct"]  = agg["avg_pct"].round(2)

        sel_sorted = sorted(selected)
        prev_c = set()
        if sel_sorted[0] in all_sorted:
            i = all_sorted.index(sel_sorted[0])
            if i>0:
                try: prev_c = set(_prep_hist_df(history[all_sorted[i-1]])["code"].astype(str))
                except: pass

        period = f"{sel_sorted[0]} ~ {sel_sorted[-1]}" if len(sel_sorted)>1 else sel_sorted[0]
        st.markdown(f'<div class="section-title">彙總 · {period} · {len(sel_sorted)} 交易日</div>',
                    unsafe_allow_html=True)
        render_legend()
        extra = [("avg_val","平均成交(億)"),("total_val","累積成交(億)"),("days","上榜天數")]
        st.dataframe(
            build_styled_table(agg, prev_c, cb_codes, extra=extra),
            use_container_width=True, height=700, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    page = st.radio("nav", ["📈  即時排行","📊  歷史排行"],
                    horizontal=True, label_visibility="collapsed")
    if "即時" in page: page_realtime()
    else:              page_history()

if __name__ == "__main__":
    main()
