"""
台股成交金額 TOP 30 追蹤系統

資料來源策略（依優先順序）:
  成交金額排行:
    1. TWSE mis 即時API (盤中)  → mis.twse.com.tw
    2. TWSE 盤後API              → twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL
    3. Yahoo Finance screener    → query1.finance.yahoo.com (備用)

  即時漲跌:
    1. TWSE mis getStockInfo
    2. Yahoo Finance quote API

  CB 可轉債:
    1. TWSE CB_OVERVIEW API
    2. TWSE CB_BOND_INFO (上市可轉債明細)
"""

import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime
import re

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
.topbar-logo { font-family:'IBM Plex Mono',monospace; font-size:20px; font-weight:600; color:#4fc3f7; letter-spacing:2px; }
.topbar-sub  { font-size:11px; color:#4a6080; margin-top:2px; letter-spacing:1px; }

.status-pill {
    display:inline-flex; align-items:center; gap:7px;
    background:#0a1e12; border:1px solid #1a5c28; border-radius:4px;
    padding:5px 14px; font-family:'IBM Plex Mono',monospace; font-size:12px; color:#2ecc71;
}
.status-pill.closed { background:#12131a; border-color:#2a3040; color:#5a6a80; }
.blink { animation:blink 1.2s step-start infinite; }
@keyframes blink { 50%{opacity:0;} }

/* KPI strip – 4 cards */
.kpi-strip { display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-bottom:20px; }
.kpi { background:#0a1520; border:1px solid #1a2940; border-radius:4px; padding:14px 18px; }
.kpi-label { font-size:10px; color:#4a6080; letter-spacing:1.5px; text-transform:uppercase; margin-bottom:8px; }
.kpi-value { font-family:'IBM Plex Mono',monospace; font-size:24px; font-weight:600; }
.kpi-value.up   { color:#e74c3c; }
.kpi-value.dn   { color:#2ecc71; }
.kpi-value.gold { color:#f39c12; }

/* ratio bar */
.ratio-bar {
    height:6px; border-radius:3px; margin-top:10px;
    background: linear-gradient(to right, #e74c3c var(--up), #2a3a50 var(--up) calc(100% - var(--dn)), #2ecc71 calc(100% - var(--dn)));
}
.ratio-labels { display:flex; justify-content:space-between; font-size:10px; margin-top:4px; color:#4a6080; }

.legend-strip {
    display:flex; align-items:center; gap:20px; flex-wrap:wrap;
    background:#0a1520; border:1px solid #1a2940; border-radius:4px;
    padding:8px 16px; font-size:11px; color:#4a6080; margin-bottom:14px;
}
.leg { display:flex; align-items:center; gap:6px; }
.dot { width:8px; height:8px; border-radius:1px; }

.refresh-info {
    font-family:'IBM Plex Mono',monospace; font-size:11px; color:#2a3a50;
    text-align:right; margin-bottom:10px;
}
.section-title {
    font-family:'IBM Plex Mono',monospace; font-size:12px; color:#4a6080;
    letter-spacing:2px; text-transform:uppercase; margin:24px 0 12px;
    display:flex; align-items:center; gap:10px;
}
.section-title::after { content:''; flex:1; height:1px; background:#1a2940; }

div[data-testid="stRadio"] > div {
    display:flex !important; flex-direction:row !important; gap:0 !important;
    background:#0a1520; border:1px solid #1a2940; border-radius:4px; padding:2px; width:fit-content;
}
div[data-testid="stRadio"] label {
    font-family:'IBM Plex Mono',monospace !important; font-size:12px !important;
    padding:6px 16px !important; border-radius:3px !important; cursor:pointer; color:#4a6080 !important;
}
div[data-testid="stRadio"] label[data-checked="true"] { background:#1a2940 !important; color:#4fc3f7 !important; }
[data-testid="stDataFrame"] { border:1px solid #1a2940 !important; border-radius:4px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
GOOGLE_SHEETS_ID = "1lTRxbT9iv3uRIrQ1wUFPV0LZbo8p5Qoh4a1FMGa-iU4"

TWSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
    "Referer":    "https://www.twse.com.tw/",
    "Accept":     "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9",
}
MIS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
    "Referer":    "https://mis.twse.com.tw/",
    "Accept":     "application/json",
}
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
    "Accept":     "application/json",
}


def _clean(val, default=0.0):
    """Parse numeric string with commas/signs."""
    try:
        s = str(val).replace(",", "").replace("+", "").strip()
        if s in ("", "--", "-", "N/A", "nan"): return default
        return float(s)
    except Exception:
        return default


# ─────────────────────────────────────────────────────────────────────────────
# CB DATA  — TWSE 上市可轉債明細
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_cb_stocks() -> set:
    """
    爬取目前流通中的可轉債，回傳對應的股票代號集合。
    來源 1: TWSE CB_OVERVIEW
    來源 2: TWSE CB_BOND_INFO (上市可轉債明細表)
    """
    codes = set()

    # ── 來源1: CB_OVERVIEW ──
    try:
        r = requests.get(
            "https://www.twse.com.tw/rwd/zh/cbInfo/CB_OVERVIEW",
            headers=TWSE_HEADERS, timeout=12)
        data = r.json()
        if data.get("stat") == "OK":
            # 欄位通常: CB代號, CB名稱, 股票代號, 股票名稱, ...
            fields = data.get("fields", [])
            stock_col = None
            for i, f in enumerate(fields):
                if "股票代號" in str(f) or "標的" in str(f):
                    stock_col = i
                    break

            for row in data.get("data", []):
                try:
                    # 先嘗試指定欄位
                    if stock_col is not None:
                        sc = str(row[stock_col]).strip()
                        if re.match(r"^\d{4}$", sc):
                            codes.add(sc)
                    # 再從 CB 代號推導（前4碼）
                    cb = str(row[0]).strip()
                    if re.match(r"^\d{5,6}", cb):
                        codes.add(cb[:4])
                    # 掃所有欄位找4碼數字
                    for cell in row:
                        s = str(cell).strip()
                        if re.match(r"^\d{4}$", s):
                            codes.add(s)
                except Exception:
                    pass
            if codes:
                return codes
    except Exception:
        pass

    # ── 來源2: CB_BOND_INFO ──
    try:
        r = requests.get(
            "https://www.twse.com.tw/rwd/zh/cbInfo/CB_BOND_INFO",
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

    # ── 來源3: 公開資訊觀測站 HTML 解析 ──
    try:
        r = requests.post(
            "https://mops.twse.com.tw/mops/web/ajax_t100sb01",
            data={"encodeURIComponent":"1","step":"1","firstin":"1",
                  "off":"1","TYPEK":"all","isnew":"false"},
            headers={**TWSE_HEADERS, "Referer":"https://mops.twse.com.tw/"},
            timeout=15)
        tables = pd.read_html(r.text, encoding="utf-8")
        for tbl in tables:
            for col in tbl.columns:
                for val in tbl[col].astype(str):
                    s = val.strip()
                    if re.match(r"^\d{4}$", s):
                        codes.add(s)
        if codes:
            return codes
    except Exception:
        pass

    return codes  # 空集合（不 fallback 靜態）


# ─────────────────────────────────────────────────────────────────────────────
# TRADING VALUE TOP30
# ─────────────────────────────────────────────────────────────────────────────
def _parse_twse_stock_day_all(data: dict) -> pd.DataFrame | None:
    """Parse TWSE STOCK_DAY_ALL response."""
    try:
        if data.get("stat") != "OK" or not data.get("data"):
            return None
        rows = data["data"]
        # 自動偵測欄數
        ncols = len(rows[0])
        # 標準欄位順序(12欄): 代號,名稱,成交股數,成交筆數,成交金額,開盤,最高,最低,收盤,漲跌(+/-),漲跌價差,本益比
        base_cols = ["code","name","vol","txn","trade_value_raw",
                     "open","high","low","close","sign","change_raw","pe"]
        cols = (base_cols[:ncols] if ncols <= len(base_cols)
                else base_cols + [f"_x{i}" for i in range(ncols-len(base_cols))])
        df = pd.DataFrame(rows, columns=cols)

        df["trade_value"] = df["trade_value_raw"].apply(_clean) / 1e8
        df["close"]       = df["close"].apply(_clean)
        df["sign_val"]    = df["sign"].astype(str).str.strip().map({"+":1,"-":-1,"":0}).fillna(0)
        df["change_num"]  = df["change_raw"].apply(_clean)
        df["change_abs"]  = df["change_num"].abs() * df["sign_val"]
        df["prev_close"]  = (df["close"] - df["change_abs"]).replace(0, float("nan"))
        df["change_pct"]  = (df["change_abs"] / df["prev_close"] * 100).round(2).fillna(0)

        # 只保留上市一般股（4碼數字）
        df = df[df["code"].str.match(r"^\d{4}$") & (df["trade_value"] > 0)]
        df = df.sort_values("trade_value", ascending=False).head(30).reset_index(drop=True)
        df["rank"] = range(1, len(df)+1)
        return df[["rank","code","name","trade_value","change_pct"]]
    except Exception as e:
        return None


@st.cache_data(ttl=55, show_spinner=False)
def fetch_top30_aftermarket() -> tuple[pd.DataFrame, str]:
    """
    盤後成交金額排行（最新交易日）。
    回傳 (df, source_label)
    """
    # ── 來源1: TWSE STOCK_DAY_ALL（無參數=最新交易日）──
    try:
        r = requests.get(
            "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL",
            headers=TWSE_HEADERS, timeout=15)
        df = _parse_twse_stock_day_all(r.json())
        if df is not None and len(df) >= 10:
            return df, "TWSE STOCK_DAY_ALL"
    except Exception:
        pass

    # ── 來源2: TWSE MI_INDEX20（指定今日日期）──
    try:
        today = datetime.now().strftime("%Y%m%d")
        r = requests.get(
            f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX20"
            f"?date={today}&selectType=ALL",
            headers=TWSE_HEADERS, timeout=15)
        data = r.json()
        if data.get("stat") == "OK" and data.get("data"):
            rows = data["data"]
            # MI_INDEX20 欄位: 代號,名稱,成交股數,成交金額,開盤,最高,最低,收盤,漲跌,漲跌幅
            cols = ["code","name","vol","trade_value_raw",
                    "open","high","low","close","change","change_pct_raw"]
            ncols = len(rows[0])
            df = pd.DataFrame(rows, columns=(cols[:ncols] if ncols<=len(cols)
                                             else cols+[f"_x{i}" for i in range(ncols-len(cols))]))
            df["trade_value"]  = df["trade_value_raw"].apply(_clean) / 1e8
            df["change_pct"]   = df["change_pct_raw"].apply(
                lambda v: _clean(str(v).replace("%","").replace("+","")))
            df = df[df["code"].str.match(r"^\d{4}$") & (df["trade_value"] > 0)]
            df = df.sort_values("trade_value", ascending=False).head(30).reset_index(drop=True)
            df["rank"] = range(1, len(df)+1)
            if len(df) >= 10:
                return df[["rank","code","name","trade_value","change_pct"]], "TWSE MI_INDEX20"
    except Exception:
        pass

    # ── 來源3: Yahoo Finance TW screener ──
    try:
        df, src = _fetch_yahoo_screener()
        if df is not None and len(df) >= 10:
            return df, src
    except Exception:
        pass

    return pd.DataFrame(), "無法取得資料"


@st.cache_data(ttl=28, show_spinner=False)
def fetch_top30_realtime() -> tuple[pd.DataFrame, str]:
    """
    盤中即時成交金額排行。
    TWSE mis API 提供即時成交量，但需要先取得全部股票清單再批次查詢。
    策略：先用盤後資料取得代號清單，再用 mis 更新即時成交與漲跌幅。
    """
    # Step1: 取盤後清單（作為股票池）
    df_base, _ = fetch_top30_aftermarket()

    # Step2: 嘗試 mis 即時資料
    try:
        # 先取 session
        session = requests.Session()
        session.get("https://mis.twse.com.tw/stock/index.jsp",
                    headers=MIS_HEADERS, timeout=8)

        if len(df_base) > 0:
            codes = df_base["code"].tolist()
            rt = _fetch_mis_batch(session, codes)
            if rt:
                for idx, row in df_base.iterrows():
                    info = rt.get(str(row["code"]))
                    if info:
                        df_base.at[idx, "change_pct"] = info["change_pct"]
                        # 更新即時成交金額
                        if info.get("trade_value", 0) > 0:
                            df_base.at[idx, "trade_value"] = info["trade_value"]
                df_base = df_base.sort_values("trade_value", ascending=False).reset_index(drop=True)
                df_base["rank"] = range(1, len(df_base)+1)
                return df_base, "TWSE MIS 即時"
    except Exception:
        pass

    # Step3: Yahoo Finance quote 更新漲跌幅
    try:
        if len(df_base) > 0:
            codes = df_base["code"].tolist()
            yahoo_rt = _fetch_yahoo_quotes(codes)
            if yahoo_rt:
                for idx, row in df_base.iterrows():
                    info = yahoo_rt.get(str(row["code"]))
                    if info:
                        df_base.at[idx, "change_pct"] = info.get("change_pct", row["change_pct"])
                return df_base, "Yahoo Finance 即時"
    except Exception:
        pass

    return df_base, "盤後靜態資料"


def _fetch_mis_batch(session, codes: list) -> dict:
    """批次查詢 TWSE mis 即時行情，回傳 {code: {change_pct, trade_value}}"""
    result = {}
    batch_size = 30
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i+batch_size]
        try:
            q = "|".join(f"tse_{c}.tw" for c in batch)
            r = session.get(
                f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
                f"?ex_ch={q}&json=1&delay=0",
                headers=MIS_HEADERS, timeout=8)
            for item in r.json().get("msgArray", []):
                code = item.get("c","")
                try:
                    z = _clean(item.get("z") or item.get("o") or 0)
                    y = _clean(item.get("y") or 0)
                    v = _clean(item.get("v") or 0)       # 成交股數(千股)
                    close = z if z > 0 else y
                    chg_pct = round((close - y)/y*100, 2) if y > 0 else 0
                    tv = round(close * v * 1000 / 1e8, 1)  # 億元
                    result[code] = {"change_pct": chg_pct, "trade_value": tv}
                except Exception:
                    pass
        except Exception:
            pass
        time.sleep(0.05)
    return result


def _fetch_yahoo_screener() -> tuple:
    """Yahoo Finance screener 取台股成交金額 TOP。"""
    try:
        # Yahoo screener: 台股依成交金額排序
        url = ("https://query1.finance.yahoo.com/v1/finance/screener"
               "?formatted=true&lang=zh-TW&region=TW&corsDomain=finance.yahoo.com")
        payload = {
            "size": 30, "offset": 0, "sortField": "dayvolume", "sortType": "DESC",
            "quoteType": "EQUITY", "topOperator": "AND",
            "query": {"operator":"AND","operands":[
                {"operator":"eq","operands":["region","tw"]},
                {"operator":"eq","operands":["exchange","TAI"]},
            ]},
            "userId": "", "userIdType": "guid",
        }
        r = requests.post(url, json=payload, headers=YAHOO_HEADERS, timeout=12)
        quotes = r.json()["finance"]["result"][0]["quotes"]
        rows = []
        for i, q in enumerate(quotes):
            sym   = q.get("symbol","").replace(".TW","")
            name  = q.get("shortName", q.get("longName",""))
            tv    = _clean(q.get("regularMarketVolume",{}).get("raw",0)) * \
                    _clean(q.get("regularMarketPrice",{}).get("raw",0)) / 1e8
            cpct  = _clean(q.get("regularMarketChangePercent",{}).get("raw",0))
            rows.append({"rank":i+1,"code":sym,"name":name,
                         "trade_value":round(tv,1),"change_pct":round(cpct,2)})
        df = pd.DataFrame(rows)
        df = df[df["trade_value"]>0].sort_values("trade_value",ascending=False).head(30).reset_index(drop=True)
        df["rank"] = range(1,len(df)+1)
        return df, "Yahoo Finance Screener"
    except Exception:
        return None, ""


def _fetch_yahoo_quotes(codes: list) -> dict:
    """Yahoo Finance batch quote，回傳 {code: {change_pct}}"""
    result = {}
    batch_size = 20
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i+batch_size]
        try:
            syms = ",".join(f"{c}.TW" for c in batch)
            r = requests.get(
                f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={syms}",
                headers=YAHOO_HEADERS, timeout=10)
            for q in r.json().get("quoteResponse",{}).get("result",[]):
                code = q.get("symbol","").replace(".TW","")
                cpct = round(_clean(q.get("regularMarketChangePercent", 0)), 2)
                result[code] = {"change_pct": cpct}
        except Exception:
            pass
        time.sleep(0.05)
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
                dict(cfg),
                scopes=["https://www.googleapis.com/auth/spreadsheets",
                        "https://www.googleapis.com/auth/drive"])
            return gspread.authorize(creds)
    except Exception as e:
        st.sidebar.caption(f"ℹ️ Google Sheets: {e}")
    return None


def save_today(client, df: pd.DataFrame, date_key: str):
    if not client or len(df) == 0: return
    try:
        import gspread
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        try:
            ws = sh.worksheet(date_key); ws.clear()
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=date_key, rows=35, cols=5)
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
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        # 只取 YYYY-MM-DD 格式的 tab
        valid = sorted(ws.title for ws in sh.worksheets()
                       if re.match(r"^\d{4}-\d{2}-\d{2}$", ws.title) and ws.title < today_key)
        if not valid: return set()
        records = sh.worksheet(valid[-1]).get_all_records()
        return {str(r.get("代號","")).strip() for r in records if r.get("代號")}
    except Exception:
        return set()


def load_history(client) -> dict:
    if not client: return {}
    out = {}
    try:
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        valid_ws = [ws for ws in sh.worksheets()
                    if re.match(r"^\d{4}-\d{2}-\d{2}$", ws.title)]
        for ws in sorted(valid_ws, key=lambda x: x.title, reverse=True)[:60]:
            try:
                records = ws.get_all_records()
                if records:
                    out[ws.title] = pd.DataFrame(records)
            except Exception:
                pass
    except Exception:
        pass
    return out


# ─────────────────────────────────────────────────────────────────────────────
# TABLE  —  只顯示 排行 / 股票名稱 / 漲跌幅 / 成交金額(億)
#           內部用 _row_type 欄控制背景色，最後 hide 掉
# ─────────────────────────────────────────────────────────────────────────────
ROW_UP   = "background-color:#1a0808"
ROW_DN   = "background-color:#041008"
ROW_NEW  = "background-color:#191000"
ROW_NEUT = "background-color:#0a1520"

def build_styled_table(df: pd.DataFrame, prev_codes: set, cb_codes: set,
                       extra: list = None) -> object:
    """
    df 需含: rank, code, name, trade_value, change_pct
    extra: [(src_col, display_name), ...] 歷史彙總額外欄位
    """
    display_rows = []
    pct_vals, is_new_vals = [], []

    for _, r in df.iterrows():
        code   = str(r["code"]).strip()
        pct    = float(r.get("change_pct") or 0)
        is_new = bool(prev_codes) and (code not in prev_codes)
        has_cb = code in cb_codes

        name = str(r["name"])
        tags = []
        if is_new: tags.append("★新")
        if has_cb: tags.append("CB")
        name_cell = name + ("  " + "  ".join(tags) if tags else "")

        pct_str = (f"▲ {pct:.2f}%" if pct > 0
                   else f"▼ {abs(pct):.2f}%" if pct < 0
                   else "─")

        row_d = {
            "排行":        int(r.get("rank", _+1)),
            "股票名稱":    name_cell,
            "漲跌幅":      pct_str,
            "成交金額(億)": float(r.get("trade_value", 0)),
        }
        if extra:
            for src, dst in extra:
                row_d[dst] = r.get(src, "")

        display_rows.append(row_d)
        pct_vals.append(pct)
        is_new_vals.append(is_new)

    disp = pd.DataFrame(display_rows)
    disp["__pct"]    = pct_vals
    disp["__is_new"] = is_new_vals

    vis = ["排行","股票名稱","漲跌幅","成交金額(億)"]
    if extra:
        vis += [dst for _, dst in extra]

    # ── styler functions ──
    def row_bg(row):
        new = row["__is_new"]
        p   = row["__pct"]
        bg  = ROW_NEW if new else (ROW_UP if p>0 else (ROW_DN if p<0 else ROW_NEUT))
        return [f"{bg}"] * len(row)

    def col_pct(col):
        return ["color:#e74c3c;font-weight:600" if v>0
                else ("color:#2ecc71;font-weight:600" if v<0 else "color:#5a6a80")
                for v in disp["__pct"]]

    def col_name(col):
        styles = []
        for i, val in enumerate(disp["股票名稱"]):
            new = disp["__is_new"].iloc[i]
            cb  = "CB" in str(val)
            styles.append("color:#f39c12;font-weight:700" if new
                          else ("color:#a78bfa" if cb else "color:#c8d6e5"))
        return styles

    def col_rank(col):
        return ["color:#4fc3f7;font-weight:700" if v<=3 else "color:#4a6080"
                for v in disp["排行"]]

    fmt = {"成交金額(億)": "{:,.1f}"}
    if extra:
        for src, dst in extra:
            if "億" in dst: fmt[dst] = "{:,.1f}"

    styled = (
        disp.style
        .apply(row_bg,  axis=1)
        .apply(col_pct, subset=["漲跌幅"])
        .apply(col_name,subset=["股票名稱"])
        .apply(col_rank,subset=["排行"])
        .format(fmt)
        .set_properties(**{"font-family":"'IBM Plex Mono',monospace","font-size":"13px","border":"none"})
        .set_properties(subset=["排行"],           **{"text-align":"center"})
        .set_properties(subset=["股票名稱"],        **{"text-align":"left"})
        .set_properties(subset=["漲跌幅","成交金額(億)"], **{"text-align":"right"})
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
        .hide(subset=["__pct","__is_new"], axis="columns")
    )
    return styled


# ─────────────────────────────────────────────────────────────────────────────
# SHARED UI
# ─────────────────────────────────────────────────────────────────────────────
def render_kpi(df: pd.DataFrame, prev_codes: set):
    """4-card KPI: 上榜股數 / 上漲 / 下跌 / 新上榜 + 漲跌比例條"""
    up      = int((df["change_pct"] > 0).sum())
    dn      = int((df["change_pct"] < 0).sum())
    nc      = len(df) - up - dn
    new_cnt = sum(1 for c in df["code"].astype(str) if prev_codes and c not in prev_codes)
    total   = len(df)

    up_pct  = round(up/total*100) if total else 0
    dn_pct  = round(dn/total*100) if total else 0
    nc_pct  = 100 - up_pct - dn_pct

    st.markdown(f"""
    <div class="kpi-strip">
        <div class="kpi">
            <div class="kpi-label">上榜股數</div>
            <div class="kpi-value" style="color:#4fc3f7">{total}</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">上漲 ▲ / 下跌 ▼ 比例</div>
            <div style="display:flex;align-items:baseline;gap:10px;margin-top:4px">
                <span class="kpi-value up">{up}</span>
                <span style="color:#4a6080;font-size:18px">/</span>
                <span class="kpi-value dn">{dn}</span>
                <span style="color:#4a6080;font-size:18px">/</span>
                <span class="kpi-value" style="color:#5a6a80;font-size:18px">{nc}</span>
            </div>
            <div class="ratio-bar" style="--up:{up_pct}%;--dn:{dn_pct}%"></div>
            <div class="ratio-labels">
                <span style="color:#e74c3c">{up_pct}% 漲</span>
                <span>{nc_pct}% 平</span>
                <span style="color:#2ecc71">{dn_pct}% 跌</span>
            </div>
        </div>
        <div class="kpi">
            <div class="kpi-label">新上榜 ★</div>
            <div class="kpi-value gold">{new_cnt}</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">CB 上榜</div>
            <div class="kpi-value" style="color:#a78bfa" id="cb-count">—</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    return up, dn


def render_legend():
    st.markdown("""
    <div class="legend-strip">
        <span style="color:#4a6080;font-size:10px;letter-spacing:1px;text-transform:uppercase">圖例</span>
        <div class="leg"><div class="dot" style="background:#4a1212"></div><span style="color:#e74c3c">上漲</span></div>
        <div class="leg"><div class="dot" style="background:#0a3018"></div><span style="color:#2ecc71">下跌</span></div>
        <div class="leg"><div class="dot" style="background:#3a2500"></div><span style="color:#f39c12">★新 = 今日新上榜（與前日比較）</span></div>
        <div class="leg" style="color:#a78bfa">CB = 已發行可轉債</div>
    </div>
    """, unsafe_allow_html=True)


def _prep_hist_df(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    rename = {}
    for col in df.columns:
        if col in ("代號","stock_code","Code","code"):             rename[col]="code"
        elif col in ("名稱","stock_name","Name","name"):           rename[col]="name"
        elif "成交金額" in col or col=="trade_value":               rename[col]="trade_value"
        elif "漲跌幅" in col or col in ("change_pct","漲跌%"):     rename[col]="change_pct"
    df = df.rename(columns=rename)
    for c in ["code","name","trade_value","change_pct"]:
        if c not in df.columns:
            df[c] = "" if c in ["code","name"] else 0.0
    df["trade_value"] = pd.to_numeric(df["trade_value"], errors="coerce").fillna(0)
    df["change_pct"]  = pd.to_numeric(df["change_pct"],  errors="coerce").fillna(0)
    df["rank"]        = range(1, len(df)+1)
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
            "① TWSE mis 即時API\n"
            "② TWSE STOCK_DAY_ALL\n"
            "③ Yahoo Finance\n\n"
            "CB：TWSE CB_OVERVIEW\n"
            "歷史：Google Sheets"
        )

    today_key = now.strftime("%Y-%m-%d")

    # 載入資料
    with st.spinner("載入成交資料中…"):
        if is_open:
            df, src = fetch_top30_realtime()
        else:
            df, src = fetch_top30_aftermarket()

    if len(df) == 0:
        st.error("❌ 無法取得市場資料，請確認網路連線或稍後再試。")
        return

    cb_codes   = fetch_cb_stocks()
    client     = gs_client()
    prev_codes = load_prev_codes(client, today_key)

    if is_open or now.hour >= 14:
        save_today(client, df, today_key)

    cb_in_top = sum(1 for c in df["code"] if c in cb_codes)
    render_kpi(df, prev_codes)
    render_legend()

    st.markdown(
        f'<div class="refresh-info">'
        f'最後更新 {now.strftime("%H:%M:%S")} &nbsp;·&nbsp; '
        f'資料來源：{src} &nbsp;·&nbsp; CB 上榜：{cb_in_top} 支'
        f'</div>',
        unsafe_allow_html=True)

    styled = build_styled_table(df, prev_codes, cb_codes)
    st.dataframe(styled, use_container_width=True, height=980, hide_index=True)

    if not is_open:
        st.info("📌 非交易時段（09:00–13:30），顯示最近收盤資料。盤後資料約 14:30 後更新。")

    if auto and is_open:
        time.sleep(ivl)
        st.rerun()


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
            "尚無歷史資料。\n\n"
            "請確認：\n"
            "1. Google Sheets API 已設定（`.streamlit/secrets.toml`）\n"
            "2. 服務帳號已加入 Sheet 的共用編輯者\n"
            "3. 系統曾在交易日 14:30 後運行並完成儲存"
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
                try:
                    prev_c = set(_prep_hist_df(history[all_sorted[idx-1]])["code"].astype(str))
                except Exception: pass

            is_first = (date == sorted(selected, reverse=True)[0])
            with st.expander(f"📅 {date}", expanded=is_first):
                render_kpi(df, prev_c)
                render_legend()
                st.dataframe(build_styled_table(df, prev_c, cb_codes),
                             use_container_width=True, height=600, hide_index=True)

    else:  # 彙總排行
        all_rows = []
        for d in selected:
            raw = history.get(d)
            if raw is None: continue
            tmp = _prep_hist_df(raw); tmp["_date"] = d; all_rows.append(tmp)

        if not all_rows:
            st.warning("選擇的日期無有效資料"); return

        combined = pd.concat(all_rows, ignore_index=True)
        agg = (combined.groupby(["code","name"])
               .agg(avg_val=("trade_value","mean"), total_val=("trade_value","sum"),
                    days=("trade_value","count"),   avg_pct=("change_pct","mean"))
               .reset_index()
               .sort_values("total_val", ascending=False).head(30).reset_index(drop=True))
        agg["rank"]        = range(1, len(agg)+1)
        agg["trade_value"] = agg["avg_val"]
        agg["change_pct"]  = agg["avg_pct"].round(2)

        sel_sorted = sorted(selected)
        prev_c = set()
        if sel_sorted[0] in all_sorted:
            i = all_sorted.index(sel_sorted[0])
            if i > 0:
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
