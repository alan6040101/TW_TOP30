"""
台股成交金額 TOP 30
資料來源: yfinance (Yahoo Finance Python 套件)
  - 不依賴特定 IP，Streamlit Cloud 可用
  - 台股代號格式: 2330.TW (上市) / 6547.TWO (上櫃)
  - 成交金額 = 成交量 × 收盤價（日內估算）
"""

import streamlit as st
import pandas as pd
import yfinance as yf
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
# 台股股票池  — 上市主要股票（可擴充）
# 覆蓋市值前 150 大，確保 TOP30 成交金額排行準確
# ─────────────────────────────────────────────────────────────────────────────
STOCK_POOL = {
    # 半導體 / 電子
    "2330":"台積電","2454":"聯發科","2303":"聯電","2344":"華邦電","2408":"南亞科",
    "3034":"聯詠","2379":"瑞昱","3443":"創意","2337":"旺宏","6770":"力積電",
    "3711":"日月光投控","2449":"京元電子","3450":"聯鈞","2498":"宏達電",
    # 電腦 / 網通
    "2317":"鴻海","2382":"廣達","2357":"華碩","2353":"宏碁","3231":"緯創",
    "2308":"台達電","3008":"大立光","2395":"研華","2376":"技嘉","2301":"光寶科",
    "2345":"智邦","4938":"和碩","6415":"矽力-KY","3533":"嘉澤","2356":"英業達",
    "2385":"群光","3008":"大立光","2392":"正崴","2474":"可成","6669":"緯穎",
    "3481":"群創","2360":"致茂","3005":"神基","6274":"台燿",
    # 金融
    "2881":"富邦金","2882":"國泰金","2886":"兆豐金","2891":"中信金","2884":"玉山金",
    "2885":"元大金","2890":"永豐金","2880":"華南金","2892":"第一金","2887":"台新金",
    "2888":"新光金","2801":"彰銀","2823":"中壽","5876":"上海商銀","5871":"中租-KY",
    "2838":"聯邦銀","2820":"華票",
    # 傳產 / 石化
    "6505":"台塑化","1301":"台塑","1303":"南亞","2002":"中鋼","1102":"亞泥",
    "1101":"台泥","2207":"和泰車","2204":"中華","1216":"統一","2912":"統一超",
    "1326":"台化","2105":"正新","1402":"遠東新","9904":"寶成",
    # 電信 / 其他
    "2412":"中華電","4904":"遠傳","3045":"台灣大","2303":"聯電",
    # 航運
    "2603":"長榮","2609":"陽明","2615":"萬海","2610":"華航","2618":"長榮航",
    # 生技
    "6446":"藥華藥","4967":"十銓","6547":"慧景","1476":"儒鴻","1477":"聚陽",
    # 其他電子
    "3702":"大聯大","3673":"TPK-KY","2325":"矽品","6271":"同欣電",
    "2049":"上銀","6176":"瑞儀","3706":"神達","5483":"中美晶",
}
# 整理成乾淨的 dict (去掉 key 打錯的)
STOCK_POOL = {k:v for k,v in STOCK_POOL.items() if re.match(r"^\d{4}$", str(k))}


def _read_token() -> str:
    """讀取 FinMind token，支援多種 secrets.toml 格式"""
    for k in ["finmind_token", "FINMIND_TOKEN", "finmind_api_token"]:
        try:
            v = str(st.secrets.get(k, "")).strip()
            if v and len(v) > 20: return v
        except: pass
    try:
        gcp = st.secrets.get("gcp_service_account", {})
        for k in ["finmind_token", "FINMIND_TOKEN", "finmind_api_token"]:
            v = str(gcp.get(k, "")).strip()
            if v and len(v) > 20: return v
    except: pass
    try:
        v = str(st.secrets.get("finmind", {}).get("token", "")).strip()
        if v and len(v) > 20: return v
    except: pass
    return ""


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_name_map() -> dict:
    """從 FinMind TaiwanStockInfo 取股票名稱對照表，備援用 STOCK_POOL"""
    token = _read_token()
    if token:
        try:
            r   = requests.get("https://api.finmindtrade.com/api/v4/data",
                               params={"dataset": "TaiwanStockInfo", "token": token},
                               timeout=15)
            raw = r.json()
            if int(str(raw.get("status", 0))) == 200 and raw.get("data"):
                df   = pd.DataFrame(raw["data"])
                id_c = next((c for c in df.columns if "stock_id"   in c.lower()), None)
                nm_c = next((c for c in df.columns if "stock_name" in c.lower()), None)
                if id_c and nm_c:
                    api_map = dict(zip(df[id_c].astype(str), df[nm_c].astype(str)))
                    return {**STOCK_POOL, **api_map}
        except Exception:
            pass
    return dict(STOCK_POOL)   # fallback to built-in pool


# CB 可轉債：只用 FinMind（TWSE SSL 被 Streamlit Cloud 封，thefew.tw 無靜態資料）
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_cb_stocks() -> tuple:
    """回傳 (codes_set, source_label)"""
    token = _read_token()
    if not token:
        return set(), "無Token"

    try:
        # FinMind TaiwanStockConvertibleBond — 取目前仍在流通的可轉債
        params = {"dataset": "TaiwanStockConvertibleBond", "token": token}
        r   = requests.get("https://api.finmindtrade.com/api/v4/data",
                           params=params, timeout=15)
        raw = r.json()
        if int(str(raw.get("status", 0))) == 200 and raw.get("data"):
            df   = pd.DataFrame(raw["data"])
            # 欄位中找股票代號欄
            id_c = next((c for c in df.columns
                         if c.lower() in ("stock_id","stockid","stock_code","underlying_stock_id")),
                        None)
            if id_c is None:
                # fallback: 掃所有欄找 4 碼數字
                codes = set()
                for col in df.columns:
                    for v in df[col].astype(str):
                        if re.match(r"^\d{4}$", v.strip()):
                            codes.add(v.strip())
            else:
                codes = {str(v).strip() for v in df[id_c]
                         if re.match(r"^\d{4}$", str(v).strip())}
            if codes:
                return codes, f"FinMind ({len(codes)} 支)"
        else:
            return set(), f"FinMind 回傳 status={raw.get('status')} msg={raw.get('msg','')}"
    except Exception as e:
        return set(), f"FinMind 例外: {e}"

# ─────────────────────────────────────────────────────────────────────────────
# TIME — 台灣時間 UTC+8
# ─────────────────────────────────────────────────────────────────────────────
TW_TZ = timezone(timedelta(hours=8))

def tw_now() -> datetime:
    return datetime.now(tz=TW_TZ).replace(tzinfo=None)

def last_trade_date() -> str:
    """最近盤後資料已完整的交易日"""
    tw = tw_now()
    if tw.hour < 14 or (tw.hour == 14 and tw.minute < 30):
        tw -= timedelta(days=1)
    while tw.weekday() >= 5:
        tw -= timedelta(days=1)
    return tw.strftime("%Y-%m-%d")

def is_market_open() -> bool:
    tw = tw_now()
    if tw.weekday() >= 5: return False
    t = tw.hour * 60 + tw.minute
    return 9 * 60 <= t <= 13 * 60 + 30

# ─────────────────────────────────────────────────────────────────────────────
# yfinance 資料抓取
# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# 成交金額 TOP30：FinMind 為主，yfinance 備援
# ─────────────────────────────────────────────────────────────────────────────
def _fm_stock_price(date_str: str) -> pd.DataFrame:
    """
    FinMind TaiwanStockPrice 取全市場當日成交。
    欄位: stock_id, Trading_money(元), close, spread
    失敗回傳空 DataFrame。
    """
    token = _read_token()
    if not token:
        return pd.DataFrame()
    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockPrice",
                    "date":    date_str,
                    "token":   token},
            timeout=25)
        raw = r.json()
        if int(str(raw.get("status", 0))) == 200 and raw.get("data"):
            df = pd.DataFrame(raw["data"])
            need = {"stock_id", "Trading_money", "close", "spread"}
            if need.issubset(df.columns):
                df["stock_id"]    = df["stock_id"].astype(str).str.strip()
                df["trade_value"] = pd.to_numeric(df["Trading_money"], errors="coerce").fillna(0) / 1e8
                df["close"]       = pd.to_numeric(df["close"],  errors="coerce").fillna(0)
                df["spread"]      = pd.to_numeric(df["spread"], errors="coerce").fillna(0)
                df["prev_close"]  = df["close"] - df["spread"]
                df["change_pct"]  = df.apply(
                    lambda r: round(r["spread"] / r["prev_close"] * 100, 2)
                    if r["prev_close"] > 0 else 0.0, axis=1)
                df = df[df["stock_id"].str.match(r"^\d{4}$") & (df["trade_value"] > 0)]
                return df
    except Exception:
        pass
    return pd.DataFrame()


def _yf_top30(symbols, name_pool, period_kw: dict) -> tuple:
    """yfinance 備援，回傳 (df, label) 或 (empty, err)"""
    try:
        raw = yf.download(tickers=symbols, auto_adjust=True, progress=False,
                          threads=True, **period_kw)
        if raw.empty:
            return pd.DataFrame(), "yfinance 無資料"
        close_df  = raw["Close"].iloc[-1]
        volume_df = raw["Volume"].iloc[-1]
        prev_df   = raw["Close"].iloc[-2] if len(raw) >= 2 else close_df
        rows = []
        for sym in symbols:
            try:
                code  = sym.replace(".TW", "")
                close = float(close_df.get(sym, 0) or 0)
                vol   = float(volume_df.get(sym, 0) or 0)
                prev  = float(prev_df.get(sym, close) or close)
                if close <= 0 or vol <= 0: continue
                tv  = round(close * vol / 1e8, 2)
                chg = round((close - prev) / prev * 100, 2) if prev > 0 and close != prev else 0.0
                rows.append({"code": code, "name": name_pool.get(code, code),
                             "trade_value": tv, "change_pct": chg})
            except: continue
        if not rows:
            return pd.DataFrame(), "yfinance 全部無資料"
        df = (pd.DataFrame(rows).sort_values("trade_value", ascending=False)
              .head(30).reset_index(drop=True))
        df["rank"] = range(1, len(df) + 1)
        return df[["rank", "code", "name", "trade_value", "change_pct"]], "yfinance"
    except Exception as e:
        return pd.DataFrame(), f"yfinance 例外: {e}"


@st.cache_data(ttl=170, show_spinner=False)
def fetch_top30(trade_date: str) -> tuple:
    """盤後成交排行。主：FinMind；備：yfinance。回傳 (df, source)"""
    name_map = fetch_name_map()

    df_fm = _fm_stock_price(trade_date)
    if len(df_fm) >= 10:
        df_fm["name"] = df_fm["stock_id"].map(name_map).fillna(df_fm["stock_id"])
        df_fm = df_fm.sort_values("trade_value", ascending=False).head(30).reset_index(drop=True)
        df_fm["rank"] = range(1, len(df_fm) + 1)
        return (df_fm.rename(columns={"stock_id": "code"})
                [["rank", "code", "name", "trade_value", "change_pct"]],
                "FinMind TaiwanStockPrice")

    symbols = [f"{c}.TW" for c in STOCK_POOL]
    start = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    end   = (datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    df, src = _yf_top30(symbols, STOCK_POOL, {"start": start, "end": end})
    return df, src or "yfinance (備援)"


@st.cache_data(ttl=170, show_spinner=False)
def fetch_realtime_top30() -> tuple:
    """盤中即時排行。主：FinMind；備：yfinance。回傳 (df, source)"""
    name_map   = fetch_name_map()
    trade_date = tw_now().strftime("%Y-%m-%d")

    df_fm = _fm_stock_price(trade_date)
    if len(df_fm) >= 10:
        df_fm["name"] = df_fm["stock_id"].map(name_map).fillna(df_fm["stock_id"])
        df_fm = df_fm.sort_values("trade_value", ascending=False).head(30).reset_index(drop=True)
        df_fm["rank"] = range(1, len(df_fm) + 1)
        return (df_fm.rename(columns={"stock_id": "code"})
                [["rank", "code", "name", "trade_value", "change_pct"]],
                "FinMind 即時")

    symbols = [f"{c}.TW" for c in STOCK_POOL]
    df, src = _yf_top30(symbols, STOCK_POOL, {"period": "2d", "interval": "1d"})
    return df, src or "yfinance 即時(備援)"

# ─────────────────────────────────────────────────────────────────────────────
# 月營收年增率 + 創歷史新高
# 資料來源：FinMind API（免費，不需 token 即可查詢月營收）
# ─────────────────────────────────────────────────────────────────────────────
def _fm_revenue(dataset: str, params: dict) -> list:
    """呼叫 FinMind API，回傳 data list 或空 list"""
    base_params = {"dataset": dataset}
    token = _read_token()
    if token:
        base_params["token"] = token
    base_params.update(params)
    try:
        r   = requests.get("https://api.finmindtrade.com/api/v4/data",
                           params=base_params, timeout=25)
        raw = r.json()
        if int(str(raw.get("status", 0))) == 200:
            return raw.get("data") or []
    except Exception:
        pass
    return []


def _calc_yoy(data: list) -> dict:
    """
    從 FinMind TaiwanStockMonthRevenue data list 計算 YoY 和創高。
    欄位: date(YYYY-MM-DD), stock_id, revenue(千元), revenue_month, revenue_year
    """
    if not data:
        return {}
    try:
        df = pd.DataFrame(data)
        # 確認必要欄位存在
        if "revenue" not in df.columns or "date" not in df.columns:
            return {}
        df["date"]    = pd.to_datetime(df["date"])
        df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0)
        df = df.sort_values("date").reset_index(drop=True)
        if len(df) < 2:
            return {}
        latest_rev = float(df["revenue"].iloc[-1])
        if latest_rev <= 0:
            return {}
        # 去年同月：找 date 距今 12 個月前最近的一筆
        latest_dt  = df["date"].iloc[-1]
        target_dt  = latest_dt - pd.DateOffset(months=12)
        df["_diff"] = (df["date"] - target_dt).abs()
        prev_row   = df.loc[df["_diff"].idxmin()]
        if prev_row["_diff"] > pd.Timedelta(days=45):
            return {}
        prev_rev = float(prev_row["revenue"])
        if prev_rev <= 0:
            return {}
        yoy     = round((latest_rev - prev_rev) / prev_rev * 100, 1)
        # 創歷史新高：最新月 >= 本段資料所有月份最高值
        is_high = bool(latest_rev >= float(df["revenue"].max()))
        return {"yoy": yoy, "is_high": is_high}
    except Exception:
        return {}


@st.cache_data(ttl=7200, show_spinner=False)
def fetch_revenue_yoy(codes_tuple: tuple) -> dict:
    """
    取月營收年增率 YoY(%) 和是否創歷史新高。
    使用 FinMind TaiwanStockMonthRevenue，逐股查詢。
    參數必須是 tuple（list 無法被 st.cache_data hash）。
    """
    result = {}
    token  = _read_token()
    if not token:
        return result   # 無 token 無法查

    start = (datetime.now() - timedelta(days=430)).strftime("%Y-%m-%d")

    for code in codes_tuple[:30]:
        code_str = str(code).strip()
        if not re.match(r"^\d{4}$", code_str):
            continue
        try:
            r = requests.get(
                "https://api.finmindtrade.com/api/v4/data",
                params={"dataset": "TaiwanStockMonthRevenue",
                        "data_id": code_str,
                        "start_date": start,
                        "token": token},
                timeout=15)
            raw = r.json()
            if int(str(raw.get("status", 0))) == 200:
                info = _calc_yoy(raw.get("data") or [])
                if info:
                    result[code_str] = info
        except Exception:
            pass
        time.sleep(0.1)   # 避免超過 FinMind 速率限制

    return result


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
# TABLE — 排行 / 股票名稱 / 漲跌幅 / 成交金額(億)
# ─────────────────────────────────────────────────────────────────────────────
def build_table(df, prev_codes, cb_codes, extra=None, revenue_map=None):
    rows = []
    # 記錄每列的 metadata 供 Styler 使用
    pct_list  = []
    new_list  = []
    cb_list   = []
    yoy_list  = []
    high_list = []

    for _, r in df.iterrows():
        code   = str(r["code"]).strip()
        pct    = float(r.get("change_pct") or 0)
        is_new = bool(prev_codes) and (code not in prev_codes)
        has_cb = code in cb_codes
        # ★ 前綴表示新上榜，CB 後綴表示可轉債
        base_name = str(r["name"])
        prefix    = "★ " if is_new else ""
        suffix    = "  CB" if has_cb else ""
        name      = prefix + base_name + suffix

        # 漲跌幅：只有非零才顯示，避免 yfinance 前後日相同導致誤判為 0
        if abs(pct) >= 0.01:
            pstr = f"▲ {pct:.2f}%" if pct > 0 else f"▼ {abs(pct):.2f}%"
        else:
            pstr = "─"

        # 月營收年增率
        rev_info  = (revenue_map or {}).get(code, {})
        yoy       = rev_info.get("yoy", None)
        is_high   = rev_info.get("is_high", False)
        if yoy is not None:
            rev_str = ("🔺創高 " if is_high else "") + f"{yoy:+.1f}%"
        else:
            rev_str = "—"

        row_d = {
            "排行":        int(r.get("rank", _+1)),
            "股票名稱":    name,
            "漲跌幅":      pstr,
            "成交金額(億)": float(r.get("trade_value", 0)),
            "月營收YoY":   rev_str,
        }
        # 記錄 yoy 數值供 Styler 著色
        yoy_list.append(yoy if yoy is not None else 0)
        high_list.append(is_high)
        if extra:
            for s, d2 in extra: row_d[d2] = r.get(s, "")

        rows.append(row_d)
        pct_list.append(pct)
        new_list.append(is_new)
        cb_list.append(has_cb)

    # 只建立「顯示欄位」的 DataFrame，不放 __p/__n
    vis_cols = ["排行", "股票名稱", "漲跌幅", "成交金額(億)", "月營收YoY"]
    if extra:
        vis_cols += [d2 for _, d2 in extra]

    disp = pd.DataFrame(rows)[vis_cols]

    # Styler apply 函式：必須回傳完整 CSS 屬性字串
    def cpct(col):
        result = []
        for v in pct_list:
            if v > 0.01:    result.append("color: #e74c3c; font-weight: 600")
            elif v < -0.01: result.append("color: #2ecc71; font-weight: 600")
            else:           result.append("color: #5a6a80")
        return result

    def cname(col):
        result = []
        for i in range(len(disp)):
            is_n = new_list[i]
            cb   = cb_list[i]
            if is_n:  result.append("color: #f39c12; font-weight: 700")
            elif cb:  result.append("color: #a78bfa")
            else:     result.append("color: #c8d6e5")
        return result

    def crank(col):
        return ["color: #4fc3f7; font-weight: 700" if v <= 3 else "color: #4a6080"
                for v in disp["排行"]]

    fmt = {"成交金額(億)": "{:,.1f}"}
    if extra:
        for _, d2 in extra:
            if "億" in d2: fmt[d2] = "{:,.1f}"

    def crev(col):
        """月營收YoY 著色：正=橘紅，負=灰，創高=金色"""
        result = []
        for i in range(len(disp)):
            if high_list[i]:
                result.append("color: #f7b731; font-weight: 700")
            elif yoy_list[i] > 0:
                result.append("color: #e67e22")
            elif yoy_list[i] < 0:
                result.append("color: #7f8c8d")
            else:
                result.append("color: #5a6a80")
        return result

    return (
        disp.style
        .apply(cpct,  subset=["漲跌幅"])
        .apply(cname, subset=["股票名稱"])
        .apply(crank, subset=["排行"])
        .apply(crev,  subset=["月營收YoY"])
        .format(fmt)
        .set_properties(**{"font-family": "IBM Plex Mono, monospace",
                           "font-size": "13px", "border": "none"})
        .set_properties(subset=["排行"],                   **{"text-align": "center"})
        .set_properties(subset=["股票名稱"],                **{"text-align": "left"})
        .set_properties(subset=["漲跌幅", "成交金額(億)", "月營收YoY"], **{"text-align": "right"})
        .set_table_styles([
            {"selector": "thead th", "props": [
                ("background-color", "#0a1520"), ("color", "#4a6080"),
                ("font-family", "IBM Plex Mono, monospace"), ("font-size", "11px"),
                ("letter-spacing", "1.5px"), ("text-transform", "uppercase"),
                ("border-bottom", "1px solid #1a2940"), ("padding", "10px 14px"),
            ]},
            {"selector": "tbody td", "props": [
                ("padding", "10px 14px"), ("border-bottom", "1px solid #0d1a28"),
            ]},
            {"selector": "tbody tr:hover td", "props": [
                ("filter", "brightness(1.3)"),
            ]},
            {"selector": "table", "props": [
                ("width", "100%"), ("border-collapse", "collapse"),
            ]},
        ])
        .hide(axis="index")
    )

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

def _prep_hist(raw):
    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    rm = {}
    for c in df.columns:
        if c in ("代號","code","stock_id"):        rm[c]="code"
        elif c in ("名稱","name","stock_name"):     rm[c]="name"
        elif "成交金額" in c or c=="trade_value":   rm[c]="trade_value"
        elif "漲跌幅"   in c or c=="change_pct":   rm[c]="change_pct"
    df=df.rename(columns=rm)
    for c in ["code","name","trade_value","change_pct"]:
        if c not in df.columns: df[c]="" if c in ["code","name"] else 0.0
    df["trade_value"]=pd.to_numeric(df["trade_value"],errors="coerce").fillna(0)
    df["change_pct"] =pd.to_numeric(df["change_pct"], errors="coerce").fillna(0)
    df["rank"]=range(1,len(df)+1)
    return df

# ─────────────────────────────────────────────────────────────────────────────
# PAGE 1 – 即時排行
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
             <div class="topbar-sub">TWSE · DAILY VOLUME LEADERS · yfinance</div></div>
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
        st.markdown(f"**台灣時間** `{tw.strftime('%H:%M:%S')}`")
        st.markdown(f"**查詢日期** `{trade_d}`")
        st.caption("資料來源: yfinance\n每 3 分鐘自動刷新")

    with st.spinner("載入成交資料…"):
        if open_:
            df, err = fetch_realtime_top30()
        else:
            df, err = fetch_top30(trade_d)

    if df is None or len(df) == 0:
        st.error(f"❌ 資料載入失敗: {err}")
        st.info("yfinance 偶爾會有延遲，請稍後點「立即刷新」重試。")
        return

    cb_codes, _cb_src = fetch_cb_stocks()
    client     = gs_client()
    prev_codes = load_prev_codes(client, today_k)

    if tw.hour >= 14:
        save_today(client, df, today_k)

    # sidebar 診斷資訊
    with st.sidebar:
        st.markdown("---")
        st.caption(f"**CB資料來源:** {_cb_src}  \n**CB上榜數:** {sum(1 for c in df['code'] if c in cb_codes)}")

    with st.spinner("載入月營收資料…"):
        rev_map = fetch_revenue_yoy(tuple(df["code"].astype(str).tolist()))
    # 顯示月營收診斷
    with st.sidebar:
        token_ok = bool(_read_token())
        rev_ok   = len(rev_map)
        st.caption(f"**FinMind Token:** {'✅ 讀到' if token_ok else '❌ 未讀到'}  \n**月營收筆數:** {rev_ok}/30")

    render_kpi(df, prev_codes, cb_codes)
    render_legend()
    st.markdown(
        f'<div class="refresh-info">資料日期: {trade_d} &nbsp;·&nbsp; '
        f'更新: {tw.strftime("%H:%M:%S")} &nbsp;·&nbsp; 每 3 分鐘自動刷新</div>',
        unsafe_allow_html=True)

    st.dataframe(build_table(df, prev_codes, cb_codes, revenue_map=rev_map),
                 use_container_width=True, height=980, hide_index=True)

    if not open_:
        st.info(f"📌 非交易時段，顯示 {trade_d} 收盤資料。")

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
    cb_codes, _cb_src = fetch_cb_stocks()

    if not history:
        st.warning("尚無歷史資料。請確認 Google Sheets API 已設定，且系統曾在交易日 14:00 後儲存資料。")
        return

    dates      = sorted(history.keys(), reverse=True)
    all_sorted = sorted(history.keys())
    c1, c2    = st.columns([3,1])
    with c1: selected = st.multiselect("選擇日期（可多選）", dates,
                                        default=dates[:7] if len(dates)>=7 else dates)
    with c2: mode = st.radio("模式", ["每日明細","彙總排行"], horizontal=True)

    if not selected:
        st.info("請選擇日期"); return

    if mode == "每日明細":
        for date in sorted(selected, reverse=True):
            raw = history.get(date)
            if raw is None or len(raw)==0: continue
            df     = _prep_hist(raw)
            prev_c = set()
            idx    = all_sorted.index(date) if date in all_sorted else -1
            if idx > 0:
                try: prev_c=set(_prep_hist(history[all_sorted[idx-1]])["code"].astype(str))
                except: pass
            with st.expander(f"📅 {date}", expanded=(date==sorted(selected,reverse=True)[0])):
                render_kpi(df, prev_c, cb_codes)
                render_legend()
                rev_map = fetch_revenue_yoy(tuple(df["code"].astype(str).tolist()))
                st.dataframe(build_table(df, prev_c, cb_codes, revenue_map=rev_map),
                             use_container_width=True, height=600, hide_index=True)

    else:
        all_rows=[]
        for d in selected:
            raw=history.get(d)
            if raw is None: continue
            tmp=_prep_hist(raw); tmp["_d"]=d; all_rows.append(tmp)
        if not all_rows: st.warning("無有效資料"); return

        combined=pd.concat(all_rows,ignore_index=True)
        agg=(combined.groupby(["code","name"])
             .agg(avg_val=("trade_value","mean"),total_val=("trade_value","sum"),
                  days=("trade_value","count"),  avg_pct=("change_pct","mean"))
             .reset_index().sort_values("total_val",ascending=False).head(30).reset_index(drop=True))
        agg["rank"]=range(1,len(agg)+1)
        agg["trade_value"]=agg["avg_val"]
        agg["change_pct"] =agg["avg_pct"].round(2)

        sel_s=sorted(selected); prev_c=set()
        if sel_s[0] in all_sorted:
            i=all_sorted.index(sel_s[0])
            if i>0:
                try: prev_c=set(_prep_hist(history[all_sorted[i-1]])["code"].astype(str))
                except: pass

        period=f"{sel_s[0]} ~ {sel_s[-1]}" if len(sel_s)>1 else sel_s[0]
        st.markdown(f'<div class="section-title">彙總 · {period} · {len(sel_s)} 交易日</div>',
                    unsafe_allow_html=True)
        render_legend()
        extra=[("avg_val","平均成交(億)"),("total_val","累積成交(億)"),("days","上榜天數")]
        rev_map = fetch_revenue_yoy(tuple(agg["code"].astype(str).tolist()))
        st.dataframe(build_table(agg, prev_c, cb_codes, extra=extra, revenue_map=rev_map),
                     use_container_width=True, height=700, hide_index=True)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def page_diag():
    """API 診斷頁面 — 直接顯示原始 API 回傳結果"""
    tw = tw_now()
    st.markdown("## 🔧 API 診斷")
    st.caption(f"台灣時間: {tw.strftime('%Y-%m-%d %H:%M:%S')} | 查詢交易日: {last_trade_date()}")

    token = _read_token()
    st.info(f"**FinMind Token 狀態:** {'✅ 讀到 (長度 ' + str(len(token)) + ')' if token else '❌ 未讀到 — 請確認 secrets.toml 格式'}")
    if token:
        st.code(f"Token 前 30 碼: {token[:30]}...", language=None)

    if not st.button("▶ 執行診斷（點擊開始）", type="primary"):
        st.info("點擊按鈕開始測試所有 API")
        return

    # ── 1. FinMind Token 驗證 ──
    st.markdown("### 1. FinMind Token 驗證")
    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/user_info",
            params={"token": token} if token else {},
            timeout=10)
        d = r.json()
        st.write(f"HTTP: {r.status_code}")
        st.json(d)
    except Exception as e:
        st.error(f"Exception: {e}")

    # ── 2. FinMind 月營收（台積電 2330）──
    st.markdown("### 2. FinMind TaiwanStockMonthRevenue (2330)")
    start = (tw - timedelta(days=430)).strftime("%Y-%m-%d")
    try:
        params = {"dataset":"TaiwanStockMonthRevenue","data_id":"2330","start_date":start}
        if token: params["token"] = token
        r = requests.get("https://api.finmindtrade.com/api/v4/data", params=params, timeout=15)
        d = r.json()
        st.write(f"HTTP: {r.status_code} | status: {d.get('status')} | msg: {d.get('msg','')}")
        data = d.get("data",[])
        st.write(f"筆數: {len(data)}")
        if data:
            df_tmp = pd.DataFrame(data)
            st.write(f"欄位: {list(df_tmp.columns)}")
            st.dataframe(df_tmp.tail(5))
        else:
            st.error(f"無資料，完整回傳: {str(d)[:300]}")
    except Exception as e:
        st.error(f"Exception: {e}")

    # ── 3. TWSE CB_OVERVIEW ──
    st.markdown("### 3. TWSE CB_OVERVIEW")
    try:
        r = requests.get(
            "https://www.twse.com.tw/rwd/zh/cbInfo/CB_OVERVIEW",
            headers={"User-Agent":"Mozilla/5.0","Referer":"https://www.twse.com.tw/"},
            timeout=15)
        d = r.json()
        st.write(f"HTTP: {r.status_code} | stat: {d.get('stat')} | fields: {d.get('fields',[])} ")
        data = d.get("data",[])
        st.write(f"筆數: {len(data)}")
        if data:
            st.write(f"第一筆: {data[0]}")
        else:
            st.error(f"無資料: {str(d)[:300]}")
    except Exception as e:
        st.error(f"Exception: {e}")

    # ── 4. TWSE CB_BOND_INFO ──
    st.markdown("### 4. TWSE CB_BOND_INFO")
    try:
        r = requests.get(
            "https://www.twse.com.tw/rwd/zh/cbInfo/CB_BOND_INFO",
            headers={"User-Agent":"Mozilla/5.0","Referer":"https://www.twse.com.tw/"},
            timeout=15)
        d = r.json()
        st.write(f"HTTP: {r.status_code} | stat: {d.get('stat')}")
        data = d.get("data",[])
        st.write(f"筆數: {len(data)}")
        if data: st.write(f"第一筆: {data[0]}")
        else: st.error(str(d)[:300])
    except Exception as e:
        st.error(f"Exception: {e}")

    # ── 5. thefew.tw/cb ──
    st.markdown("### 5. thefew.tw/cb")
    try:
        r = requests.get("https://thefew.tw/cb",
            headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
                     "Accept":"text/html,application/xhtml+xml;q=0.9,*/*;q=0.8"},
            timeout=15)
        st.write(f"HTTP: {r.status_code} | Content-Type: {r.headers.get('content-type','?')} | 長度: {len(r.text)}")
        # 找 __NEXT_DATA__
        import json as _j
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "html.parser")
        nd = soup.find("script", {"id":"__NEXT_DATA__"})
        if nd:
            data = _j.loads(nd.string)
            st.success(f"✅ 找到 __NEXT_DATA__，keys: {list(data.keys())[:5]}")
        else:
            st.warning("❌ 未找到 __NEXT_DATA__")
            st.code(r.text[:500], language="html")
    except Exception as e:
        st.error(f"Exception: {e}")

    # ── 6. yfinance 台積電測試 ──
    st.markdown("### 6. yfinance 2330.TW")
    try:
        import yfinance as yf
        t = yf.Ticker("2330.TW")
        info = t.fast_info
        st.success(f"✅ lastPrice={info.last_price:.2f}, shares={info.shares:,.0f}")
    except Exception as e:
        st.error(f"Exception: {e}")

    st.success("診斷完成！請將以上結果截圖。")


def main():
    page = st.radio("nav", ["📈  即時排行","📊  歷史排行","🔧  診斷"],
                    horizontal=True, label_visibility="collapsed")
    if "即時" in page:   page_realtime()
    elif "歷史" in page: page_history()
    else:                page_diag()

if __name__ == "__main__":
    main()
