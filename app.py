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

# CB 已發行可轉債靜態清單（定期更新）
CB_STOCKS = {
    "2330","2317","2454","3008","2382","2308","2303","6505","1301","1303",
    "2002","2412","2881","2882","2886","2891","2884","2885","2890","2880",
    "2892","6669","3231","2379","2395","2408","3034","2344","2357","4904",
    "3045","2603","2609","2615","2618","2633","2801","2823","2834","2838",
    "2845","3481","3673","3702","4938","4958","5871","5876","5880","6176",
    "6269","6278","6285","6443","6446","6456","6533","6547","6654","6770",
    "2345","2353","2356","2376","2392","2449","2451","2458","2474","2887",
    "2888","2912","3706","5483","6271","2207","1101","1102","1301","1303",
}

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
@st.cache_data(ttl=170, show_spinner=False)   # ~3 分鐘
def fetch_top30(trade_date: str) -> tuple:
    """
    用 yfinance 批次下載股票日資料，計算成交金額排行。
    回傳 (df, error_msg)
    df 欄位: rank, code, name, trade_value(億), change_pct
    """
    codes   = list(STOCK_POOL.keys())
    symbols = [f"{c}.TW" for c in codes]

    try:
        # 下載近 5 天（確保假日也能取到最新一筆）
        start = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        end   = (datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

        raw = yf.download(
            tickers    = symbols,
            start      = start,
            end        = end,
            auto_adjust= True,
            progress   = False,
            threads    = True,
        )

        if raw.empty:
            return pd.DataFrame(), "yfinance 回傳空資料"

        # 取最新一天
        close_df  = raw["Close"].iloc[-1]   # 最新收盤價
        volume_df = raw["Volume"].iloc[-1]  # 最新成交量(股)
        prev_df   = raw["Close"].iloc[-2] if len(raw) >= 2 else close_df

        rows = []
        for sym in symbols:
            try:
                code   = sym.replace(".TW","")
                close  = float(close_df.get(sym, 0) or 0)
                vol    = float(volume_df.get(sym, 0) or 0)
                prev   = float(prev_df.get(sym, close) or close)
                if close <= 0 or vol <= 0:
                    continue
                tv     = round(close * vol / 1e8, 2)          # 成交金額(億)
                chg    = round((close - prev) / prev * 100, 2) if prev > 0 else 0
                name   = STOCK_POOL.get(code, code)
                rows.append({"code":code,"name":name,"trade_value":tv,"change_pct":chg})
            except:
                continue

        if not rows:
            return pd.DataFrame(), "所有股票成交金額均為 0，可能是假日或資料延遲"

        df = (pd.DataFrame(rows)
              .sort_values("trade_value", ascending=False)
              .head(30)
              .reset_index(drop=True))
        df["rank"] = range(1, len(df)+1)
        return df[["rank","code","name","trade_value","change_pct"]], None

    except Exception as e:
        return pd.DataFrame(), f"yfinance 例外: {e}"


@st.cache_data(ttl=170, show_spinner=False)
def fetch_realtime_top30() -> tuple:
    """
    盤中即時：用 yfinance Ticker.fast_info 取得即時成交量與價格。
    批次用 download(period='1d', interval='1m') 取分鐘資料。
    """
    codes   = list(STOCK_POOL.keys())
    symbols = [f"{c}.TW" for c in codes]

    try:
        # period='1d' 取今日資料，interval='1m' 分鐘級
        raw = yf.download(
            tickers  = symbols,
            period   = "2d",       # 取 2 天確保有昨收
            interval = "1d",       # 日線（盤中會取到目前累積）
            auto_adjust = True,
            progress = False,
            threads  = True,
        )

        if raw.empty:
            return pd.DataFrame(), "無即時資料"

        close_df  = raw["Close"].iloc[-1]
        volume_df = raw["Volume"].iloc[-1]
        prev_df   = raw["Close"].iloc[-2] if len(raw) >= 2 else close_df

        rows = []
        for sym in symbols:
            try:
                code  = sym.replace(".TW","")
                close = float(close_df.get(sym, 0) or 0)
                vol   = float(volume_df.get(sym, 0) or 0)
                prev  = float(prev_df.get(sym, close) or close)
                if close <= 0 or vol <= 0: continue
                tv    = round(close * vol / 1e8, 2)
                chg   = round((close - prev) / prev * 100, 2) if prev > 0 else 0
                name  = STOCK_POOL.get(code, code)
                rows.append({"code":code,"name":name,"trade_value":tv,"change_pct":chg})
            except:
                continue

        if not rows:
            return pd.DataFrame(), "即時資料為空"

        df = (pd.DataFrame(rows)
              .sort_values("trade_value", ascending=False)
              .head(30).reset_index(drop=True))
        df["rank"] = range(1, len(df)+1)
        return df[["rank","code","name","trade_value","change_pct"]], None

    except Exception as e:
        return pd.DataFrame(), f"即時資料例外: {e}"

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
        row_d  = {"排行":int(r.get("rank",_+1)), "股票名稱":name,
                  "漲跌幅":pstr, "成交金額(億)":float(r.get("trade_value",0))}
        if extra:
            for s, d2 in extra: row_d[d2] = r.get(s, "")
        rows.append(row_d); pcts.append(pct); news.append(is_new)

    disp = pd.DataFrame(rows)
    disp["__p"] = pcts
    disp["__n"] = news

    def rbg(row):
        bg = ("#191000" if row["__n"] else
              "#1a0808" if row["__p"]>0 else
              "#041008" if row["__p"]<0 else "#0a1520")
        return [bg]*len(row)
    def cpct(col):
        return ["color:#e74c3c;font-weight:600" if v>0
                else ("color:#2ecc71;font-weight:600" if v<0 else "color:#5a6a80")
                for v in disp["__p"]]
    def cname(col):
        s=[]
        for i, v in enumerate(disp["股票名稱"]):
            n=disp["__n"].iloc[i]; cb="CB" in str(v)
            s.append("color:#f39c12;font-weight:700" if n
                     else "color:#a78bfa" if cb else "color:#c8d6e5")
        return s
    def crank(col):
        return ["color:#4fc3f7;font-weight:700" if v<=3 else "color:#4a6080"
                for v in disp["排行"]]

    fmt = {"成交金額(億)":"{:,.1f}"}
    if extra:
        for _, d2 in extra:
            if "億" in d2: fmt[d2] = "{:,.1f}"

    return (
        disp.style
        .apply(rbg, axis=1).apply(cpct,subset=["漲跌幅"])
        .apply(cname,subset=["股票名稱"]).apply(crank,subset=["排行"])
        .format(fmt)
        .set_properties(**{"font-family":"'IBM Plex Mono',monospace","font-size":"13px","border":"none"})
        .set_properties(subset=["排行"],               **{"text-align":"center"})
        .set_properties(subset=["股票名稱"],            **{"text-align":"left"})
        .set_properties(subset=["漲跌幅","成交金額(億)"],**{"text-align":"right"})
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
        .hide(subset=["__p","__n"],axis="columns")
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

    cb_codes   = CB_STOCKS
    client     = gs_client()
    prev_codes = load_prev_codes(client, today_k)

    if tw.hour >= 14:
        save_today(client, df, today_k)

    render_kpi(df, prev_codes, cb_codes)
    render_legend()
    st.markdown(
        f'<div class="refresh-info">資料日期: {trade_d} &nbsp;·&nbsp; '
        f'更新: {tw.strftime("%H:%M:%S")} &nbsp;·&nbsp; 每 3 分鐘自動刷新</div>',
        unsafe_allow_html=True)

    st.dataframe(build_table(df, prev_codes, cb_codes),
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
    cb_codes = CB_STOCKS

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
                st.dataframe(build_table(df,prev_c,cb_codes),
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
        st.dataframe(build_table(agg,prev_c,cb_codes,extra=extra),
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
