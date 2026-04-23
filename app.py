"""
台股成交金額 TOP 30 追蹤系統
資料來源：
  - 盤後成交：https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL  (每日全部股票)
  - 盤中即時：https://mis.twse.com.tw/stock/api/getStockInfo.jsp
  - CB 可轉債：https://www.twse.com.tw/rwd/zh/cbInfo/CB_OVERVIEW  (公開資訊觀測站)
  - 歷史儲存：Google Sheets
"""

import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import random

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

.kpi-strip { display:grid; grid-template-columns:repeat(5,1fr); gap:10px; margin-bottom:20px; }
.kpi { background:#0a1520; border:1px solid #1a2940; border-radius:4px; padding:12px 16px; }
.kpi-label { font-size:10px; color:#4a6080; letter-spacing:1.5px; text-transform:uppercase; margin-bottom:6px; }
.kpi-value { font-family:'IBM Plex Mono',monospace; font-size:22px; font-weight:600; }
.kpi-value.up   { color:#e74c3c; }
.kpi-value.dn   { color:#2ecc71; }
.kpi-value.neu  { color:#4fc3f7; }
.kpi-value.gold { color:#f39c12; }

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
div[data-testid="stRadio"] label[data-checked="true"] {
    background:#1a2940 !important; color:#4fc3f7 !important; }

/* hide dataframe index and extra scrollbar */
[data-testid="stDataFrame"] { border:1px solid #1a2940 !important; border-radius:4px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
GOOGLE_SHEETS_ID = "1lTRxbT9iv3uRIrQ1wUFPV0LZbo8p5Qoh4a1FMGa-iU4"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.twse.com.tw/",
    "Accept": "application/json, text/plain, */*",
}

DEMO_STOCKS = [
    ("2330","台積電"),("2317","鴻海"),("2454","聯發科"),("3008","大立光"),
    ("2382","廣達"),("2308","台達電"),("2303","聯電"),("6505","台塑化"),
    ("1301","台塑"),("2002","中鋼"),("2412","中華電"),("2881","富邦金"),
    ("2882","國泰金"),("2886","兆豐金"),("2891","中信金"),("2884","玉山金"),
    ("2885","元大金"),("2890","永豐金"),("2880","華南金"),("2892","第一金"),
    ("6669","緯穎"),("3231","緯創"),("2379","瑞昱"),("2395","研華"),
    ("2408","南亞科"),("3034","聯詠"),("2344","華邦電"),("2357","華碩"),
    ("4904","遠傳"),("3045","台灣大"),
]

# ─────────────────────────────────────────────────────────────────────────────
# CB DATA  – 從 TWSE 公開資訊觀測站爬取已發行可轉債股票代號
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)   # cache 1 hour
def fetch_cb_stocks() -> set:
    """
    爬取目前仍在流通的可轉債，取出對應的股票代號。
    來源：TWSE CB_OVERVIEW API (公開資訊觀測站)
    """
    codes = set()
    try:
        # 方法1: TWSE CB overview
        url = "https://www.twse.com.tw/rwd/zh/cbInfo/CB_OVERVIEW"
        r = requests.get(url, headers=HEADERS, timeout=12)
        data = r.json()
        if data.get("stat") == "OK":
            for row in data.get("data", []):
                # row[0] = CB 代號 (e.g. 33401), row[1] or row[2] = 股票代號
                # 通常 CB 代號前4碼 = 股票代號，或欄位中有股票代號
                try:
                    # 嘗試從 CB 代號取股票代號（去掉末位字母/數字）
                    cb_code = str(row[0]).strip()
                    stock_code = cb_code[:4]  # CB代號前4碼通常是股票代號
                    codes.add(stock_code)
                    # 若有額外欄位也嘗試取
                    if len(row) > 2:
                        sc = str(row[2]).strip()
                        if sc.isdigit() and len(sc) == 4:
                            codes.add(sc)
                except Exception:
                    pass
            if codes:
                return codes
    except Exception:
        pass

    try:
        # 方法2: 公開資訊觀測站 可轉債查詢
        url2 = "https://mops.twse.com.tw/mops/web/ajax_t100sb01"
        payload = {
            "encodeURIComponent": "1",
            "step": "1",
            "firstin": "1",
            "off": "1",
            "keyword4": "",
            "code1": "",
            "TYPEK2": "",
            "checkbtn": "",
            "queryName": "co_id",
            "inpuType": "co_id",
            "TYPEK": "all",
            "isnew": "false",
            "co_id": "",
        }
        r2 = requests.post(url2, data=payload, headers={**HEADERS, "Referer": "https://mops.twse.com.tw/"}, timeout=12)
        # 用 pandas 解析 HTML 表格
        tables = pd.read_html(r2.text, encoding="utf-8")
        for tbl in tables:
            for col in tbl.columns:
                col_data = tbl[col].astype(str)
                for val in col_data:
                    val = val.strip()
                    if val.isdigit() and len(val) == 4:
                        codes.add(val)
        if codes:
            return codes
    except Exception:
        pass

    # Fallback: 回傳空集合（不用靜態清單，讓 CB 欄位顯示為空）
    return set()


# ─────────────────────────────────────────────────────────────────────────────
# TWSE DATA  – 正確端點說明
#
#  盤後（今日收盤後）：
#    STOCK_DAY_ALL  → 全市場當日所有股票成交資料，含成交金額
#    網址：https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL
#    無需日期參數，抓最新一個交易日
#
#  盤中即時：
#    mis.twse.com.tw/stock/api/getStockInfo.jsp
#    需要個別股票代號
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def fetch_twse_top30_aftermarket() -> pd.DataFrame:
    """
    盤後資料：抓 STOCK_DAY_ALL 取得所有股票成交，排序取 TOP30。
    回傳欄位：rank, code, name, trade_value(億), change_pct
    """
    try:
        url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL"
        r = requests.get(url, headers=HEADERS, timeout=15)
        data = r.json()

        if data.get("stat") == "OK" and data.get("data"):
            # 欄位：證券代號、證券名稱、成交股數、成交筆數、成交金額、開盤價、最高價、最低價、收盤價、漲跌(+/-)、漲跌價差、本益比
            rows = data["data"]
            df = pd.DataFrame(rows)

            # 動態取欄位（依 TWSE 文件）
            # col0=代號 col1=名稱 col2=成交股數 col3=成交筆數 col4=成交金額 col5=開盤 col6=最高 col7=最低 col8=收盤 col9=漲跌符號 col10=漲跌價差
            df.columns = ["code","name","vol","txn","trade_value_raw",
                          "open","high","low","close","sign","change_raw"] + \
                         list(df.columns[11:]) if len(df.columns) > 11 else \
                         ["code","name","vol","txn","trade_value_raw",
                          "open","high","low","close","sign","change_raw"]

            # 清理數值
            def clean_num(s):
                return pd.to_numeric(str(s).replace(",","").replace("+","").replace("--","0").strip(),
                                     errors="coerce")

            df["trade_value"] = df["trade_value_raw"].apply(clean_num) / 1e8
            df["close"]       = df["close"].apply(clean_num)
            df["change_raw"]  = df.apply(
                lambda r: clean_num(str(r.get("sign","")) + str(r.get("change_raw","0")).replace(",","")),
                axis=1)
            df["prev_close"]  = df["close"] - df["change_raw"]
            df["change_pct"]  = (df["change_raw"] / df["prev_close"] * 100).round(2)

            # 只保留上市一般股（代號4碼數字）
            df = df[df["code"].str.match(r"^\d{4}$")]
            df = df[df["trade_value"] > 0]
            df = df.sort_values("trade_value", ascending=False).head(30).reset_index(drop=True)
            df["rank"] = range(1, len(df)+1)

            return df[["rank","code","name","trade_value","change_pct"]]

    except Exception as e:
        st.sidebar.caption(f"⚠️ 盤後API錯誤: {e}")

    return _demo_df()


@st.cache_data(ttl=25, show_spinner=False)
def fetch_twse_realtime_batch(codes: list) -> dict:
    """
    批次抓即時行情。
    mis.twse.com.tw 每次最多約 50 支，分批請求。
    回傳 {code: change_pct}
    """
    result = {}
    batch_size = 40

    for i in range(0, len(codes), batch_size):
        batch = codes[i:i+batch_size]
        try:
            query = "|".join(f"tse_{c}.tw" for c in batch)
            url = (f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
                   f"?ex_ch={query}&json=1&delay=0")
            rt_headers = {**HEADERS, "Referer": "https://mis.twse.com.tw/"}
            r = requests.get(url, headers=rt_headers, timeout=8)
            for item in r.json().get("msgArray", []):
                code = item.get("c", "")
                try:
                    # z = 成交價, y = 昨收
                    z = item.get("z", "")  # 當前成交
                    y = item.get("y", "")  # 昨收
                    if not z or z == "-": z = item.get("o", "0")  # fallback 開盤
                    z = float(z.replace(",","") if z else 0)
                    y = float(y.replace(",","") if y else 0)
                    if y > 0:
                        result[code] = round((z - y) / y * 100, 2)
                except Exception:
                    pass
        except Exception:
            pass
        time.sleep(0.1)  # 避免被擋

    return result


def _demo_df() -> pd.DataFrame:
    """當 API 不通時的 demo 資料（測試用）"""
    rng = random.Random(int(datetime.now().strftime("%Y%m%d%H")))
    rows = []
    for i, (code, name) in enumerate(DEMO_STOCKS):
        val = round(rng.uniform(10, 900), 1)
        pct = round(rng.uniform(-6, 6), 2)
        rows.append({"rank":i+1,"code":code,"name":name,
                     "trade_value":val,"change_pct":pct})
    df = pd.DataFrame(rows).sort_values("trade_value", ascending=False).reset_index(drop=True)
    df["rank"] = range(1, len(df)+1)
    st.sidebar.warning("⚠️ 使用示範資料（API 連線失敗或非交易時間）")
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
        st.sidebar.caption(f"ℹ️ Google Sheets 未設定: {e}")
    return None


def save_today(client, df: pd.DataFrame, date_key: str):
    if not client: return
    try:
        import gspread
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        try:
            ws = sh.worksheet(date_key)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=date_key, rows=35, cols=5)

        out = df[["code","name","trade_value","change_pct"]].copy()
        out.columns = ["代號","名稱","成交金額(億)","漲跌幅(%)"]
        # 四捨五入避免浮點
        out["成交金額(億)"] = out["成交金額(億)"].round(2)
        out["漲跌幅(%)"]   = out["漲跌幅(%)"].round(2)
        ws.update([out.columns.tolist()] + out.values.tolist())
    except Exception as e:
        st.sidebar.caption(f"⚠️ Sheets 寫入失敗: {e}")


def load_prev_codes(client, today_key: str) -> set:
    """載入前一個有效交易日的股票代號集合"""
    if not client: return set()
    try:
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        # 只取格式符合 YYYY-MM-DD 的 sheet（排除 工作表1 等預設頁）
        valid_titles = sorted(
            ws.title for ws in sh.worksheets()
            if len(ws.title) == 10 and ws.title[4] == "-" and ws.title < today_key
        )
        if not valid_titles:
            return set()
        records = sh.worksheet(valid_titles[-1]).get_all_records()
        return {str(r.get("代號","")).strip() for r in records if r.get("代號")}
    except Exception:
        return set()


def load_history(client) -> dict:
    """載入所有歷史 sheets（只取 YYYY-MM-DD 格式的 tab）"""
    if not client: return {}
    out = {}
    try:
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        valid_ws = [
            ws for ws in sh.worksheets()
            if len(ws.title) == 10 and ws.title[4] == "-"
        ]
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
# TABLE BUILDER  – Pandas Styler，只顯示 排行/股票名稱/漲跌幅/成交金額
# ─────────────────────────────────────────────────────────────────────────────
def build_and_style(df: pd.DataFrame, prev_codes: set, cb_codes: set,
                    extra_cols: list = None) -> object:
    """
    df 必須有欄位: rank, code, name, trade_value, change_pct
    prev_codes: 前日代號集合，不在其中 → 新上榜
    cb_codes  : 已發行可轉債代號集合
    extra_cols: 歷史彙總額外欄位 list of (col_name, display_name)
    """
    rows = []
    for _, r in df.iterrows():
        code   = str(r["code"]).strip()
        pct    = float(r.get("change_pct") or 0)
        is_new = bool(prev_codes) and (code not in prev_codes)
        has_cb = code in cb_codes

        # 名稱 + inline 標記
        name = str(r["name"])
        tags = []
        if is_new: tags.append("★新")
        if has_cb: tags.append("CB")
        name_cell = name + ("  " + "  ".join(tags) if tags else "")

        # 漲跌幅
        if pct > 0:   pct_str = f"▲ {pct:.2f}%"
        elif pct < 0: pct_str = f"▼ {abs(pct):.2f}%"
        else:         pct_str = "─"

        row = {
            "排行":     int(r.get("rank", _+1)),
            "股票名稱": name_cell,
            "漲跌幅":   pct_str,
            "成交金額(億)": float(r.get("trade_value", 0)),
            "_pct":    pct,
            "_new":    is_new,
            "_cb":     has_cb,
        }
        # 歷史彙總額外欄位
        if extra_cols:
            for src, dst in extra_cols:
                row[dst] = r.get(src, "")
        rows.append(row)

    disp = pd.DataFrame(rows)

    # ── 欄位順序 ──
    vis_cols = ["排行","股票名稱","漲跌幅","成交金額(億)"]
    if extra_cols:
        vis_cols += [dst for _, dst in extra_cols]

    # ── Styler ──
    def row_bg(row):
        if row["_new"]: bg = "#191000"
        elif row["_pct"] > 0: bg = "#1a0808"
        elif row["_pct"] < 0: bg = "#051208"
        else: bg = "#0a1520"
        return [f"background-color:{bg}"] * len(row)

    def pct_fmt(col):
        styles = []
        for v in disp["_pct"]:
            if v > 0:   styles.append("color:#e74c3c; font-weight:600")
            elif v < 0: styles.append("color:#2ecc71; font-weight:600")
            else:       styles.append("color:#5a6a80")
        return styles

    def name_fmt(col):
        styles = []
        for new, cb in zip(disp["_new"], disp["_cb"]):
            if new:  styles.append("color:#f39c12; font-weight:700")
            elif cb: styles.append("color:#a78bfa")
            else:    styles.append("color:#c8d6e5")
        return styles

    def rank_fmt(col):
        return ["color:#4fc3f7; font-weight:700" if v <= 3 else "color:#4a6080"
                for v in disp["排行"]]

    styled = (
        disp.style
        .apply(row_bg, axis=1)
        .apply(pct_fmt,  subset=["漲跌幅"])
        .apply(name_fmt, subset=["股票名稱"])
        .apply(rank_fmt, subset=["排行"])
        .format({"成交金額(億)": "{:,.1f}"})
        .set_properties(**{
            "font-family": "'IBM Plex Mono', monospace",
            "font-size": "13px", "border": "none",
        })
        .set_properties(subset=["排行"], **{"text-align":"center"})
        .set_properties(subset=["股票名稱"], **{"text-align":"left"})
        .set_properties(subset=["漲跌幅","成交金額(億)"], **{"text-align":"right"})
        .set_table_styles([
            {"selector":"thead th", "props":[
                ("background-color","#0a1520"),("color","#4a6080"),
                ("font-family","'IBM Plex Mono', monospace"),("font-size","11px"),
                ("letter-spacing","1.5px"),("text-transform","uppercase"),
                ("border-bottom","1px solid #1a2940"),("padding","10px 14px"),
            ]},
            {"selector":"tbody td",  "props":[
                ("padding","10px 14px"),("border-bottom","1px solid #0d1a28"),
            ]},
            {"selector":"tbody tr:hover td", "props":[("filter","brightness(1.3)")]},
            {"selector":"table", "props":[("width","100%"),("border-collapse","collapse")]},
        ])
        .hide(axis="index")
        .hide(subset=["_pct","_new","_cb"], axis="columns")
    )

    # 歷史額外欄位格式
    if extra_cols:
        for src, dst in extra_cols:
            if dst in ["平均成交(億)","累積成交(億)"]:
                styled = styled.format({dst: "{:,.1f}"})

    return styled, disp[vis_cols + ["_pct","_new","_cb"]]


# ─────────────────────────────────────────────────────────────────────────────
# SHARED COMPONENTS
# ─────────────────────────────────────────────────────────────────────────────
def render_kpi(df: pd.DataFrame, prev_codes: set):
    up      = int((df["change_pct"] > 0).sum())
    dn      = int((df["change_pct"] < 0).sum())
    new_cnt = sum(1 for c in df["code"].astype(str) if prev_codes and c not in prev_codes)
    tot     = df["trade_value"].sum()
    ratio   = f"{up/len(df)*100:.0f}%" if len(df) else "0%"
    st.markdown(f"""
    <div class="kpi-strip">
        <div class="kpi"><div class="kpi-label">上榜股數</div>
            <div class="kpi-value neu">{len(df)}</div></div>
        <div class="kpi"><div class="kpi-label">上漲 ▲</div>
            <div class="kpi-value up">{up} <span style="font-size:14px;color:#8b3030">({ratio})</span></div></div>
        <div class="kpi"><div class="kpi-label">下跌 ▼</div>
            <div class="kpi-value dn">{dn}</div></div>
        <div class="kpi"><div class="kpi-label">新上榜 ★</div>
            <div class="kpi-value gold">{new_cnt}</div></div>
        <div class="kpi"><div class="kpi-label">合計成交 (億)</div>
            <div class="kpi-value neu">{tot:,.0f}</div></div>
    </div>
    """, unsafe_allow_html=True)


def render_legend():
    st.markdown("""
    <div class="legend-strip">
        <span style="color:#4a6080;font-size:10px;letter-spacing:1px;text-transform:uppercase">圖例</span>
        <div class="leg"><div class="dot" style="background:#4a1212"></div>
            <span style="color:#e74c3c">上漲（紅）</span></div>
        <div class="leg"><div class="dot" style="background:#0a3018"></div>
            <span style="color:#2ecc71">下跌（綠）</span></div>
        <div class="leg"><div class="dot" style="background:#3a2500"></div>
            <span style="color:#f39c12">★新 = 今日新上榜（與前日比較）</span></div>
        <div class="leg" style="color:#a78bfa">CB = 已發行可轉債</div>
    </div>
    """, unsafe_allow_html=True)


def _prep_hist_df(raw: pd.DataFrame) -> pd.DataFrame:
    """將 Google Sheets 讀回的 DataFrame 統一欄位名稱"""
    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # 欄位對應（容錯多種可能命名）
    rename_map = {}
    for col in df.columns:
        if col in ("代號","股票代號","Code","code"):        rename_map[col] = "code"
        elif col in ("名稱","股票名稱","Name","name"):      rename_map[col] = "name"
        elif "成交金額" in col or col in ("trade_value",):  rename_map[col] = "trade_value"
        elif "漲跌幅" in col or col in ("change_pct",):     rename_map[col] = "change_pct"

    df = df.rename(columns=rename_map)

    # 確保必要欄位存在
    for col in ["code","name","trade_value","change_pct"]:
        if col not in df.columns:
            df[col] = "" if col in ["code","name"] else 0.0

    df["trade_value"] = pd.to_numeric(df["trade_value"], errors="coerce").fillna(0)
    df["change_pct"]  = pd.to_numeric(df["change_pct"],  errors="coerce").fillna(0)
    df["rank"]        = range(1, len(df)+1)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 1 – REAL-TIME
# ─────────────────────────────────────────────────────────────────────────────
def page_realtime():
    now = datetime.now()
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
        ivl  = st.select_slider("刷新間隔(秒)", [15, 30, 60, 120], value=60)
        if st.button("⟳ 立即刷新", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        st.markdown("---")
        st.caption(
            "📡 資料來源\n\n"
            "成交金額：TWSE STOCK_DAY_ALL\n"
            "即時行情：mis.twse.com.tw\n"
            "CB資料：TWSE CB_OVERVIEW\n"
            "歷史儲存：Google Sheets"
        )

    today_key = now.strftime("%Y-%m-%d")
    today_str = now.strftime("%Y%m%d")

    # ── 載入資料 ──
    with st.spinner("載入成交資料中…"):
        df = fetch_twse_top30_aftermarket()

    # 盤中用即時報價更新漲跌幅
    if is_open and len(df) > 0:
        with st.spinner("取得即時行情…"):
            live = fetch_twse_realtime_batch(df["code"].tolist())
        for idx, row in df.iterrows():
            p = live.get(str(row["code"]))
            if p is not None:
                df.at[idx, "change_pct"] = p

    # CB 資料
    cb_codes = fetch_cb_stocks()

    # Google Sheets
    client     = gs_client()
    prev_codes = load_prev_codes(client, today_key)

    # 收盤後儲存
    if is_open or now.hour >= 14:
        save_today(client, df, today_key)

    # ── UI ──
    render_kpi(df, prev_codes)
    render_legend()
    st.markdown(
        f'<div class="refresh-info">最後更新 {now.strftime("%H:%M:%S")} &nbsp;·&nbsp; '
        f'CB 上榜：{sum(1 for c in df["code"] if c in cb_codes)} 支</div>',
        unsafe_allow_html=True)

    styled, _ = build_and_style(df, prev_codes, cb_codes)
    st.dataframe(styled, use_container_width=True, height=980, hide_index=True)

    if not is_open:
        st.info("📌 非交易時段（09:00–13:30），顯示最近收盤資料。盤後資料約 14:30 更新。")

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

    client  = gs_client()
    history = load_history(client)
    cb_codes = fetch_cb_stocks()

    if not history:
        st.warning(
            "尚無歷史資料。\n\n"
            "請先確認：\n"
            "1. Google Sheets API 已設定（參考 README）\n"
            "2. 服務帳戶已加入 Sheet 的共用編輯者\n"
            "3. 系統曾在交易日運行並完成儲存"
        )
        return

    dates      = sorted(history.keys(), reverse=True)
    all_sorted = sorted(history.keys())

    c1, c2 = st.columns([3, 1])
    with c1:
        selected = st.multiselect(
            "選擇日期（可多選）", dates,
            default=dates[:7] if len(dates) >= 7 else dates)
    with c2:
        mode = st.radio("顯示模式", ["每日明細", "彙總排行"], horizontal=True)

    if not selected:
        st.info("請選擇至少一個日期")
        return

    # ── 每日明細 ──
    if mode == "每日明細":
        for date in sorted(selected, reverse=True):
            raw = history.get(date)
            if raw is None or len(raw) == 0:
                continue
            df = _prep_hist_df(raw)

            prev_c = set()
            idx = all_sorted.index(date) if date in all_sorted else -1
            if idx > 0:
                try:
                    prev_raw = history.get(all_sorted[idx-1])
                    if prev_raw is not None:
                        prev_c = set(_prep_hist_df(prev_raw)["code"].astype(str).tolist())
                except Exception:
                    pass

            is_first = (date == sorted(selected, reverse=True)[0])
            with st.expander(f"📅 {date}", expanded=is_first):
                render_kpi(df, prev_c)
                render_legend()
                styled, _ = build_and_style(df, prev_c, cb_codes)
                st.dataframe(styled, use_container_width=True, height=600, hide_index=True)

    # ── 彙總排行 ──
    else:
        all_rows = []
        for d in selected:
            raw = history.get(d)
            if raw is None: continue
            tmp = _prep_hist_df(raw)
            tmp["_date"] = d
            all_rows.append(tmp)

        if not all_rows:
            st.warning("選擇的日期無有效資料")
            return

        combined = pd.concat(all_rows, ignore_index=True)
        agg = (combined.groupby(["code","name"])
               .agg(avg_val=("trade_value","mean"),
                    total_val=("trade_value","sum"),
                    days=("trade_value","count"),
                    avg_pct=("change_pct","mean"))
               .reset_index()
               .sort_values("total_val", ascending=False)
               .head(30).reset_index(drop=True))
        agg["rank"]        = range(1, len(agg)+1)
        agg["trade_value"] = agg["avg_val"]
        agg["change_pct"]  = agg["avg_pct"].round(2)

        sel_sorted = sorted(selected)
        prev_c = set()
        if sel_sorted[0] in all_sorted:
            i = all_sorted.index(sel_sorted[0])
            if i > 0:
                try:
                    praw = history.get(all_sorted[i-1])
                    if praw is not None:
                        prev_c = set(_prep_hist_df(praw)["code"].astype(str).tolist())
                except Exception:
                    pass

        period = f"{sel_sorted[0]} ~ {sel_sorted[-1]}" if len(sel_sorted) > 1 else sel_sorted[0]
        st.markdown(
            f'<div class="section-title">彙總 · {period} · {len(sel_sorted)} 個交易日</div>',
            unsafe_allow_html=True)
        render_legend()

        extra = [
            ("avg_val",   "平均成交(億)"),
            ("total_val", "累積成交(億)"),
            ("days",      "上榜天數"),
        ]
        styled, _ = build_and_style(agg, prev_c, cb_codes, extra_cols=extra)
        st.dataframe(styled, use_container_width=True, height=700, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    page = st.radio(
        "nav",
        ["📈  即時排行", "📊  歷史排行"],
        horizontal=True,
        label_visibility="collapsed",
    )
    if "即時" in page:
        page_realtime()
    else:
        page_history()


if __name__ == "__main__":
    main()
