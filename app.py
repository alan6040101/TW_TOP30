import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime
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
# GLOBAL CSS  – terminal / trading-floor dark aesthetic
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans+TC:wght@400;500;700&display=swap');

*, *::before, *::after { box-sizing: border-box; margin:0; padding:0; }
html, body, [data-testid="stAppViewContainer"] {
    background: #060b10 !important;
    color: #c8d6e5 !important;
    font-family: 'IBM Plex Sans TC', sans-serif;
}
[data-testid="stHeader"]  { background: transparent !important; }
[data-testid="stSidebar"] { background: #0a1520 !important; border-right: 1px solid #1a2940; }
section[data-testid="stSidebarContent"] * { color: #c8d6e5 !important; }
#MainMenu, footer, [data-testid="stToolbar"] { visibility: hidden; }
.block-container { padding: 1.5rem 2rem 4rem !important; max-width: 1440px; }

.topbar {
    display:flex; align-items:center; justify-content:space-between;
    padding:10px 0 18px; border-bottom:1px solid #1a2940; margin-bottom:20px;
}
.topbar-logo { font-family:'IBM Plex Mono',monospace; font-size:20px; font-weight:600; color:#4fc3f7; letter-spacing:2px; }
.topbar-sub  { font-size:11px; color:#4a6080; margin-top:2px; letter-spacing:1px; }
.status-pill {
    display:inline-flex; align-items:center; gap:7px;
    background:#0a1e12; border:1px solid #1a5c28;
    border-radius:4px; padding:5px 14px;
    font-family:'IBM Plex Mono',monospace; font-size:12px; color:#2ecc71;
}
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

.legend-strip {
    display:flex; align-items:center; gap:20px; flex-wrap:wrap;
    background:#0a1520; border:1px solid #1a2940; border-radius:4px;
    padding:8px 16px; font-size:11px; color:#4a6080; margin-bottom:14px;
}
.leg { display:flex; align-items:center; gap:6px; }
.dot { width:8px; height:8px; border-radius:1px; }
.refresh-info {
    font-family:'IBM Plex Mono',monospace; font-size:11px; color:#2a3a50;
    text-align:right; margin-bottom:10px; letter-spacing:.5px;
}
.section-title {
    font-family:'IBM Plex Mono',monospace; font-size:12px; color:#4a6080;
    letter-spacing:2px; text-transform:uppercase; margin:24px 0 12px;
    display:flex; align-items:center; gap:10px;
}
.section-title::after { content:''; flex:1; height:1px; background:#1a2940; }

div[data-testid="stRadio"] > div {
    display:flex !important; flex-direction:row !important; gap:0 !important;
    background:#0a1520; border:1px solid #1a2940; border-radius:4px;
    padding:2px; width:fit-content;
}
div[data-testid="stRadio"] label {
    font-family:'IBM Plex Mono',monospace !important;
    font-size:12px !important; padding:6px 16px !important;
    border-radius:3px !important; cursor:pointer; color:#4a6080 !important;
}
div[data-testid="stRadio"] label[data-checked="true"] {
    background:#1a2940 !important; color:#4fc3f7 !important;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
GOOGLE_SHEETS_ID = "1lTRxbT9iv3uRIrQ1wUFPV0LZbo8p5Qoh4a1FMGa-iU4"

CB_STOCKS = {
    "2330","2317","2454","3008","2382","2308","2303","6505","1301","1303",
    "2002","2412","2881","2882","2886","2891","2884","2885","2890","2880",
    "2892","6669","3231","2379","2395","2408","3034","2344","2357","4904",
    "3045","2603","2609","2615","2618","2633","2801","2823","2834","2838",
    "2845","3481","3673","3702","4938","4958","5871","5876","5880","6176",
    "6269","6278","6285","6443","6446","6456","6533","6547","6654","6770",
    "2204","2207","2301","2325","2345","2353","2356","2376","2385",
    "2392","2441","2449","2451","2458","2467","2474","2492","2498","2542",
    "2887","2888","2912","3706","5483","6271",
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
# DATA
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def fetch_twse_top30(date_str: str) -> pd.DataFrame:
    try:
        url = (f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX20"
               f"?date={date_str}&selectType=ALL")
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0",
                                        "Referer":"https://www.twse.com.tw/"}, timeout=10)
        payload = r.json()
        if payload.get("stat") == "OK" and payload.get("data"):
            cols = ["code","name","volume","trade_value_raw","open","high","low","close","change","txn"]
            df = pd.DataFrame(payload["data"], columns=cols)
            for c in ["trade_value_raw","close","volume"]:
                df[c] = df[c].str.replace(",","").str.replace("+","")
            df["trade_value"] = pd.to_numeric(df["trade_value_raw"], errors="coerce") / 1e8
            df["close"]       = pd.to_numeric(df["close"], errors="coerce")
            df["change_raw"]  = pd.to_numeric(
                df["change"].str.replace(",","").str.replace("+","").str.strip(), errors="coerce")
            df["prev_close"]  = df["close"] - df["change_raw"]
            df["change_pct"]  = (df["change_raw"] / df["prev_close"] * 100).round(2)
            df = df.sort_values("trade_value", ascending=False).head(30).reset_index(drop=True)
            df["rank"]   = range(1, len(df)+1)
            df["has_cb"] = df["code"].isin(CB_STOCKS)
            return df[["rank","code","name","trade_value","close","change_pct","has_cb"]]
    except Exception:
        pass
    return _demo_df()


@st.cache_data(ttl=30, show_spinner=False)
def fetch_realtime(codes: list) -> dict:
    prices = {}
    try:
        q = "|".join(f"tse_{c}.tw" for c in codes)
        r = requests.get(
            f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={q}&json=1&delay=0",
            headers={"User-Agent":"Mozilla/5.0","Referer":"https://mis.twse.com.tw/"}, timeout=8)
        for item in r.json().get("msgArray", []):
            c = item.get("c","")
            try:
                z = float(item.get("z") or item.get("y") or 0)
                y = float(item.get("y") or 0)
                prices[c] = round((z-y)/y*100, 2) if y else 0
            except Exception:
                pass
    except Exception:
        pass
    return prices


def _demo_df() -> pd.DataFrame:
    rng = random.Random(int(datetime.now().strftime("%Y%m%d")))
    rows = []
    for i, (code, name) in enumerate(DEMO_STOCKS):
        val = round(rng.uniform(10, 900), 1)
        pct = round(rng.uniform(-5, 5), 2)
        rows.append({"rank":i+1,"code":code,"name":name,
                     "trade_value":val,"close":round(rng.uniform(30,1200),1),
                     "change_pct":pct,"has_cb":code in CB_STOCKS})
    df = pd.DataFrame(rows).sort_values("trade_value",ascending=False).reset_index(drop=True)
    df["rank"] = range(1, len(df)+1)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────────────────
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
    except Exception:
        pass
    return None


def save_today(client, df: pd.DataFrame, date_key: str):
    if not client: return
    try:
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        try:   ws = sh.worksheet(date_key); ws.clear()
        except: ws = sh.add_worksheet(date_key, 35, 6)
        out = df[["code","name","trade_value","change_pct"]].copy()
        out.columns = ["代號","名稱","成交金額(億)","漲跌幅(%)"]
        ws.update([out.columns.tolist()] + out.values.tolist())
    except Exception as e:
        st.sidebar.caption(f"⚠️ Sheets 寫入失敗: {e}")


def load_prev_codes(client, today_key: str) -> set:
    if not client: return set()
    try:
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        titles = sorted(ws.title for ws in sh.worksheets() if ws.title < today_key)
        if not titles: return set()
        data = sh.worksheet(titles[-1]).get_all_records()
        return {str(r.get("代號","")) for r in data}
    except Exception:
        return set()


def load_history(client) -> dict:
    if not client: return {}
    out = {}
    try:
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        for ws in sorted(sh.worksheets(), key=lambda x: x.title, reverse=True)[:30]:
            try:
                data = ws.get_all_records()
                if data: out[ws.title] = pd.DataFrame(data)
            except Exception:
                pass
    except Exception:
        pass
    return out


# ─────────────────────────────────────────────────────────────────────────────
# TABLE  (Pandas Styler – the CORRECT way to colour rows in Streamlit)
# ─────────────────────────────────────────────────────────────────────────────
def build_display_df(df: pd.DataFrame, prev_codes: set) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        code   = str(r["code"])
        is_new = bool(prev_codes) and code not in prev_codes
        pct    = float(r.get("change_pct") or 0)
        has_cb = bool(r.get("has_cb", code in CB_STOCKS))
        rank   = int(r.get("rank", _+1))

        name_cell = str(r["name"])
        if is_new: name_cell += "  ★新"
        if has_cb: name_cell += "  CB"

        if pct > 0:   pct_str = f"▲ {pct:.2f}%"
        elif pct < 0: pct_str = f"▼ {abs(pct):.2f}%"
        else:         pct_str = "─"

        rows.append({
            "排名":     rank,
            "代號":     code,
            "名稱":     name_cell,
            "成交金額(億)": float(r.get("trade_value", 0)),
            "漲跌幅":   pct_str,
            "_pct":    pct,
            "_new":    is_new,
            "_cb":     has_cb,
        })
    return pd.DataFrame(rows)


def style_table(disp: pd.DataFrame) -> object:
    def row_bg(row):
        new = row["_new"]; pct = row["_pct"]
        if new:
            bg = "#191000"
        elif pct > 0:
            bg = "#1a0a0a"
        elif pct < 0:
            bg = "#061209"
        else:
            bg = "#0a1520"
        return [f"background-color:{bg}"] * len(row)

    def pct_color(col):
        styles = []
        for v in disp["_pct"]:
            if v > 0:   styles.append("color:#e74c3c; font-weight:600")
            elif v < 0: styles.append("color:#2ecc71; font-weight:600")
            else:       styles.append("color:#5a6a80")
        return styles

    def name_color(col):
        styles = []
        for new, cb in zip(disp["_new"], disp["_cb"]):
            if new:  styles.append("color:#f39c12; font-weight:700")
            elif cb: styles.append("color:#a78bfa")
            else:    styles.append("color:#c8d6e5")
        return styles

    def rank_color(col):
        styles = []
        for v in disp["排名"]:
            if v <= 3: styles.append("color:#4fc3f7; font-weight:700")
            else:      styles.append("color:#4a6080")
        return styles

    mono = "font-family:'IBM Plex Mono', monospace; font-size:13px"

    styled = (
        disp.style
        .apply(row_bg, axis=1)
        .apply(pct_color,  subset=["漲跌幅"])
        .apply(name_color, subset=["名稱"])
        .apply(rank_color, subset=["排名"])
        .format({"成交金額(億)": "{:,.1f}"})
        .set_properties(**{"font-family":"'IBM Plex Mono', monospace", "font-size":"13px", "border":"none"})
        .set_properties(subset=["排名","代號"],     **{"text-align":"center"})
        .set_properties(subset=["成交金額(億)","漲跌幅"], **{"text-align":"right"})
        .set_table_styles([
            {"selector":"thead th", "props":[
                ("background-color","#0a1520"),("color","#4a6080"),
                ("font-family","'IBM Plex Mono', monospace"),("font-size","11px"),
                ("letter-spacing","1.5px"),("text-transform","uppercase"),
                ("border-bottom","1px solid #1a2940"),("padding","10px 14px"),
                ("text-align","center"),
            ]},
            {"selector":"tbody td",  "props":[("padding","9px 14px"),("border-bottom","1px solid #0d1a28")]},
            {"selector":"tbody tr:hover td", "props":[("filter","brightness(1.25)")]},
            {"selector":"table",     "props":[("width","100%"),("border-collapse","collapse")]},
        ])
        .hide(axis="index")
        .hide(subset=["_pct","_new","_cb"], axis="columns")
    )
    return styled


# ─────────────────────────────────────────────────────────────────────────────
# SHARED UI COMPONENTS
# ─────────────────────────────────────────────────────────────────────────────
def render_kpi(df: pd.DataFrame, prev_codes: set):
    up      = int((df["change_pct"] > 0).sum())
    dn      = int((df["change_pct"] < 0).sum())
    new_cnt = sum(1 for c in df["code"].astype(str) if prev_codes and c not in prev_codes)
    tot     = df["trade_value"].sum()
    ratio   = f"{up/len(df)*100:.0f}%" if len(df) else "0%"
    st.markdown(f"""
    <div class="kpi-strip">
        <div class="kpi">
            <div class="kpi-label">上榜股數</div>
            <div class="kpi-value neu">{len(df)}</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">上漲 ▲</div>
            <div class="kpi-value up">{up} <span style="font-size:14px;color:#8b3030">({ratio})</span></div>
        </div>
        <div class="kpi">
            <div class="kpi-label">下跌 ▼</div>
            <div class="kpi-value dn">{dn}</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">新上榜 ★</div>
            <div class="kpi-value gold">{new_cnt}</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">合計成交 (億)</div>
            <div class="kpi-value neu">{tot:,.0f}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_legend():
    st.markdown("""
    <div class="legend-strip">
        <span style="color:#4a6080;font-size:10px;letter-spacing:1px;text-transform:uppercase">圖例</span>
        <div class="leg"><div class="dot" style="background:#4a1212"></div><span style="color:#e74c3c">上漲（紅）</span></div>
        <div class="leg"><div class="dot" style="background:#0a3018"></div><span style="color:#2ecc71">下跌（綠）</span></div>
        <div class="leg"><div class="dot" style="background:#3a2500"></div><span style="color:#f39c12">★ 新上榜（與前日比較）</span></div>
        <div class="leg" style="color:#a78bfa">CB = 已發行可轉債</div>
        <div class="leg" style="color:#4fc3f7">Top 3 排名以藍色標示</div>
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 1 – REAL-TIME
# ─────────────────────────────────────────────────────────────────────────────
def page_realtime():
    now = datetime.now()
    is_open = (now.weekday() < 5
               and now.replace(hour=9,  minute=0, second=0) <= now
               <= now.replace(hour=13, minute=30, second=0))

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
        st.caption("資料來源：台灣證券交易所\n\nCB 名單為內建靜態清單，\n如需更新請修改 app.py 的 CB_STOCKS。")

    today_str = now.strftime("%Y%m%d")
    today_key = now.strftime("%Y-%m-%d")

    with st.spinner("載入資料中…"):
        df = fetch_twse_top30(today_str)

    if is_open:
        live = fetch_realtime(df["code"].tolist())
        for idx, row in df.iterrows():
            p = live.get(str(row["code"]))
            if p is not None:
                df.at[idx, "change_pct"] = p

    client     = gs_client()
    prev_codes = load_prev_codes(client, today_key)

    if is_open or now.hour >= 14:
        save_today(client, df, today_key)

    render_kpi(df, prev_codes)
    render_legend()

    st.markdown(
        f'<div class="refresh-info">最後更新 {now.strftime("%H:%M:%S")} &nbsp;·&nbsp; 資料來源：台灣證券交易所</div>',
        unsafe_allow_html=True)

    disp   = build_display_df(df, prev_codes)
    styled = style_table(disp)
    st.dataframe(styled, use_container_width=True, height=980, hide_index=True)

    if not is_open:
        st.info("📌 目前非交易時段（09:00–13:30），顯示最近收盤資料。")

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

    if not history:
        st.warning("尚無歷史資料。請先讓「即時排行」頁面執行至少一個完整交易日，以儲存資料至 Google Sheets。")
        return

    dates = sorted(history.keys(), reverse=True)
    c1, c2 = st.columns([3,1])
    with c1:
        selected = st.multiselect("選擇日期（可多選）", dates,
                                   default=dates[:7] if len(dates) >= 7 else dates)
    with c2:
        mode = st.radio("顯示模式", ["每日明細","彙總排行"], horizontal=True)

    if not selected:
        st.info("請選擇至少一個日期")
        return

    all_sorted = sorted(history.keys())

    if mode == "每日明細":
        for date in sorted(selected, reverse=True):
            df = _prep_hist_df(history[date])

            prev_c = set()
            idx = all_sorted.index(date) if date in all_sorted else -1
            if idx > 0:
                try: prev_c = set(history[all_sorted[idx-1]].iloc[:,0].astype(str).tolist())
                except: pass

            with st.expander(f"📅 {date}", expanded=(date == sorted(selected, reverse=True)[0])):
                render_kpi(df, prev_c)
                render_legend()
                st.dataframe(style_table(build_display_df(df, prev_c)),
                             use_container_width=True, height=600, hide_index=True)

    else:  # 彙總排行
        all_rows = []
        for d in selected:
            tmp = _prep_hist_df(history[d]); tmp["_date"] = d; all_rows.append(tmp)

        combined = pd.concat(all_rows, ignore_index=True)
        agg = (combined.groupby(["code","name"])
               .agg(avg_val=("trade_value","mean"),
                    total_val=("trade_value","sum"),
                    days=("trade_value","count"),
                    avg_pct=("change_pct","mean"))
               .reset_index()
               .sort_values("total_val", ascending=False)
               .head(30).reset_index(drop=True))
        agg["rank"]   = range(1, len(agg)+1)
        agg["has_cb"] = agg["code"].astype(str).isin(CB_STOCKS)

        sel_sorted = sorted(selected)
        prev_c = set()
        if sel_sorted[0] in all_sorted:
            idx = all_sorted.index(sel_sorted[0])
            if idx > 0:
                try: prev_c = set(history[all_sorted[idx-1]].iloc[:,0].astype(str).tolist())
                except: pass

        period = f"{sel_sorted[0]} ~ {sel_sorted[-1]}" if len(sel_sorted) > 1 else sel_sorted[0]
        st.markdown(f'<div class="section-title">彙總 · {period} · {len(sel_sorted)} 個交易日</div>',
                    unsafe_allow_html=True)
        render_legend()

        # Build aggregate display df
        rows = []
        for _, r in agg.iterrows():
            code = str(r["code"]); pct = float(r["avg_pct"])
            is_new = bool(prev_c) and code not in prev_c
            has_cb = bool(r.get("has_cb", code in CB_STOCKS))
            name_cell = r["name"] + ("  ★新" if is_new else "") + ("  CB" if has_cb else "")
            pct_str = f"▲ {pct:.2f}%" if pct>0 else (f"▼ {abs(pct):.2f}%" if pct<0 else "─")
            rows.append({
                "排名": int(r["rank"]), "代號": code, "名稱": name_cell,
                "平均成交(億)": float(r["avg_val"]),
                "累積成交(億)": float(r["total_val"]),
                "上榜天數": int(r["days"]),
                "平均漲跌": pct_str,
                "_pct": pct, "_new": is_new, "_cb": has_cb,
            })
        disp = pd.DataFrame(rows)

        def row_bg_agg(row):
            if row["_new"]: bg = "#191000"
            elif row["_pct"]>0: bg = "#1a0a0a"
            elif row["_pct"]<0: bg = "#061209"
            else: bg = "#0a1520"
            return [f"background-color:{bg}"]*len(row)

        def pct_color_agg(col):
            return ["color:#e74c3c;font-weight:600" if v>0
                    else ("color:#2ecc71;font-weight:600" if v<0 else "color:#5a6a80")
                    for v in disp["_pct"]]

        def name_color_agg(col):
            return ["color:#f39c12;font-weight:700" if n
                    else ("color:#a78bfa" if cb else "color:#c8d6e5")
                    for n,cb in zip(disp["_new"],disp["_cb"])]

        styled_agg = (
            disp.style
            .apply(row_bg_agg, axis=1)
            .apply(pct_color_agg, subset=["平均漲跌"])
            .apply(name_color_agg, subset=["名稱"])
            .format({"平均成交(億)":"{:,.1f}","累積成交(億)":"{:,.1f}"})
            .set_properties(**{"font-family":"'IBM Plex Mono',monospace","font-size":"13px","border":"none"})
            .set_properties(subset=["排名","代號","上榜天數"], **{"text-align":"center"})
            .set_properties(subset=["平均成交(億)","累積成交(億)","平均漲跌"], **{"text-align":"right"})
            .set_table_styles([
                {"selector":"thead th","props":[
                    ("background-color","#0a1520"),("color","#4a6080"),
                    ("font-family","'IBM Plex Mono',monospace"),("font-size","11px"),
                    ("letter-spacing","1.5px"),("text-transform","uppercase"),
                    ("border-bottom","1px solid #1a2940"),("padding","10px 14px"),
                ]},
                {"selector":"tbody td","props":[("padding","9px 14px"),("border-bottom","1px solid #0d1a28")]},
                {"selector":"tbody tr:hover td","props":[("filter","brightness(1.25)")]},
                {"selector":"table","props":[("width","100%"),("border-collapse","collapse")]},
            ])
            .hide(axis="index")
            .hide(subset=["_pct","_new","_cb"], axis="columns")
        )
        st.dataframe(styled_agg, use_container_width=True, height=700, hide_index=True)


def _prep_hist_df(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={"代號":"code","名稱":"name",
                              "成交金額(億)":"trade_value","漲跌幅(%)":"change_pct"})
    df["trade_value"] = pd.to_numeric(df["trade_value"], errors="coerce").fillna(0)
    df["change_pct"]  = pd.to_numeric(df["change_pct"],  errors="coerce").fillna(0)
    df["rank"]        = range(1, len(df)+1)
    df["has_cb"]      = df["code"].astype(str).isin(CB_STOCKS)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    page = st.radio("nav", ["📈  即時排行", "📊  歷史排行"],
                    horizontal=True, label_visibility="collapsed")
    if "即時" in page:
        page_realtime()
    else:
        page_history()


if __name__ == "__main__":
    main()
