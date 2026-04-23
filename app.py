import streamlit as st
import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import os

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="台股成交金額排行",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+TC:wght@400;500;700&family=JetBrains+Mono:wght@400;600&display=swap');

:root {
    --bg-primary: #0d1117;
    --bg-secondary: #161b22;
    --bg-card: #1c2128;
    --border: #30363d;
    --text-primary: #e6edf3;
    --text-secondary: #8b949e;
    --red: #ff6b6b;
    --red-bg: rgba(255,107,107,0.12);
    --green: #3fb950;
    --green-bg: rgba(63,185,80,0.12);
    --new-entry: #e3b341;
    --new-entry-bg: rgba(227,179,65,0.15);
    --cb-mark: #a371f7;
    --accent: #388bfd;
}

html, body, [class*="css"] {
    font-family: 'Noto Sans TC', sans-serif;
    background-color: var(--bg-primary);
    color: var(--text-primary);
}

.main { background: var(--bg-primary); }

/* Header */
.hero-header {
    background: linear-gradient(135deg, #0d1117 0%, #161b22 50%, #1c2128 100%);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 24px 32px;
    margin-bottom: 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
}
.hero-title { font-size: 28px; font-weight: 700; color: var(--text-primary); margin: 0; }
.hero-subtitle { font-size: 13px; color: var(--text-secondary); margin-top: 4px; }
.live-badge {
    display: inline-flex; align-items: center; gap: 6px;
    background: rgba(56,139,253,0.15); border: 1px solid var(--accent);
    border-radius: 20px; padding: 4px 12px;
    font-size: 12px; color: var(--accent); font-weight: 600;
}
.live-dot {
    width: 8px; height: 8px; border-radius: 50%;
    background: var(--accent); animation: pulse 1.5s infinite;
}
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }

/* Stats bar */
.stats-bar {
    display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap;
}
.stat-card {
    background: var(--bg-card); border: 1px solid var(--border);
    border-radius: 10px; padding: 14px 20px; flex: 1; min-width: 140px;
}
.stat-label { font-size: 11px; color: var(--text-secondary); text-transform: uppercase; letter-spacing: .8px; }
.stat-value { font-size: 22px; font-weight: 700; margin-top: 4px; font-family: 'JetBrains Mono', monospace; }
.stat-up { color: var(--red); }
.stat-dn { color: var(--green); }
.stat-neu { color: var(--text-primary); }

/* Table */
.stock-table { width: 100%; border-collapse: separate; border-spacing: 0; }
.stock-table th {
    background: var(--bg-secondary); color: var(--text-secondary);
    font-size: 11px; text-transform: uppercase; letter-spacing: .8px;
    padding: 10px 14px; border-bottom: 1px solid var(--border);
    text-align: right; position: sticky; top: 0; z-index: 10;
}
.stock-table th:first-child, .stock-table th:nth-child(2), .stock-table th:nth-child(3) {
    text-align: left;
}
.stock-table td {
    padding: 10px 14px; border-bottom: 1px solid rgba(48,54,61,.5);
    font-size: 13px; text-align: right;
}
.stock-table td:first-child, .stock-table td:nth-child(2), .stock-table td:nth-child(3) {
    text-align: left;
}
.stock-table tr:hover td { background: rgba(48,54,61,.4); }

/* Row types */
.row-up td { background: var(--red-bg); }
.row-dn td { background: var(--green-bg); }
.row-new td { background: var(--new-entry-bg) !important; }
.row-new td:nth-child(2) { color: var(--new-entry); font-weight: 600; }

/* Badges */
.badge { display: inline-block; border-radius: 4px; padding: 1px 6px; font-size: 11px; font-weight: 600; margin-left: 4px; }
.badge-new { background: rgba(227,179,65,.25); color: var(--new-entry); border: 1px solid var(--new-entry); }
.badge-cb { background: rgba(163,113,247,.2); color: var(--cb-mark); border: 1px solid var(--cb-mark); }

/* Change cell */
.chg-up { color: var(--red); font-weight: 600; font-family: 'JetBrains Mono', monospace; }
.chg-dn { color: var(--green); font-weight: 600; font-family: 'JetBrains Mono', monospace; }
.chg-nc { color: var(--text-secondary); font-family: 'JetBrains Mono', monospace; }

/* Rank */
.rank-num {
    font-family: 'JetBrains Mono', monospace; font-weight: 600;
    color: var(--text-secondary); font-size: 13px;
}
.rank-top { color: var(--accent) !important; }

/* Legend */
.legend {
    display: flex; gap: 20px; flex-wrap: wrap;
    background: var(--bg-card); border: 1px solid var(--border);
    border-radius: 8px; padding: 12px 16px; margin-bottom: 16px;
    font-size: 12px; color: var(--text-secondary); align-items: center;
}
.legend-item { display: flex; align-items: center; gap: 6px; }
.legend-dot { width: 10px; height: 10px; border-radius: 2px; }

/* Auto-refresh progress */
.refresh-bar {
    font-size: 11px; color: var(--text-secondary);
    display: flex; align-items: center; gap: 8px;
}

/* Streamlit overrides */
[data-testid="stSidebar"] { background: var(--bg-secondary) !important; }
[data-testid="stSidebar"] * { color: var(--text-primary) !important; }
div[data-testid="metric-container"] { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
.stProgress > div > div > div { background: var(--accent) !important; }
</style>
""", unsafe_allow_html=True)

# ── Constants ──────────────────────────────────────────────────────────────────
GOOGLE_SHEETS_ID = "1lTRxbT9iv3uRIrQ1wUFPV0LZbo8p5Qoh4a1FMGa-iU4"
TWSE_API = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX20"
TWSE_REALTIME = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"

# ── Convertible Bond Data ──────────────────────────────────────────────────────
# Known stocks with convertible bonds (CB) - update periodically
# Source: Taiwan Stock Exchange CB list
CB_STOCKS = {
    "2330", "2317", "2454", "3008", "2382", "2308", "2303", "6505",
    "1301", "1303", "2002", "2105", "2204", "2207", "2301", "2325",
    "2345", "2353", "2356", "2357", "2376", "2379", "2385", "2392",
    "2395", "2408", "2412", "2441", "2449", "2451", "2458", "2467",
    "2474", "2492", "2498", "2542", "2603", "2609", "2615", "2618",
    "2633", "2801", "2823", "2834", "2838", "2845", "2880", "2881",
    "2882", "2883", "2884", "2885", "2886", "2887", "2888", "2890",
    "2891", "2892", "2912", "3034", "3045", "3231", "3481", "3673",
    "3702", "4904", "4938", "4958", "5871", "5876", "5880", "6176",
    "6269", "6278", "6285", "6443", "6446", "6456", "6533", "6547",
    "6654", "6669", "6770", "8�詣詣8詣", "9933", "9938",
    "2615", "5483", "6271", "3706", "8詣詣8", "2344", "3552",
}

# ── Google Sheets ──────────────────────────────────────────────────────────────
def get_gspread_client():
    """Get authenticated gspread client using service account from secrets."""
    try:
        creds_dict = st.secrets.get("gcp_service_account", None)
        if creds_dict:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_info(dict(creds_dict), scopes=scopes)
            return gspread.authorize(creds)
    except Exception as e:
        st.sidebar.warning(f"Google Sheets 未設定: {e}")
    return None


def save_to_sheets(client, df: pd.DataFrame, date_str: str):
    """Save today's TOP30 to Google Sheets."""
    if client is None:
        return
    try:
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        try:
            ws = sh.worksheet(date_str)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=date_str, rows=40, cols=10)
        df_save = df[["code", "name", "trade_value", "change_pct"]].copy()
        df_save.columns = ["代號", "名稱", "成交金額(億)", "漲跌幅(%)"]
        ws.update([df_save.columns.tolist()] + df_save.values.tolist())
    except Exception as e:
        st.sidebar.warning(f"儲存 Sheets 失敗: {e}")


def load_prev_day_from_sheets(client, date_str: str) -> set:
    """Load previous trading day's TOP30 stock codes."""
    if client is None:
        return set()
    try:
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        worksheets = sh.worksheets()
        titles = sorted([ws.title for ws in worksheets if ws.title != date_str])
        if not titles:
            return set()
        prev_ws = sh.worksheet(titles[-1])
        data = prev_ws.get_all_records()
        return {str(row.get("代號", "")) for row in data}
    except Exception:
        return set()


def load_history_from_sheets(client) -> dict:
    """Load all historical data from Google Sheets."""
    if client is None:
        return {}
    history = {}
    try:
        sh = client.open_by_key(GOOGLE_SHEETS_ID)
        worksheets = sh.worksheets()
        for ws in sorted(worksheets, key=lambda x: x.title, reverse=True)[:30]:
            try:
                data = ws.get_all_records()
                if data:
                    history[ws.title] = pd.DataFrame(data)
            except Exception:
                continue
    except Exception:
        pass
    return history


# ── TWSE Data Fetching ─────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def fetch_twse_top30() -> pd.DataFrame:
    """Fetch today's TOP30 by trading value from TWSE."""
    today = datetime.now().strftime("%Y%m%d")
    try:
        # Try real-time first (during market hours)
        url = f"{TWSE_API}?date={today}&selectType=ALL"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.twse.com.tw/",
        }
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()

        if data.get("stat") == "OK" and data.get("data"):
            rows = data["data"]
            df = pd.DataFrame(rows, columns=[
                "code", "name", "volume", "trade_value_raw",
                "open", "high", "low", "close", "change", "transactions"
            ])
            df["trade_value"] = df["trade_value_raw"].str.replace(",", "").astype(float) / 1e8
            df = df.sort_values("trade_value", ascending=False).head(30).reset_index(drop=True)
            df["rank"] = range(1, len(df) + 1)
            df["close"] = df["close"].str.replace(",", "").astype(float, errors='ignore')
            df["change"] = pd.to_numeric(df["change"].str.replace("+", "").str.replace(",", ""), errors='coerce')
            df["change_pct"] = (df["change"] / (df["close"] - df["change"]) * 100).round(2)
            df["has_cb"] = df["code"].isin(CB_STOCKS)
            return df
    except Exception:
        pass

    # Return sample/demo data if fetch fails
    return generate_demo_data()


@st.cache_data(ttl=30)
def fetch_realtime_prices(codes: list) -> dict:
    """Fetch real-time prices for given stock codes."""
    prices = {}
    try:
        query = "|".join([f"tse_{c}.tw" for c in codes])
        url = f"{TWSE_REALTIME}?ex_ch={query}&json=1&delay=0"
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://mis.twse.com.tw/"}
        r = requests.get(url, headers=headers, timeout=8)
        data = r.json()
        for item in data.get("msgArray", []):
            code = item.get("c", "")
            try:
                z = float(item.get("z", item.get("y", 0)) or 0)
                y = float(item.get("y", 0) or 0)
                chg = round(z - y, 2) if y else 0
                chg_pct = round(chg / y * 100, 2) if y else 0
                prices[code] = {"price": z, "prev_close": y, "change": chg, "change_pct": chg_pct}
            except Exception:
                pass
    except Exception:
        pass
    return prices


def generate_demo_data() -> pd.DataFrame:
    """Generate demo data when API is unavailable."""
    import random
    stocks = [
        ("2330", "台積電"), ("2317", "鴻海"), ("2454", "聯發科"), ("3008", "大立光"),
        ("2382", "廣達"), ("2308", "台達電"), ("2303", "聯電"), ("6505", "台塑化"),
        ("1301", "台塑"), ("2002", "中鋼"), ("2412", "中華電"), ("2881", "富邦金"),
        ("2882", "國泰金"), ("2886", "兆豐金"), ("2891", "中信金"), ("2884", "玉山金"),
        ("2885", "元大金"), ("2890", "永豐金"), ("2880", "華南金"), ("2892", "第一金"),
        ("6669", "緯穎"), ("3231", "緯創"), ("2379", "瑞昱"), ("2395", "研華"),
        ("2408", "南亞科"), ("3034", "聯詠"), ("2344", "華邦電"), ("2357", "華碩"),
        ("4904", "遠傳"), ("3045", "台灣大"),
    ]
    random.seed(int(datetime.now().strftime("%Y%m%d")))
    data = []
    for i, (code, name) in enumerate(stocks):
        val = round(random.uniform(5, 800), 1)
        chg_pct = round(random.uniform(-5, 5), 2)
        data.append({
            "rank": i + 1, "code": code, "name": name,
            "trade_value": val,
            "close": round(random.uniform(50, 1000), 1),
            "change": round(chg_pct * random.uniform(0.5, 2), 2),
            "change_pct": chg_pct,
            "has_cb": code in CB_STOCKS,
        })
    df = pd.DataFrame(data).sort_values("trade_value", ascending=False).reset_index(drop=True)
    df["rank"] = range(1, len(df) + 1)
    return df


# ── Rendering helpers ──────────────────────────────────────────────────────────
def render_change(pct):
    if pd.isna(pct):
        return '<span class="chg-nc">－</span>'
    if pct > 0:
        return f'<span class="chg-up">▲ {pct:.2f}%</span>'
    elif pct < 0:
        return f'<span class="chg-dn">▼ {abs(pct):.2f}%</span>'
    return f'<span class="chg-nc">0.00%</span>'


def render_rank(i):
    cls = "rank-top" if i <= 3 else ""
    return f'<span class="rank-num {cls}">{i}</span>'


def render_table(df: pd.DataFrame, prev_codes: set, title="", show_date_col=False):
    """Render the main stock table as HTML."""
    st.markdown(f"### {title}" if title else "", unsafe_allow_html=True)

    # Stats
    up = (df["change_pct"] > 0).sum()
    dn = (df["change_pct"] < 0).sum()
    nc = len(df) - up - dn
    new_count = len([c for c in df["code"] if c in prev_codes == False])

    cols = st.columns(5)
    with cols[0]:
        st.markdown(f"""<div class="stat-card"><div class="stat-label">上榜股數</div>
        <div class="stat-value stat-neu">{len(df)}</div></div>""", unsafe_allow_html=True)
    with cols[1]:
        ratio = f"{up/len(df)*100:.0f}%" if len(df) else "0%"
        st.markdown(f"""<div class="stat-card"><div class="stat-label">上漲</div>
        <div class="stat-value stat-up">▲ {up} ({ratio})</div></div>""", unsafe_allow_html=True)
    with cols[2]:
        st.markdown(f"""<div class="stat-card"><div class="stat-label">下跌</div>
        <div class="stat-value stat-dn">▼ {dn}</div></div>""", unsafe_allow_html=True)
    with cols[3]:
        st.markdown(f"""<div class="stat-card"><div class="stat-label">平盤</div>
        <div class="stat-value stat-neu">— {nc}</div></div>""", unsafe_allow_html=True)
    with cols[4]:
        total_val = df["trade_value"].sum()
        st.markdown(f"""<div class="stat-card"><div class="stat-label">合計成交(億)</div>
        <div class="stat-value stat-neu">{total_val:,.0f}</div></div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Legend
    st.markdown("""
    <div class="legend">
        <span style="color:#8b949e;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.8px;">圖例</span>
        <div class="legend-item"><div class="legend-dot" style="background:rgba(255,107,107,.35)"></div> 上漲</div>
        <div class="legend-item"><div class="legend-dot" style="background:rgba(63,185,80,.35)"></div> 下跌</div>
        <div class="legend-item"><div class="legend-dot" style="background:rgba(227,179,65,.35)"></div> <span style="color:var(--new-entry)">★ 新上榜</span></div>
        <div class="legend-item"><span class="badge badge-cb">CB</span> 已發行可轉債</div>
    </div>
    """, unsafe_allow_html=True)

    # Table
    date_col = "<th>日期</th>" if show_date_col else ""
    html = f"""<table class="stock-table">
    <thead><tr>
        <th style="width:50px">排名</th>
        <th>代號</th>
        <th>名稱</th>
        {date_col}
        <th>成交金額 (億)</th>
        <th>漲跌幅</th>
    </tr></thead><tbody>"""

    for _, row in df.iterrows():
        code = str(row["code"])
        is_up = row.get("change_pct", 0) > 0
        is_dn = row.get("change_pct", 0) < 0
        is_new = code not in prev_codes if prev_codes else False
        has_cb = row.get("has_cb", code in CB_STOCKS)

        row_class = ""
        if is_new:
            row_class = "row-new"
        elif is_up:
            row_class = "row-up"
        elif is_dn:
            row_class = "row-dn"

        new_badge = '<span class="badge badge-new">★ 新</span>' if is_new else ""
        cb_badge = '<span class="badge badge-cb">CB</span>' if has_cb else ""
        rank_html = render_rank(int(row.get("rank", _+1)))
        chg_html = render_change(row.get("change_pct", None))

        date_td = f"<td>{row.get('date','')}</td>" if show_date_col else ""

        html += f"""<tr class="{row_class}">
            <td>{rank_html}</td>
            <td><b style="font-family:'JetBrains Mono',monospace">{code}</b></td>
            <td>{row['name']}{new_badge}{cb_badge}</td>
            {date_td}
            <td><b style="font-family:'JetBrains Mono',monospace">{float(row['trade_value']):,.1f}</b></td>
            <td>{chg_html}</td>
        </tr>"""

    html += "</tbody></table>"
    st.markdown(html, unsafe_allow_html=True)


# ── Pages ──────────────────────────────────────────────────────────────────────
def page_realtime():
    now = datetime.now()
    market_open = now.replace(hour=9, minute=0, second=0)
    market_close = now.replace(hour=13, minute=30, second=0)
    is_weekday = now.weekday() < 5
    is_trading = is_weekday and market_open <= now <= market_close

    st.markdown(f"""
    <div class="hero-header">
        <div>
            <div class="hero-title">📈 台股成交金額 TOP 30</div>
            <div class="hero-subtitle">Taiwan Stock Exchange · Daily Volume Leaders</div>
        </div>
        <div>
            <div class="live-badge">
                <div class="live-dot"></div>
                {"盤中即時" if is_trading else "盤後資料"}
            </div>
            <div style="color:#8b949e;font-size:12px;margin-top:6px;text-align:right">
                {now.strftime("%Y/%m/%d %H:%M:%S")}
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Auto-refresh controls
    with st.sidebar:
        st.markdown("### ⚙️ 設定")
        auto_refresh = st.checkbox("自動更新", value=True)
        refresh_interval = st.selectbox("更新間隔", [30, 60, 120, 300], index=1, format_func=lambda x: f"{x} 秒")
        st.markdown("---")
        if st.button("🔄 立即更新"):
            st.cache_data.clear()
            st.rerun()

    # Fetch data
    with st.spinner("載入資料中..."):
        df = fetch_twse_top30()

    # Enrich with real-time prices during trading hours
    if is_trading and len(df) > 0:
        codes = df["code"].tolist()
        rt_prices = fetch_realtime_prices(codes)
        for idx, row in df.iterrows():
            rt = rt_prices.get(str(row["code"]))
            if rt:
                df.at[idx, "change_pct"] = rt["change_pct"]
                df.at[idx, "change"] = rt["change"]

    # Load prev day
    gs_client = get_gspread_client()
    today_str = now.strftime("%Y-%m-%d")
    prev_codes = load_prev_day_from_sheets(gs_client, today_str)

    # Save today's data (during/after market)
    if is_trading or now.hour >= 14:
        save_to_sheets(gs_client, df, today_str)

    render_table(df, prev_codes, title="")

    # Auto refresh
    if auto_refresh and is_trading:
        time.sleep(refresh_interval)
        st.rerun()
    elif auto_refresh and not is_trading:
        st.info("📌 目前非交易時段（09:00–13:30），顯示最後收盤資料")


def page_history():
    st.markdown("""
    <div class="hero-header">
        <div>
            <div class="hero-title">📊 歷史成交金額排行</div>
            <div class="hero-subtitle">Historical Volume Leaders · 近期交易日</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    gs_client = get_gspread_client()
    history = load_history_from_sheets(gs_client)

    if not history:
        st.warning("尚無歷史資料。請先在「即時排行」頁面讓系統儲存資料至 Google Sheets。")
        st.info("💡 如果您尚未設定 Google Sheets API，請參考側邊欄說明。")
        # Show demo history
        st.markdown("#### 示範：歷史排行表格格式")
        _show_demo_history()
        return

    dates = sorted(history.keys(), reverse=True)
    with st.sidebar:
        st.markdown("### 📅 選擇時間範圍")
        view_mode = st.radio("顯示模式", ["單日詳細", "週彙總", "月彙總"], index=0)
        selected_dates = st.multiselect("選擇日期", dates, default=dates[:5] if len(dates) >= 5 else dates)

    if not selected_dates:
        st.info("請在左側選擇要查看的日期")
        return

    if view_mode == "單日詳細":
        for date in sorted(selected_dates, reverse=True):
            df = history[date].copy()
            df.columns = [c.strip() for c in df.columns]
            col_map = {"代號": "code", "名稱": "name", "成交金額(億)": "trade_value", "漲跌幅(%)": "change_pct"}
            df = df.rename(columns=col_map)
            df["trade_value"] = pd.to_numeric(df["trade_value"], errors="coerce").fillna(0)
            df["change_pct"] = pd.to_numeric(df["change_pct"], errors="coerce").fillna(0)
            df["rank"] = range(1, len(df)+1)
            df["has_cb"] = df["code"].astype(str).isin(CB_STOCKS)

            # Prev codes = all dates earlier than this
            earlier = [d for d in dates if d < date]
            prev_c = set()
            if earlier:
                prev_df = history.get(earlier[0])
                if prev_df is not None:
                    try:
                        prev_c = set(prev_df.iloc[:, 0].astype(str).tolist())
                    except Exception:
                        pass

            with st.expander(f"📅 {date}", expanded=(date == sorted(selected_dates, reverse=True)[0])):
                render_table(df, prev_c, title="")

    elif view_mode == "週彙總":
        _render_aggregate_table(history, selected_dates, "week")
    else:
        _render_aggregate_table(history, selected_dates, "month")


def _render_aggregate_table(history, selected_dates, mode):
    """Render aggregated view by week or month."""
    all_rows = []
    for date in selected_dates:
        df = history[date].copy()
        df.columns = [c.strip() for c in df.columns]
        col_map = {"代號": "code", "名稱": "name", "成交金額(億)": "trade_value", "漲跌幅(%)": "change_pct"}
        df = df.rename(columns=col_map)
        df["date"] = date
        all_rows.append(df)

    if not all_rows:
        return

    combined = pd.concat(all_rows, ignore_index=True)
    combined["trade_value"] = pd.to_numeric(combined["trade_value"], errors="coerce").fillna(0)

    # Group by stock
    agg = combined.groupby(["code", "name"]).agg(
        avg_value=("trade_value", "mean"),
        total_value=("trade_value", "sum"),
        appearances=("trade_value", "count"),
        avg_chg=("change_pct", "mean"),
    ).reset_index()
    agg = agg.sort_values("total_value", ascending=False).head(30).reset_index(drop=True)
    agg["rank"] = range(1, len(agg)+1)
    agg["has_cb"] = agg["code"].astype(str).isin(CB_STOCKS)

    # Prev codes (earliest selected date's prev)
    sorted_sel = sorted(selected_dates)
    prev_c = set()
    dates_all = sorted(history.keys())
    for i, d in enumerate(dates_all):
        if d == sorted_sel[0] and i > 0:
            prev_df = history.get(dates_all[i-1])
            if prev_df is not None:
                try:
                    prev_c = set(prev_df.iloc[:, 0].astype(str).tolist())
                except Exception:
                    pass

    label = "週" if mode == "week" else "月"
    period = f"{sorted_sel[0]} ~ {sorted_sel[-1]}" if len(sorted_sel) > 1 else sorted_sel[0]
    st.markdown(f"#### 📊 {label}彙總排行 · {period}")

    # Stats bar
    up = (agg["avg_chg"] > 0).sum()
    dn = (agg["avg_chg"] < 0).sum()
    cols = st.columns(4)
    cols[0].metric("上榜股數", len(agg))
    cols[1].metric("平均上漲", f"{up} 支")
    cols[2].metric("平均下跌", f"{dn} 支")
    cols[3].metric("漲跌比", f"{up/len(agg)*100:.0f}%" if len(agg) else "—")

    st.markdown("""
    <div class="legend">
        <div class="legend-item"><div class="legend-dot" style="background:rgba(255,107,107,.35)"></div> 平均上漲</div>
        <div class="legend-item"><div class="legend-dot" style="background:rgba(63,185,80,.35)"></div> 平均下跌</div>
        <div class="legend-item"><div class="legend-dot" style="background:rgba(227,179,65,.35)"></div> <span style="color:var(--new-entry)">★ 新上榜</span></div>
        <div class="legend-item"><span class="badge badge-cb">CB</span> 已發行可轉債</div>
    </div>
    """, unsafe_allow_html=True)

    html = """<table class="stock-table"><thead><tr>
        <th>排名</th><th>代號</th><th>名稱</th>
        <th>平均成交(億)</th><th>累積成交(億)</th>
        <th>上榜天數</th><th>平均漲跌</th>
    </tr></thead><tbody>"""

    for _, row in agg.iterrows():
        code = str(row["code"])
        is_up = row["avg_chg"] > 0
        is_dn = row["avg_chg"] < 0
        is_new = code not in prev_c if prev_c else False
        has_cb = row.get("has_cb", code in CB_STOCKS)

        row_class = "row-new" if is_new else ("row-up" if is_up else ("row-dn" if is_dn else ""))
        new_badge = '<span class="badge badge-new">★ 新</span>' if is_new else ""
        cb_badge = '<span class="badge badge-cb">CB</span>' if has_cb else ""
        chg_html = render_change(row["avg_chg"])
        rank_html = render_rank(int(row["rank"]))

        html += f"""<tr class="{row_class}">
            <td>{rank_html}</td>
            <td><b style="font-family:'JetBrains Mono',monospace">{code}</b></td>
            <td>{row['name']}{new_badge}{cb_badge}</td>
            <td><b style="font-family:'JetBrains Mono',monospace">{row['avg_value']:,.1f}</b></td>
            <td><b style="font-family:'JetBrains Mono',monospace">{row['total_value']:,.1f}</b></td>
            <td style="text-align:center">{int(row['appearances'])}</td>
            <td>{chg_html}</td>
        </tr>"""

    html += "</tbody></table>"
    st.markdown(html, unsafe_allow_html=True)


def _show_demo_history():
    """Show demo table when no history available."""
    import random
    random.seed(42)
    demo_dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, 6)]

    html = """<table class="stock-table"><thead><tr>
        <th>排名</th><th>代號</th><th>名稱</th>
        <th>日期1</th><th>日期2</th><th>日期3</th><th>平均成交(億)</th>
    </tr></thead><tbody>"""

    stocks = [("2330","台積電"), ("2317","鴻海"), ("2454","聯發科"), ("3008","大立光"), ("2382","廣達")]
    for i, (code, name) in enumerate(stocks, 1):
        vals = [random.uniform(50, 500) for _ in range(3)]
        avg = sum(vals)/3
        pct = random.uniform(-3, 3)
        chg_html = render_change(pct)
        has_cb = code in CB_STOCKS
        cb_badge = '<span class="badge badge-cb">CB</span>' if has_cb else ""
        html += f"""<tr class="{'row-up' if pct>0 else 'row-dn'}">
            <td>{render_rank(i)}</td>
            <td><b style="font-family:'JetBrains Mono',monospace">{code}</b></td>
            <td>{name}{cb_badge}</td>
            {''.join(f'<td>{v:,.0f}</td>' for v in vals)}
            <td><b>{avg:,.0f}</b></td>
        </tr>"""
    html += "</tbody></table>"
    st.markdown(html, unsafe_allow_html=True)


def page_setup():
    st.markdown("""
    <div class="hero-header">
        <div>
            <div class="hero-title">⚙️ 設定說明</div>
            <div class="hero-subtitle">Google Sheets API 設定指引</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    ## 📋 如何設定 Google Sheets API

    ### Step 1：建立 Google Cloud 專案
    1. 前往 [Google Cloud Console](https://console.cloud.google.com/)
    2. 建立新專案
    3. 啟用 **Google Sheets API** 和 **Google Drive API**

    ### Step 2：建立服務帳戶
    1. 前往「API 與服務 → 憑證」
    2. 點選「建立憑證 → 服務帳戶」
    3. 下載 JSON 金鑰檔

    ### Step 3：設定 Streamlit Secrets
    在專案根目錄建立 `.streamlit/secrets.toml`：

    ```toml
    [gcp_service_account]
    type = "service_account"
    project_id = "your-project-id"
    private_key_id = "your-key-id"
    private_key = "-----BEGIN RSA PRIVATE KEY-----\\n...\\n-----END RSA PRIVATE KEY-----\\n"
    client_email = "your-service-account@project.iam.gserviceaccount.com"
    client_id = "your-client-id"
    auth_uri = "https://accounts.google.com/o/oauth2/auth"
    token_uri = "https://oauth2.googleapis.com/token"
    ```

    ### Step 4：共用 Google Sheets
    將服務帳戶 email 加入您的 Google Sheet 的「共用」清單（編輯者權限）。

    **目標 Sheet：** `https://docs.google.com/spreadsheets/d/1lTRxbT9iv3uRIrQ1wUFPV0LZbo8p5Qoh4a1FMGa-iU4/`

    ### 可轉債(CB)資料更新
    目前 CB 名單為內建靜態資料。如需更新，請修改 `app.py` 中的 `CB_STOCKS` 集合，
    或串接 [公開資訊觀測站](https://mops.twse.com.tw/) 的可轉債查詢 API。
    """)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    pages = {
        "📈 即時成交排行": page_realtime,
        "📊 歷史成交排行": page_history,
        "⚙️ 設定說明": page_setup,
    }

    with st.sidebar:
        st.markdown("""
        <div style="padding:16px 0 8px;border-bottom:1px solid #30363d;margin-bottom:16px">
            <div style="font-size:18px;font-weight:700;color:#e6edf3">台股成交追蹤</div>
            <div style="font-size:11px;color:#8b949e;margin-top:2px">Taiwan Volume Tracker</div>
        </div>
        """, unsafe_allow_html=True)
        page = st.radio("導覽", list(pages.keys()), label_visibility="collapsed")
        st.markdown("---")
        st.markdown("""
        <div style="font-size:11px;color:#8b949e;line-height:1.6">
        資料來源：台灣證券交易所<br>
        更新頻率：盤中每 30-60 秒<br>
        盤後：13:30 後靜態資料<br><br>
        <span style="color:#e3b341">★ 新上榜</span>：與前一日比較<br>
        <span style="color:#a371f7">CB</span>：已發行可轉債
        </div>
        """, unsafe_allow_html=True)

    pages[page]()


if __name__ == "__main__":
    main()
