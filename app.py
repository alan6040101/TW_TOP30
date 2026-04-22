import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timezone, timedelta
import re
import gspread
from google.oauth2.service_account import Credentials

# ----------------- 絕對防彈：套件載入保護網 -----------------
try:
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

# 設定網頁佈局
st.set_page_config(page_title="台股成交金額 TOP30 監控系統", layout="wide", page_icon="🔥")

# 1. 基本參數設定
tw_tz = timezone(timedelta(hours=8))
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1lTRxbT9iv3uRIrQ1wUFPV0LZbo8p5Qoh4a1FMGa-iU4/edit?usp=sharing"

# 判斷是否為有效的開盤紀錄時間 (排除週末，且早上 9 點後才算當日盤)
now_tw = datetime.now(tw_tz)
is_weekend = now_tw.weekday() >= 5  # 5:週六, 6:週日
is_trading_time = now_tw.hour >= 9
IS_VALID_TRADING_DAY = (not is_weekend) and is_trading_time

# ----------------- 真實可轉債(CB)動態爬蟲 (指定來源 TheFew) -----------------
@st.cache_data(ttl=3600, show_spinner=False) 
def get_real_cb_stock_ids():
    """
    從指定的 TheFew 網站抓取可轉債名單，並保留官方 API 備援。
    """
    cb_stock_ids = set()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }
    
    try:
        # 優先來源：使用者指定的 https://thefew.tw/cb
        url_thefew = "https://thefew.tw/cb"
        res = requests.get(url_thefew, headers=headers, timeout=15)
        if res.status_code == 200:
            # 抓取 5 碼可轉債代碼 (例如 23177 -> 取前 4 碼 2317)
            found_5_digits = re.findall(r'(?<!\d)([1-9]\d{3}[1-9])(?!\d)', res.text)
            for code in found_5_digits:
                cb_stock_ids.add(code[:4])
                
            # 抓取網頁中直接標示的 4 碼現股代碼
            found_4_digits = re.findall(r'(?<!\d)([1-9]\d{3})(?!\d)', res.text)
            for code in found_4_digits:
                if code not in ['2023', '2024', '2025', '2026', '2027']:
                    cb_stock_ids.add(code)
    except Exception as e:
        st.sidebar.warning(f"⚠️ TheFew 來源獲取超時，切換備援。")

    # 備用來源：官方櫃買中心 API 
    if not cb_stock_ids:
        try:
            tpex_url = "https://www.tpex.org.tw/web/bond/tradeinfo/cb/cb_daily_qut_result.php?l=zh-tw&o=json"
            res_tpex = requests.get(tpex_url, headers=headers, timeout=10)
            if res_tpex.status_code == 200:
                data = res_tpex.json()
                if "aaData" in data:
                    for row in data["aaData"]:
                        cb_code = str(row[1])
                        if len(cb_code) >= 4:
                            cb_stock_ids.add(cb_code[:4])
        except Exception:
            pass

    return cb_stock_ids

# ----------------- 資料獲取與儲存區塊 -----------------

def safe_float(text):
    cleaned = re.sub(r'[^\d.-]', '', text) 
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0

@st.cache_data(ttl=180, show_spinner=False)
def get_yahoo_turnover_top30():
    real_cb_ids = get_real_cb_stock_ids()
    
    url = f"https://tw.stock.yahoo.com/rank/turnover?v={int(time.time())}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = soup.find_all('li', class_='List(n)')
        data = []
        for row in rows[:30]: 
            cols = row.find_all('div', class_='Fxg(1)') 
            if not cols: continue
            try:
                name_block = row.find('div', class_='Lh(20px)')
                ticker_block = row.find('span', class_='C(#979ba7)')
                stock_name = name_block.text.strip() if name_block else "未知"
                stock_id = ticker_block.text.replace('.TW', '').replace('.TWO', '').strip() if ticker_block else "未知"
                
                # 標記 CB
                if stock_id in real_cb_ids:
                    stock_name = f"{stock_name} (CB)"

                prices_texts = [c.text.strip() for c in cols]
                price = safe_float(prices_texts[0])

                # ---- 修正：從 HTML class 判斷漲跌方向 ----
                # Yahoo 用顏色 class 區分：紅色(#ff333a)=上漲，綠色(#459b16)=下跌
                change_col = cols[2] if len(cols) > 2 else None
                change_pct_raw = safe_float(prices_texts[2])
                if change_col:
                    col_html = str(change_col)
                    if 'C(#ff333a)' in col_html:
                        change_pct = abs(change_pct_raw)    # 上漲為正
                    elif 'C(#459b16)' in col_html:
                        change_pct = -abs(change_pct_raw)   # 下跌為負
                    else:
                        change_pct = change_pct_raw         # 平盤
                else:
                    change_pct = change_pct_raw
                # ---- 修正結束 ----

                turnover_str = prices_texts[-1]
                if '億' in turnover_str: turnover = safe_float(turnover_str)
                elif '萬' in turnover_str: turnover = safe_float(turnover_str) / 10000
                else: turnover = safe_float(turnover_str)
                
                data.append([stock_id, stock_name, price, change_pct, turnover])
            except Exception:
                continue
        return pd.DataFrame(data, columns=['股票代號', '股票名稱', '目前股價', '漲幅(%)', '成交金額(億)'])
    except Exception as e:
        return pd.DataFrame(columns=['股票代號', '股票名稱', '目前股價', '漲幅(%)', '成交金額(億)'])

def update_and_load_history(df_current):
    """雲端儲存邏輯"""
    today_str = datetime.now(tw_tz).strftime('%Y-%m-%d')
    df_current_save = df_current.copy() if not df_current.empty else pd.DataFrame()
    
    if not df_current_save.empty:
        df_current_save['日期'] = today_str
        df_current_save['股票代號'] = df_current_save['股票代號'].astype(str)

    try:
        if GSPREAD_AVAILABLE and hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
            scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
            gc = gspread.authorize(creds)
            sh = gc.open_by_url(GOOGLE_SHEET_URL)
            worksheet = sh.sheet1
            
            records = worksheet.get_all_records()
            df_hist = pd.DataFrame(records) if records else pd.DataFrame(columns=['股票代號', '股票名稱', '目前股價', '漲幅(%)', '成交金額(億)', '日期'])
            
            if not df_hist.empty and '股票代號' in df_hist.columns:
                df_hist['股票代號'] = df_hist['股票代號'].astype(str)
            
            if not df_current_save.empty and IS_VALID_TRADING_DAY:
                if not df_hist.empty and '日期' in df_hist.columns:
                    df_hist = df_hist[df_hist['日期'] != today_str]
                df_hist = pd.concat([df_hist, df_current_save], ignore_index=True)
                
                data_to_upload = [df_hist.columns.values.tolist()] + df_hist.values.tolist()
                worksheet.clear()
                worksheet.update(values=data_to_upload, range_name='A1')
                
            return df_hist
            
    except Exception as e:
        st.sidebar.error(f"❌ Google Sheets 連線失敗: {e}")
        
    return pd.DataFrame()

# ----------------- 樣式處理區塊 -----------------

def style_realtime_dataframe(df, prev_day_set):
    def row_style(row):
        styles = [''] * len(row)
        change_idx = df.columns.get_loc('漲幅(%)')
        if row['漲幅(%)'] > 0:
            styles[change_idx] = 'color: #ff4b4b; font-weight: bold;' 
        elif row['漲幅(%)'] < 0:
            styles[change_idx] = 'color: #00fa9a; font-weight: bold;' 
            
        if len(prev_day_set) > 0 and str(row['股票代號']) not in prev_day_set:
            styles = [s + 'background-color: rgba(255, 255, 0, 0.2);' for s in styles]
        return styles

    return df.style.apply(row_style, axis=1)\
                   .format({'目前股價': '{:.2f}', '漲幅(%)': '{:+.2f}', '成交金額(億)': '{:.2f}'})

# [修正]: 傳入 real_cb_ids 來動態補上歷史資料的 CB 標籤
def create_5days_history_styler(df_hist, real_cb_ids):
    dates = sorted(df_hist['日期'].unique())
    recent_dates = dates[-5:] 
    
    display_dict = {}
    style_dict = {}

    for i, current_date in enumerate(recent_dates):
        df_day = df_hist[df_hist['日期'] == current_date].copy()
        df_day = df_day.sort_values(by='成交金額(億)', ascending=False).head(30).reset_index(drop=True)
        
        up_count = len(df_day[df_day['漲幅(%)'] > 0])
        up_ratio = (up_count / len(df_day)) * 100 if len(df_day) > 0 else 0
        col_header = f"{current_date} (上漲 {up_ratio:.0f}%)"
        
        prev_day_set = set()
        if i > 0:
            prev_date = recent_dates[i-1]
            prev_day_set = set(df_hist[df_hist['日期'] == prev_date]['股票代號'].astype(str))
        else:
            prev_idx = dates.index(current_date) - 1
            if prev_idx >= 0:
                prev_date = dates[prev_idx]
                prev_day_set = set(df_hist[df_hist['日期'] == prev_date]['股票代號'].astype(str))
                
        col_display = []
        col_style = []
        
        for _, row in df_day.iterrows():
            name = str(row['股票名稱'])
            stock_id = str(row['股票代號'])
            change = row['漲幅(%)']
            
            # [修正]: 動態為歷史資料補上 (CB) 標記，並防止重複標記
            if stock_id in real_cb_ids and "(CB)" not in name:
                name = f"{name} (CB)"
                
            col_display.append(name)
            
            css = "text-align: center; "
            if change > 0:
                css += "color: #ff4b4b; font-weight: bold; "
            elif change < 0:
                css += "color: #00fa9a; font-weight: bold; "
                
            if str(row['股票代號']) not in prev_day_set and len(prev_day_set) > 0:
                css += "background-color: rgba(255, 255, 0, 0.2); "
                
            col_style.append(css)
            
        while len(col_display) < 30:
            col_display.append("")
            col_style.append("")
            
        display_dict[col_header] = col_display
        style_dict[col_header] = col_style

    df_display = pd.DataFrame(display_dict)
    if not df_display.empty:
        df_display.index = [f"第 {idx} 名" for idx in range(1, 31)] 
    df_style = pd.DataFrame(style_dict, index=df_display.index)

    return df_display.style.apply(lambda _: df_style, axis=None)

# ----------------- 主程式 UI -----------------
if not GSPREAD_AVAILABLE:
    st.sidebar.error("⚠️ 尚未安裝 Google 套件。請在終端機執行 `pip install gspread google-auth`。")

st.title("📈 台股成交金額 TOP30 雲端監控系統")

if IS_VALID_TRADING_DAY:
    st.success("🟢 目前狀態：交易日開盤時段 (資料自動紀錄更新中)")
else:
    st.warning("⏸️ 目前狀態：非交易時間或週末 (顯示最近一個交易日資料，不更新資料庫)")

if IS_VALID_TRADING_DAY:
    df_current_top30 = get_yahoo_turnover_top30()
    df_history_all = update_and_load_history(df_current_top30)
else:
    df_history_all = update_and_load_history(pd.DataFrame())
    df_current_top30 = pd.DataFrame()

current_time_str = datetime.now(tw_tz).strftime('%H:%M:%S')

today_str = datetime.now(tw_tz).strftime('%Y-%m-%d')
display_date_str = today_str 
yesterday_set = set()
latest_past_date = "無紀錄"

if not df_history_all.empty and '日期' in df_history_all.columns:
    all_dates = sorted(df_history_all['日期'].unique())
    
    if IS_VALID_TRADING_DAY:
        past_dates = [d for d in all_dates if d < today_str]
        if past_dates:
            latest_past_date = past_dates[-1]
            yesterday_set = set(df_history_all[df_history_all['日期'] == latest_past_date]['股票代號'].astype(str))
    else:
        if len(all_dates) >= 1:
            display_date_str = all_dates[-1]
            df_current_top30 = df_history_all[df_history_all['日期'] == display_date_str].copy()
            if '日期' in df_current_top30.columns:
                df_current_top30 = df_current_top30.drop(columns=['日期'])
            
            if len(all_dates) >= 2:
                latest_past_date = all_dates[-2]
                yesterday_set = set(df_history_all[df_history_all['日期'] == latest_past_date]['股票代號'].astype(str))
        else:
            df_current_top30 = get_yahoo_turnover_top30()

tab1, tab2 = st.tabs(["🔥 即時成交金額排行", "📅 歷史 5 日趨勢表"])

# ==================== 分頁 1：即時排行 ====================
with tab1:
    col_ctrl1, col_ctrl2 = st.columns([2, 3])
    with col_ctrl1:
        auto_refresh = st.checkbox("開啟自動更新 (每 3 分鐘)", value=True, key="auto_refresh")
    with col_ctrl2:
        if st.button("🔄 手動重新整理"):
            st.rerun()

    if not df_current_top30.empty:
        up_count = len(df_current_top30[df_current_top30['漲幅(%)'] > 0])
        down_count = len(df_current_top30[df_current_top30['漲幅(%)'] < 0])
        flat_count = len(df_current_top30[df_current_top30['漲幅(%)'] == 0])
        up_ratio = (up_count / len(df_current_top30)) * 100 if len(df_current_top30) > 0 else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("🔝 TOP30 上漲檔數", f"{up_count} 檔")
        col2.metric("📉 TOP30 下跌檔數", f"{down_count} 檔")
        col3.metric("➖ TOP30 平盤檔數", f"{flat_count} 檔")
        col4.metric("🔥 多方勢力 (上漲比例)", f"{up_ratio:.1f} %")

        st.markdown("---")
        if IS_VALID_TRADING_DAY:
            st.subheader(f"📊 今日即時排行榜 (最後更新: {current_time_str})")
        else:
            st.subheader(f"📊 最近交易日 ({display_date_str}) 排行榜 (非交易時段)")
            
        st.info(f"💡 整列顯示**黃色底色**代表與 **前一個開盤日 ({latest_past_date})** 相比的新進榜股票。名字後方帶有 **(CB)** 標籤代表具備可轉債題材。")

        styled_df = style_realtime_dataframe(df_current_top30, yesterday_set)
        st.dataframe(styled_df, use_container_width=True, height=1050)
    else:
        st.warning("目前無法取得資料。請確認資料庫是否為空，或網路是否正常。")

# ==================== 分頁 2：歷史排行 ====================
with tab2:
    st.subheader("📅 近 5 日成交金額 TOP30 資金輪動表")
    st.caption("說明：此表僅顯示股票名稱。**紅色**為當日上漲，**綠色**為當日下跌，**黃底**為新進榜股票。帶有 **(CB)** 代表具可轉債題材。")
    
    if not df_history_all.empty:
        # [修正]: 抓取最新的 CB 名單，並傳入歷史圖表函式中進行比對
        real_cb_ids = get_real_cb_stock_ids()
        styled_hist_df = create_5days_history_styler(df_history_all, real_cb_ids)
        st.dataframe(styled_hist_df, use_container_width=True, height=1100)
    else:
        st.info("資料庫目前為空，資料將在開盤日自動累積！")

# ==================== 自動更新邏輯 ====================
if 'auto_refresh' in st.session_state and st.session_state.auto_refresh:
    time.sleep(180) 
    st.rerun()
