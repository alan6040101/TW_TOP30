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
# 你的 Google 試算表網址
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1lTRxbT9iv3uRIrQ1wUFPV0LZbo8p5Qoh4a1FMGa-iU4/edit?usp=sharing"

# ----------------- [修正] 真實可轉債(CB)動態爬蟲 (避開櫃買阻擋) -----------------
@st.cache_data(ttl=3600, show_spinner=False) 
def get_real_cb_stock_ids():
    """
    改從「嗨投資 (HiStock)」與「MoneyDJ」等較不易阻擋爬蟲的網站
    爬取目前所有交易中的可轉債，避免櫃買中心(TPEx)嚴格的阻擋機制。
    """
    cb_stock_ids = set()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
    
    try:
        # 優先來源：HiStock 嗨投資
        url_histock = "https://histock.tw/stock/cb.aspx"
        res = requests.get(url_histock, headers=headers, timeout=10)
        if res.status_code == 200:
            # 尋找類似 /stock/23177 的連結，抓出 5 碼代號
            cb_codes = re.findall(r'/stock/(\d{5})', res.text)
            for code in cb_codes:
                cb_stock_ids.add(code[:4]) # 前 4 碼為標的股票代號
                
        # 如果 HiStock 沒抓到，啟用備用來源：MoneyDJ
        if not cb_stock_ids:
            url_moneydj = "https://www.moneydj.com/z/zg/zgc/zgc_0.djhtm"
            res2 = requests.get(url_moneydj, headers=headers, timeout=10)
            if res2.status_code == 200:
                # 尋找類似 zca_23177.djhtm 的連結，抓出 5 碼代號
                cb_codes2 = re.findall(r'zca_(\d{5})\.djhtm', res2.text)
                for code in cb_codes2:
                    cb_stock_ids.add(code[:4])

        return cb_stock_ids
    except Exception as e:
        # 即使發生錯誤，也不會報 json 錯，只會安靜地印出提示
        st.sidebar.warning(f"⚠️ CB 名單更新失敗，暫時略過標註。錯誤: {e}")
        return set()

# ----------------- 資料獲取與儲存區塊 -----------------

def safe_float(text):
    cleaned = re.sub(r'[^\d.-]', '', text) 
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0

@st.cache_data(ttl=180, show_spinner=False)
def get_yahoo_turnover_top30():
    # 取得真實的 CB 股票代號名單
    real_cb_ids = get_real_cb_stock_ids()
    
    url = f"https://tw.stock.yahoo.com/rank/turnover?v={int(time.time())}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
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
                
                # 使用動態爬取的真實名單來判斷是否有 CB
                if stock_id in real_cb_ids:
                    stock_name = f"{stock_name} (CB)"

                prices_texts = [c.text.strip() for c in cols]
                price = safe_float(prices_texts[0])
                change_pct = safe_float(prices_texts[2])
                    
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
    """雲端儲存邏輯 (保持不變)"""
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
            
            if not df_current_save.empty:
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

# ----------------- 樣式處理區塊 (保持不變) -----------------

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

def create_5days_history_styler(df_hist):
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
            name = row['股票名稱']
            change = row['漲幅(%)']
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

# 獲取今日即時資料並更新資料庫
df_current_top30 = get_yahoo_turnover_top30()
df_history_all = update_and_load_history(df_current_top30)
current_time_str = datetime.now(tw_tz).strftime('%H:%M:%S')

# 找出「昨日」的名單供即時頁面比對
today_str = datetime.now(tw_tz).strftime('%Y-%m-%d')
yesterday_set = set()
if not df_history_all.empty and '日期' in df_history_all.columns:
    past_dates = sorted(df_history_all[df_history_all['日期'] < today_str]['日期'].unique())
    if past_dates:
        latest_past_date = past_dates[-1]
        yesterday_set = set(df_history_all[df_history_all['日期'] == latest_past_date]['股票代號'].astype(str))

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
        st.subheader(f"📊 今日即時排行榜 (最後更新: {current_time_str})")
        st.info("💡 整列顯示**黃色底色**代表新進榜股票。名字後方帶有 **(CB)** 標籤代表由爬蟲偵測該公司目前有發行可轉債。")

        styled_df = style_realtime_dataframe(df_current_top30, yesterday_set)
        st.dataframe(styled_df, use_container_width=True, height=1050)
    else:
        st.warning("目前無法取得即時資料。可能網路中斷或非開盤時段。")

# ==================== 分頁 2：歷史排行 ====================
with tab2:
    st.subheader("📅 近 5 日成交金額 TOP30 資金輪動表")
    st.caption("說明：此表僅顯示股票名稱。**紅色**為當日上漲，**綠色**為當日下跌，**黃底**為新進榜股票。帶有 **(CB)** 代表具可轉債題材。")
    
    if not df_history_all.empty:
        styled_hist_df = create_5days_history_styler(df_history_all)
        st.dataframe(styled_hist_df, use_container_width=True, height=1100)
    else:
        st.info("資料庫目前為空，資料將從今天開始自動累積！")

# ==================== 自動更新邏輯 ====================
if 'auto_refresh' in st.session_state and st.session_state.auto_refresh:
    time.sleep(180) 
    st.rerun()
