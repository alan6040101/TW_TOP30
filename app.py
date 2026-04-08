import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timezone, timedelta
import re
import os

# 設定網頁佈局
st.set_page_config(page_title="台股成交金額 TOP30 監控系統", layout="wide", page_icon="🔥")

# 1. 設定台灣時區與檔案路徑
tw_tz = timezone(timedelta(hours=8))
HISTORY_FILE = "history_top30.csv"

# 2. 建立 requests Session (連線池)
if 'http_session' not in st.session_state:
    st.session_state.http_session = requests.Session()
    st.session_state.http_session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })

def safe_float(text):
    cleaned = re.sub(r'[^\d.-]', '', text) 
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0

# ----------------- 資料獲取與儲存區塊 -----------------

@st.cache_data(ttl=180, show_spinner=False)
def get_yahoo_turnover_top30():
    url = f"https://tw.stock.yahoo.com/rank/turnover?v={int(time.time())}"
    try:
        response = st.session_state.http_session.get(url, timeout=10)
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
    """將今日最新資料寫入 CSV，並回傳完整的歷史資料"""
    today_str = datetime.now(tw_tz).strftime('%Y-%m-%d')
    
    # 確保當前資料有日期欄位
    if not df_current.empty:
        df_current_save = df_current.copy()
        df_current_save['日期'] = today_str
        
        if os.path.exists(HISTORY_FILE):
            df_hist = pd.read_csv(HISTORY_FILE)
            # 移除舊的「今日」資料，換成最新抓到的「今日」資料
            df_hist = df_hist[df_hist['日期'] != today_str]
            df_hist = pd.concat([df_hist, df_current_save], ignore_index=True)
        else:
            df_hist = df_current_save
            
        # 存檔
        df_hist.to_csv(HISTORY_FILE, index=False)
        return df_hist
    else:
        # 若當前沒抓到資料，僅讀取歷史紀錄
        if os.path.exists(HISTORY_FILE):
            return pd.read_csv(HISTORY_FILE)
        else:
            return pd.DataFrame()

# ----------------- 樣式處理區塊 -----------------

def style_realtime_dataframe(df, prev_day_set):
    """即時分頁的樣式 (包含所有欄位)"""
    def row_style(row):
        styles = [''] * len(row)
        change_idx = df.columns.get_loc('漲幅(%)')
        if row['漲幅(%)'] > 0:
            styles[change_idx] = 'color: #ff4b4b; font-weight: bold;' 
        elif row['漲幅(%)'] < 0:
            styles[change_idx] = 'color: #00fa9a; font-weight: bold;' 
            
        if len(prev_day_set) > 0 and row['股票代號'] not in prev_day_set:
            styles = [s + 'background-color: rgba(255, 255, 0, 0.2);' for s in styles]
        return styles

    return df.style.apply(row_style, axis=1)\
                   .format({'目前股價': '{:.2f}', '漲幅(%)': '{:+.2f}', '成交金額(億)': '{:.2f}'})

def create_5days_history_styler(df_hist):
    """產生歷史 5 日橫向比較表的專屬 DataFrame 與樣式"""
    dates = sorted(df_hist['日期'].unique())
    recent_dates = dates[-5:] # 取最近 5 天
    
    display_dict = {}
    style_dict = {}

    for i, current_date in enumerate(recent_dates):
        # 1. 抓取該日資料
        df_day = df_hist[df_hist['日期'] == current_date].copy()
        df_day = df_day.sort_values(by='成交金額(億)', ascending=False).head(30).reset_index(drop=True)
        
        # 2. 計算上漲比例作為表頭
        up_count = len(df_day[df_day['漲幅(%)'] > 0])
        up_ratio = (up_count / len(df_day)) * 100 if len(df_day) > 0 else 0
        col_header = f"{current_date} (上漲 {up_ratio:.0f}%)"
        
        # 3. 取得「該日的前一天」用來比較新進榜
        prev_day_set = set()
        if i > 0:
            prev_date = recent_dates[i-1]
            prev_day_set = set(df_hist[df_hist['日期'] == prev_date]['股票代號'])
        else:
            # 若為畫面上的第一天，去歷史紀錄找更前面的一天
            prev_idx = dates.index(current_date) - 1
            if prev_idx >= 0:
                prev_date = dates[prev_idx]
                prev_day_set = set(df_hist[df_hist['日期'] == prev_date]['股票代號'])
                
        # 4. 建立當日的顯示與樣式欄位
        col_display = []
        col_style = []
        
        for _, row in df_day.iterrows():
            name = row['股票名稱']
            change = row['漲幅(%)']
            col_display.append(name)
            
            # 設定 CSS
            css = "text-align: center; "
            if change > 0:
                css += "color: #ff4b4b; font-weight: bold; "
            elif change < 0:
                css += "color: #00fa9a; font-weight: bold; "
                
            if row['股票代號'] not in prev_day_set and len(prev_day_set) > 0:
                css += "background-color: rgba(255, 255, 0, 0.2); "
                
            col_style.append(css)
            
        # 補齊 30 列 (若某天資料不足)
        while len(col_display) < 30:
            col_display.append("")
            col_style.append("")
            
        display_dict[col_header] = col_display
        style_dict[col_header] = col_style

    df_display = pd.DataFrame(display_dict)
    df_display.index = [f"第 {idx} 名" for idx in range(1, 31)] # 把索引改成 1~30 名
    df_style = pd.DataFrame(style_dict, index=df_display.index)

    # 將樣式對應套用
    return df_display.style.apply(lambda _: df_style, axis=None)

# ----------------- 主程式 UI -----------------
st.title("📈 台股成交金額 TOP30 監控系統")

# 獲取今日即時資料並更新資料庫
df_current_top30 = get_yahoo_turnover_top30()
df_history_all = update_and_load_history(df_current_top30)
current_time_str = datetime.now(tw_tz).strftime('%H:%M:%S')

# 找出「昨日」的名單供即時頁面比對
today_str = datetime.now(tw_tz).strftime('%Y-%m-%d')
yesterday_set = set()
if not df_history_all.empty:
    past_dates = sorted(df_history_all[df_history_all['日期'] < today_str]['日期'].unique())
    if past_dates:
        latest_past_date = past_dates[-1]
        yesterday_set = set(df_history_all[df_history_all['日期'] == latest_past_date]['股票代號'])

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
        st.info("💡 整列顯示**黃色底色**代表與「前一個交易日」相比，今天新擠進前 30 名的強勢股。")

        styled_df = style_realtime_dataframe(df_current_top30, yesterday_set)
        st.dataframe(styled_df, use_container_width=True, height=1050)
    else:
        st.warning("目前無法取得即時資料。")

# ==================== 分頁 2：歷史排行 ====================
with tab2:
    st.subheader("📅 近 5 日成交金額 TOP30 資金輪動表")
    st.caption("說明：此表僅顯示股票名稱。**紅色**為當地上漲，**綠色**為當日下跌，**黃底**代表相對前一日的新進榜股票。欄位名稱包含當天的上漲比例。")
    
    if not df_history_all.empty:
        # 使用自訂的 Styler 產生 5 日比較表
        styled_hist_df = create_5days_history_styler(df_history_all)
        st.dataframe(styled_hist_df, use_container_width=True, height=1100)
    else:
        st.info("目前還沒有足夠的歷史資料，資料將從今天開始自動累積！")

# ==================== 自動更新邏輯 ====================
if 'auto_refresh' in st.session_state and st.session_state.auto_refresh:
    time.sleep(180) 
    st.rerun()
