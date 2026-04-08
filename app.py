import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timezone, timedelta
import re

# 設定網頁佈局
st.set_page_config(page_title="台股即時成交金額監控", layout="wide", page_icon="📈")

# 設定台灣時區
tw_tz = timezone(timedelta(hours=8))

# 初始化 Session State
if 'prev_top30' not in st.session_state:
    st.session_state.prev_top30 = set()
if 'http_session' not in st.session_state:
    st.session_state.http_session = requests.Session()

# 輔助：安全轉型數字
def safe_float(text):
    cleaned = re.sub(r'[^\d.-]', '', text)
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0

# ----------------- 核心爬蟲函式 -----------------
@st.cache_data(ttl=30, show_spinner=False) # 增加快取存活時間
def get_yahoo_data():
    # 加上隨機參數避免快取，但降低請求頻率
    url = f"https://tw.stock.yahoo.com/rank/turnover?v={int(time.time() / 30)}" 
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        response = st.session_state.http_session.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.find_all('li', class_='List(n)')
        
        data = []
        for row in rows[:30]:
            name_block = row.find('div', class_='Lh(20px)')
            ticker_block = row.find('span', class_='C(#979ba7)')
            cols = row.find_all('div', class_='Fxg(1)')
            
            if not (name_block and ticker_block and cols): continue
            
            texts = [c.text.strip() for c in cols]
            stock_name = name_block.text.strip()
            stock_id = ticker_block.text.replace('.TW', '').replace('.TWO', '').strip()
            price = safe_float(texts[0])
            change_pct = safe_float(texts[2])
            
            # 成交值解析 (處理「億」字)
            turnover_text = texts[-1]
            turnover = safe_float(turnover_text)
            if '萬' in turnover_text: turnover /= 10000
            
            data.append([stock_id, stock_name, price, change_pct, turnover])
            
        return pd.DataFrame(data, columns=['股票代號', '股票名稱', '目前股價', '漲幅(%)', '成交金額(億)'])
    except Exception as e:
        return pd.DataFrame()

# ----------------- UI 介面 -----------------
st.sidebar.header("⚙️ 監控設定")
# 讓使用者自訂頻率，預設 60 秒更安全
refresh_interval = st.sidebar.slider("自動更新頻率 (秒)", min_value=30, max_value=300, value=60, step=30)
auto_refresh = st.sidebar.checkbox("開啟自動刷新", value=True)

st.title("🚀 台股成交值 TOP30 即時監控")

df = get_yahoo_data()
current_time = datetime.now(tw_tz).strftime('%Y-%m-%d %H:%M:%S')

if not df.empty:
    # 漲跌統計
    up = len(df[df['漲幅(%)'] > 0])
    down = len(df[df['漲幅(%)'] < 0])
    ratio = (up / len(df)) * 100
    
    m1, m2, m3 = st.columns(3)
    m1.metric("多方 (上漲)", f"{up} 檔", delta=f"{ratio:.1f}%", delta_color="normal")
    m2.metric("空方 (下跌)", f"{down} 檔", delta=f"{100-ratio:.1f}%", delta_color="inverse")
    m3.metric("更新時間", current_time.split(' ')[1])

    # 樣式定義
    def apply_style(row):
        styles = [''] * len(row)
        # 1. 紅綠色邏輯
        c_idx = df.columns.get_loc('漲幅(%)')
        if row['漲幅(%)'] > 0: styles[c_idx] = 'color: #ff4b4b;'
        elif row['漲幅(%)'] < 0: styles[c_idx] = 'color: #00fa9a;'
        
        # 2. 新進榜邏輯 (黃色底)
        if len(st.session_state.prev_top30) > 0 and row['股票代號'] not in st.session_state.prev_top30:
            styles = [s + 'background-color: rgba(255, 255, 0, 0.1);' for s in styles]
        return styles

    st.dataframe(
        df.style.apply(apply_style, axis=1).format({'目前股價': '{:.2f}', '漲幅(%)': '{:+.2f}%', '成交金額(億)': '{:.2f}'}),
        use_container_width=True, height=800
    )
    
    # 存入這次的名單
    st.session_state.prev_top30 = set(df['股票代號'])
else:
    st.error("暫時抓不到資料，請檢查網路或稍後再試。")

if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
