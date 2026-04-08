import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timezone, timedelta
import re

# 設定網頁佈局
st.set_page_config(page_title="台股即時成交金額 TOP30", layout="wide", page_icon="🔥")

# 1. 設定台灣時區 (UTC+8)
tw_tz = timezone(timedelta(hours=8))

# 2. 建立 requests Session (連線池)，重複利用連線可大幅提升爬取速度
if 'http_session' not in st.session_state:
    st.session_state.http_session = requests.Session()
    st.session_state.http_session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7"
    })

# 安全轉型數字的輔助函式 (過濾掉奇奇怪怪的符號)
def safe_float(text):
    cleaned = re.sub(r'[^\d.-]', '', text) # 只保留數字、小數點和負號
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0

# ----------------- 優化版 Yahoo 爬蟲 -----------------
# 將快取有效時間設為 10 秒，且隱藏讀取動畫(show_spinner=False)避免畫面一直閃爍
@st.cache_data(ttl=10, show_spinner=False)
def get_yahoo_turnover_top30():
    # 3. 在網址加入隨機時間戳記 (?v=...)，強制破解 Yahoo 的舊快取，保證拿到最新資料
    url = f"https://tw.stock.yahoo.com/rank/turnover?v={int(time.time())}"
    
    try:
        response = st.session_state.http_session.get(url, timeout=5)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = soup.find_all('li', class_='List(n)')
        data = []
        for row in rows[:30]: 
            cols = row.find_all('div', class_='Fxg(1)') 
            if not cols: continue
                
            try:
                # 解析名稱與代號
                name_block = row.find('div', class_='Lh(20px)')
                ticker_block = row.find('span', class_='C(#979ba7)')
                stock_name = name_block.text.strip() if name_block else "未知"
                stock_id = ticker_block.text.replace('.TW', '').replace('.TWO', '').strip() if ticker_block else "未知"
                
                prices_texts = [c.text.strip() for c in cols]
                
                # 目前股價
                price = safe_float(prices_texts[0])
                
                # 漲幅 (%)
                change_pct = safe_float(prices_texts[2])
                    
                # 成交金額(億)
                turnover_str = prices_texts[-1]
                if '億' in turnover_str:
                    turnover = safe_float(turnover_str)
                elif '萬' in turnover_str:
                    turnover = safe_float(turnover_str) / 10000
                else:
                    turnover = safe_float(turnover_str)
                
                data.append([stock_id, stock_name, price, change_pct, turnover])
                
            except Exception as e:
                continue
                
        return pd.DataFrame(data, columns=['股票代號', '股票名稱', '目前股價', '漲幅(%)', '成交金額(億)'])
        
    except Exception as e:
        return pd.DataFrame(columns=['股票代號', '股票名稱', '目前股價', '漲幅(%)', '成交金額(億)'])

# ----------------- 樣式處理 -----------------
def style_dataframe(df, prev_top30_set):
    def row_style(row):
        styles = [''] * len(row)
        change_idx = df.columns.get_loc('漲幅(%)')
        if row['漲幅(%)'] > 0:
            styles[change_idx] = 'color: #ff4b4b; font-weight: bold;' 
        elif row['漲幅(%)'] < 0:
            styles[change_idx] = 'color: #00fa9a; font-weight: bold;' 
            
        if len(prev_top30_set) > 0 and row['股票代號'] not in prev_top30_set:
            styles = [s + 'background-color: rgba(255, 255, 0, 0.2);' for s in styles]
        return styles

    return df.style.apply(row_style, axis=1)\
                   .format({'目前股價': '{:.2f}', '漲幅(%)': '{:+.2f}', '成交金額(億)': '{:.2f}'})

# ----------------- 主程式 UI -----------------
st.title("🔥 台股即時成交金額 TOP30 (極速版)")

if 'prev_top30' not in st.session_state:
    st.session_state.prev_top30 = set()

col_ctrl1, col_ctrl2 = st.columns([1, 4])
with col_ctrl1:
    auto_refresh = st.checkbox("開啟自動更新 (每 10 秒)", value=True)
with col_ctrl2:
    # 移除會卡頓的 cache_clear，改用自然的重新整理
    if st.button("🔄 手動強制更新"):
        st.rerun()

# 取得資料
df_current_top30 = get_yahoo_turnover_top30()
current_time_str = datetime.now(tw_tz).strftime('%H:%M:%S')

if not df_current_top30.empty:
    current_top30_set = set(df_current_top30['股票代號'])

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
    st.subheader(f"📊 即時成交金額排名 (台灣時間更新: {current_time_str})")

    styled_df = style_dataframe(df_current_top30, st.session_state.prev_top30)
    st.dataframe(styled_df, use_container_width=True, height=1050)

    st.session_state.prev_top30 = current_top30_set
else:
    st.warning("目前無法取得資料，可能網路不穩或正在非交易時段。")

# 自動更新邏輯
if auto_refresh:
    time.sleep(10) 
    st.rerun()
