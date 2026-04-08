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

# 2. 建立 requests Session (連線池)，重複利用連線提升效率
if 'http_session' not in st.session_state:
    st.session_state.http_session = requests.Session()
    st.session_state.http_session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7"
    })

# 安全轉型數字的輔助函式
def safe_float(text):
    cleaned = re.sub(r'[^\d.-]', '', text) 
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0

# ----------------- Yahoo 爬蟲區塊 -----------------
# 將快取有效時間 (ttl) 改為 180 秒 (3分鐘)
# 這樣即使手動重新整理，3 分鐘內也會從快取抓資料，保護 IP
@st.cache_data(ttl=180, show_spinner=False)
def get_yahoo_turnover_top30():
    # 加入隨機時間戳記，確保拿到最新資料
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
                if '億' in turnover_str:
                    turnover = safe_float(turnover_str)
                elif '萬' in turnover_str:
                    turnover = safe_float(turnover_str) / 10000
                else:
                    turnover = safe_float(turnover_str)
                
                data.append([stock_id, stock_name, price, change_pct, turnover])
            except Exception:
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
st.title("📈 台股成交金額 TOP30 監控 (安全更新版)")

if 'prev_top30' not in st.session_state:
    st.session_state.prev_top30 = set()

col_ctrl1, col_ctrl2 = st.columns([2, 3])
with col_ctrl1:
    # 這裡將標示改為 3 分鐘
    auto_refresh = st.checkbox("開啟自動更新 (每 3 分鐘)", value=True)
with col_ctrl2:
    if st.button("🔄 手動重新整理 (若不滿 3 分鐘將從快取讀取)"):
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
    st.subheader(f"📊 即時排行榜 (最後更新時間: {current_time_str})")
    st.info("系統設定每 3 分鐘自動向伺服器請求一次數據，以確保連線安全。")

    styled_df = style_dataframe(df_current_top30, st.session_state.prev_top30)
    st.dataframe(styled_df, use_container_width=True, height=1050)

    st.session_state.prev_top30 = current_top30_set
else:
    st.warning("目前無法取得資料，請檢查網路連線或是否處於開盤時段。")

# 自動更新邏輯：等待 180 秒
if auto_refresh:
    time.sleep(180) 
    st.rerun()
