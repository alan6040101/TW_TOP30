import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timezone, timedelta
import re
import random

# 設定網頁佈局
st.set_page_config(page_title="台股成交金額 TOP30 儀表板", layout="wide", page_icon="🔥")

# 1. 設定台灣時區 (UTC+8)
tw_tz = timezone(timedelta(hours=8))

# 2. 建立 requests Session (連線池)
if 'http_session' not in st.session_state:
    st.session_state.http_session = requests.Session()
    st.session_state.http_session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7"
    })

def safe_float(text):
    cleaned = re.sub(r'[^\d.-]', '', text) 
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0

# ----------------- 資料獲取區塊 -----------------

# (保持不變) Yahoo 即時爬蟲
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

# [新增] 模擬歷史資料庫 (未來可替換為你的真實資料庫或 API)
@st.cache_data(ttl=3600)
def get_mock_historical_data(days_ago, current_df):
    """
    因為 Yahoo 沒提供歷史排行榜，這裡利用「今天的資料」加上隨機變化，
    產生逼真的歷史數據，用來完美展示你要的 UI 邏輯。
    """
    if current_df.empty:
        return current_df
    
    random.seed(datetime.now().day + days_ago) # 讓同一天的模擬資料固定
    
    hist_data = []
    # 隨機保留 20~25 檔今天的股票，並加入一些隨機假股票來模擬「新進榜 / 掉出榜」的輪動
    keep_count = random.randint(20, 25)
    kept_rows = current_df.sample(n=keep_count).to_dict('records')
    
    for row in kept_rows:
        row['漲幅(%)'] = round(random.uniform(-9.5, 9.5), 2)
        row['成交金額(億)'] = round(row['成交金額(億)'] * random.uniform(0.8, 1.2), 2)
        hist_data.append(row)
        
    for i in range(30 - keep_count):
        fake_id = f"23{random.randint(10, 99)}"
        hist_data.append({
            '股票代號': fake_id, '股票名稱': f"模擬股{fake_id}", 
            '目前股價': round(random.uniform(50, 500), 2),
            '漲幅(%)': round(random.uniform(-9.5, 9.5), 2),
            '成交金額(億)': round(random.uniform(10, 100), 2)
        })
        
    hist_df = pd.DataFrame(hist_data).sort_values(by='成交金額(億)', ascending=False).reset_index(drop=True)
    return hist_df

# ----------------- 樣式處理 -----------------
def style_dataframe(df, prev_day_set):
    """
    共用樣式：紅漲、綠跌、與前一日比較的新進榜標示底色
    """
    def row_style(row):
        styles = [''] * len(row)
        change_idx = df.columns.get_loc('漲幅(%)')
        # 上漲紅，下跌綠
        if row['漲幅(%)'] > 0:
            styles[change_idx] = 'color: #ff4b4b; font-weight: bold;' 
        elif row['漲幅(%)'] < 0:
            styles[change_idx] = 'color: #00fa9a; font-weight: bold;' 
            
        # 與前一日的名單比對，若不在裡面就是新進榜 (標示黃底)
        if len(prev_day_set) > 0 and row['股票代號'] not in prev_day_set:
            styles = [s + 'background-color: rgba(255, 255, 0, 0.2);' for s in styles]
        return styles

    return df.style.apply(row_style, axis=1)\
                   .format({'目前股價': '{:.2f}', '漲幅(%)': '{:+.2f}', '成交金額(億)': '{:.2f}'})

# ----------------- 主程式 UI -----------------
st.title("📈 台股成交金額 TOP30 監控系統")

# 使用 Streamlit 分頁功能
tab1, tab2 = st.tabs(["🔥 即時成交金額排行", "📅 歷史成交金額排行"])

# 先抓取今日即時資料 (供兩個分頁使用)
df_current_top30 = get_yahoo_turnover_top30()
current_time_str = datetime.now(tw_tz).strftime('%H:%M:%S')

# 推算「昨天」的資料 (用來當作今日的比較基準)
df_yesterday_top30 = get_mock_historical_data(1, df_current_top30)
yesterday_top30_set = set(df_yesterday_top30['股票代號']) if not df_yesterday_top30.empty else set()


# ==================== 分頁 1：即時排行 ====================
with tab1:
    col_ctrl1, col_ctrl2 = st.columns([2, 3])
    with col_ctrl1:
        auto_refresh = st.checkbox("開啟自動更新 (每 3 分鐘)", value=True, key="auto_refresh")
    with col_ctrl2:
        if st.button("🔄 手動重新整理 (若不滿 3 分鐘將從快取讀取)"):
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
        st.subheader(f"📊 今日即時排行榜 (最後更新時間: {current_time_str})")
        st.info("💡 整列顯示**黃色底色**代表與「昨日收盤」相比，今天新擠進前 30 名的強勢股。系統設定每 3 分鐘自動向伺服器請求一次數據。")

        # 套用樣式 (傳入昨天的名單作為比對基準)
        styled_df = style_dataframe(df_current_top30, yesterday_top30_set)
        st.dataframe(styled_df, use_container_width=True, height=1050)
    else:
        st.warning("目前無法取得即時資料，請檢查網路連線或是否處於開盤時段。")


# ==================== 分頁 2：歷史排行 ====================
with tab2:
    st.subheader("📅 近五日歷史成交排行榜 (模擬資料展示區)")
    st.caption("註：因 Yahoo 未提供歷史排行，此處以模擬資料展示 UI 邏輯。未來可替換為真實資料庫。")
    
    # 建立近五個交易日的選項 (簡單推算前 1~5 天)
    history_dates = [(datetime.now(tw_tz) - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, 6)]
    selected_date_str = st.selectbox("請選擇查詢日期：", history_dates)
    
    # 根據選取的日期，推算它是「幾天前」
    days_ago = history_dates.index(selected_date_str) + 1
    
    # 獲取「選取日」的資料
    df_selected_day = get_mock_historical_data(days_ago, df_current_top30)
    
    # 獲取「選取日的前一天」的資料 (用來抓新進榜)
    df_prev_day = get_mock_historical_data(days_ago + 1, df_current_top30)
    prev_day_set = set(df_prev_day['股票代號']) if not df_prev_day.empty else set()
    
    if not df_selected_day.empty:
        st.markdown(f"**{selected_date_str} 成交金額 TOP30** (黃色底色為相較於前一日的新進榜股票)")
        styled_hist_df = style_dataframe(df_selected_day, prev_day_set)
        st.dataframe(styled_hist_df, use_container_width=True, height=1050)

# ==================== 自動更新邏輯 ====================
# 放在最外層確保勾選後整個 App 都可以運作
if 'auto_refresh' in st.session_state and st.session_state.auto_refresh:
    time.sleep(180) 
    st.rerun()
