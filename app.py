import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

# 設定網頁佈局
st.set_page_config(page_title="台股即時成交金額 TOP30", layout="wide", page_icon="🔥")

# ----------------- Yahoo 爬蟲區塊 -----------------
@st.cache_data(ttl=5) # Streamlit 快取機制，限制每 5 秒最多只爬一次，避免被 Yahoo 封鎖
def get_yahoo_turnover_top30():
    url = "https://tw.stock.yahoo.com/rank/turnover"
    # 必須偽裝成正常的瀏覽器，否則會被 Yahoo 擋下
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Yahoo 的排行榜通常包在特定的 list 結構中
        # 這裡尋找所有包含股票資訊的列 (通常是 <li> 標籤且帶有特定的 flex class)
        rows = soup.find_all('li', class_='List(n)')
        
        data = []
        for row in rows[:30]: # 只取前 30 名
            # 由於 Yahoo 的 DOM 結構常有深層 div，我們用 text 來擷取
            cols = row.find_all('div', class_='Fxg(1)') 
            if not cols:
                continue
                
            try:
                # 1. 股票名稱與代號 (通常在第一個區塊)
                name_block = row.find('div', class_='Lh(20px)')
                ticker_block = row.find('span', class_='C(#979ba7)')
                stock_name = name_block.text.strip() if name_block else "未知"
                stock_id = ticker_block.text.replace('.TW', '').replace('.TWO', '').strip() if ticker_block else "未知"
                
                # 2. 獲取所有數據欄位文字
                # Yahoo 排列大約是：股價, 漲跌, 漲跌幅(%), 開盤, 昨收, 最高, 最低, 成交張數, 成交值(億)
                # 為了避免順序變動，我們直接抓取文字並做基本清理
                prices_texts = [c.text.strip() for c in cols]
                
                # 目前股價 (通常是欄位 0)
                price_str = prices_texts[0].replace(',', '')
                price = float(price_str) if price_str.replace('.', '', 1).isdigit() else 0.0
                
                # 漲幅 % (通常是欄位 2，包含 % 符號)
                change_str = prices_texts[2].replace('%', '').replace('+', '').replace(',', '')
                # 如果是平盤或沒抓到，設為 0
                if change_str == '-' or not change_str:
                    change_pct = 0.0
                else:
                    change_pct = float(change_str)
                    
                # 成交金額(億) (通常在最後面，有時顯示 M 代表萬，B 代表億，或直接寫中文字)
                turnover_str = prices_texts[-1]
                if '億' in turnover_str:
                    turnover = float(turnover_str.replace('億', '').replace(',', ''))
                elif '萬' in turnover_str:
                    turnover = float(turnover_str.replace('萬', '').replace(',', '')) / 10000
                else:
                    turnover = float(turnover_str.replace(',', '')) # 預設當作億
                
                data.append([stock_id, stock_name, price, change_pct, turnover])
                
            except Exception as e:
                # 若某一行解析失敗，跳過並印出錯誤供開發者除錯
                print(f"解析錯誤: {e}")
                continue
                
        df = pd.DataFrame(data, columns=['股票代號', '股票名稱', '目前股價', '漲幅(%)', '成交金額(億)'])
        return df
        
    except Exception as e:
        st.error(f"連線 Yahoo 股市失敗或網頁結構已更改：{e}")
        # 如果爬蟲失敗，回傳一個空的 DataFrame 避免程式崩潰
        return pd.DataFrame(columns=['股票代號', '股票名稱', '目前股價', '漲幅(%)', '成交金額(億)'])

# ----------------- 樣式與邏輯處理區塊 -----------------
def style_dataframe(df, prev_top30_set):
    def row_style(row):
        styles = [''] * len(row)
        
        # 1. 漲跌幅顏色處理 (台股：紅漲、綠跌)
        change_idx = df.columns.get_loc('漲幅(%)')
        if row['漲幅(%)'] > 0:
            styles[change_idx] = 'color: #ff4b4b; font-weight: bold;' # 紅色
        elif row['漲幅(%)'] < 0:
            styles[change_idx] = 'color: #00fa9a; font-weight: bold;' # 綠色
            
        # 2. 新進榜標示 (如果在上一筆紀錄沒有這個代號)
        if len(prev_top30_set) > 0 and row['股票代號'] not in prev_top30_set:
            styles = [s + 'background-color: rgba(255, 255, 0, 0.2);' for s in styles]
            
        return styles

    # 設定顯示格式
    return df.style.apply(row_style, axis=1)\
                   .format({'目前股價': '{:.2f}', '漲幅(%)': '{:+.2f}', '成交金額(億)': '{:.2f}'})

# ----------------- 主程式 UI 區塊 -----------------
st.title("🔥 台股即時成交金額 TOP30 (爬蟲版)")

# 初始化 Session State
if 'prev_top30' not in st.session_state:
    st.session_state.prev_top30 = set()

# 控制區塊
col_ctrl1, col_ctrl2 = st.columns([1, 4])
with col_ctrl1:
    auto_refresh = st.checkbox("開啟自動更新 (每 10 秒)")
with col_ctrl2:
    if st.button("🔄 手動強制更新"):
        st.cache_data.clear() # 清除快取，強制重新爬取
        st.rerun()

# 取得資料
with st.spinner("正在前往 Yahoo 股市抓取最新資料..."):
    df_current_top30 = get_yahoo_turnover_top30()

if not df_current_top30.empty:
    current_top30_set = set(df_current_top30['股票代號'])

    # 計算漲跌統計
    up_count = len(df_current_top30[df_current_top30['漲幅(%)'] > 0])
    down_count = len(df_current_top30[df_current_top30['漲幅(%)'] < 0])
    flat_count = len(df_current_top30[df_current_top30['漲幅(%)'] == 0])
    up_ratio = (up_count / len(df_current_top30)) * 100 if len(df_current_top30) > 0 else 0

    # 顯示統計指標
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🔝 TOP30 上漲檔數", f"{up_count} 檔")
    col2.metric("📉 TOP30 下跌檔數", f"{down_count} 檔")
    col3.metric("➖ TOP30 平盤檔數", f"{flat_count} 檔")
    col4.metric("🔥 多方勢力 (上漲比例)", f"{up_ratio:.1f} %")

    st.markdown("---")
    st.subheader(f"📊 即時成交金額排名 (最後更新: {time.strftime('%H:%M:%S')})")
    st.caption("💡 字體顯示**紅色**為上漲，**綠色**為下跌。整列顯示**黃色底色**代表剛擠進前 30 名的股票。")

    # 套用樣式並顯示表格
    styled_df = style_dataframe(df_current_top30, st.session_state.prev_top30)
    st.dataframe(styled_df, use_container_width=True, height=1050)

    # 更新名單供下次比對
    st.session_state.prev_top30 = current_top30_set

else:
    st.warning("目前無法取得資料，可能 Yahoo 網頁結構有變動，或正在非交易時段進行維護。")

# 自動更新邏輯 (建議爬蟲版間隔設長一點，例如 10 秒，以免被鎖 IP)
if auto_refresh:
    time.sleep(10) 
    st.cache_data.clear() # 確保下次重整時會重新發送 Request
    st.rerun()
