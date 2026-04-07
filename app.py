import streamlit as st
import pandas as pd
import numpy as np
import time

# 設定網頁佈局
st.set_page_config(page_title="台股即時成交金額 TOP30", layout="wide", page_icon="📈")

# ----------------- 模擬資料生成區塊 -----------------
# 在實際應用中，請將此段替換為串接 Fugle、Twstock 或券商 API 的程式碼
def get_mock_market_data():
    np.random.seed(int(time.time()) % 100) # 讓每次產生的資料有一點隨機變動
    
    # 模擬 50 檔熱門股票池
    stock_ids = [f"23{str(i).zfill(2)}" for i in range(10, 60)]
    names = [f"測試電子股{i}" for i in range(10, 60)]
    
    data = []
    for sid, name in zip(stock_ids, names):
        price = np.random.uniform(50, 800)
        change_pct = np.random.uniform(-9.5, 9.5) # 模擬漲跌幅 -9.5% 到 9.5%
        # 模擬成交金額 (單位: 億)
        turnover = np.random.uniform(10, 500) 
        data.append([sid, name, round(price, 2), round(change_pct, 2), round(turnover, 2)])
        
    df = pd.DataFrame(data, columns=['股票代號', '股票名稱', '目前股價', '漲幅(%)', '成交金額(億)'])
    # 依照成交金額排序，取 Top 30
    df_top30 = df.sort_values(by='成交金額(億)', ascending=False).head(30).reset_index(drop=True)
    return df_top30

# ----------------- 樣式與邏輯處理區塊 -----------------
def style_dataframe(df, prev_top30_set):
    """
    處理表格的顏色：
    1. 漲幅 > 0 顯示紅色，< 0 顯示綠色
    2. 新進 TOP30 的股票標示底色
    """
    def row_style(row):
        styles = [''] * len(row)
        
        # 漲跌幅顏色處理 (台股：紅漲、綠跌)
        change_idx = df.columns.get_loc('漲幅(%)')
        if row['漲幅(%)'] > 0:
            styles[change_idx] = 'color: #ff4b4b; font-weight: bold;' # 紅色
        elif row['漲幅(%)'] < 0:
            styles[change_idx] = 'color: #00fa9a; font-weight: bold;' # 綠色
            
        # 新進榜標示 (如果有上一筆紀錄，且當前代號不在上一筆紀錄中)
        if len(prev_top30_set) > 0 and row['股票代號'] not in prev_top30_set:
            # 將整列加上微黃色底色來突顯
            styles = [s + 'background-color: rgba(255, 255, 0, 0.15);' for s in styles]
            
        return styles

    return df.style.apply(row_style, axis=1).format({'目前股價': '{:.2f}', '漲幅(%)': '{:.2f}', '成交金額(億)': '{:.2f}'})

# ----------------- 主程式 UI 區塊 -----------------
st.title("📈 台股即時成交金額 TOP30 監控儀表板")

# 初始化 Session State 來記錄「前一次」的 TOP30 名單，藉此判斷誰是新進榜
if 'prev_top30' not in st.session_state:
    st.session_state.prev_top30 = set()

# 控制自動更新的開關
auto_refresh = st.checkbox("開啟即時自動更新 (每 5 秒)")

# 取得資料
df_current_top30 = get_mock_market_data()
current_top30_set = set(df_current_top30['股票代號'])

# 計算漲跌統計
up_count = len(df_current_top30[df_current_top30['漲幅(%)'] > 0])
down_count = len(df_current_top30[df_current_top30['漲幅(%)'] < 0])
flat_count = len(df_current_top30[df_current_top30['漲幅(%)'] == 0])

up_ratio = (up_count / 30) * 100

# 顯示統計指標
col1, col2, col3, col4 = st.columns(4)
col1.metric("TOP30 上漲檔數", f"{up_count} 檔")
col2.metric("TOP30 下跌檔數", f"{down_count} 檔")
col3.metric("TOP30 平盤檔數", f"{flat_count} 檔")
col4.metric("多方勢力 (上漲比例)", f"{up_ratio:.1f} %")

st.markdown("---")
st.subheader(f"📊 今日成交金額 TOP30 (更新時間: {time.strftime('%H:%M:%S')})")
st.caption("💡 提示：字體顯示**紅色**為上漲，**綠色**為下跌。整列顯示**黃色底色**代表剛擠進前 30 名的新進榜股票。")

# 套用樣式並顯示表格
styled_df = style_dataframe(df_current_top30, st.session_state.prev_top30)
st.dataframe(styled_df, use_container_width=True, height=800)

# 更新前一次的名單為當前名單，供下次迴圈比對
st.session_state.prev_top30 = current_top30_set

# 自動更新邏輯
if auto_refresh:
    time.sleep(5) # 等待 5 秒
    st.rerun()    # 重新整理頁面