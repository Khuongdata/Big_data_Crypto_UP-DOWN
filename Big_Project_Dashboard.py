import pandas as pd
import requests
from datetime import datetime, timezone
import s3fs
import json
import streamlit as st 

# --- KIỂM TRA THƯ VIỆN BẮT BUỘC ---
try:
    import s3fs
    import pyarrow 
except ImportError:
    st.error("LỖI: Thiếu thư viện 's3fs' hoặc 'pyarrow'. Vui lòng cài đặt bằng lệnh: pip install s3fs pyarrow")
    st.stop()
# -----------------------------------

# --- CẤU HÌNH MINIO (S3A) ---
# **Đọc Cấu hình từ Streamlit Secrets**
try:
    MINIO_ENDPOINT = st.secrets["minio"]["endpoint"]
    MINIO_ACCESS_KEY = st.secrets["minio"]["access_key"]
    MINIO_SECRET_KEY = st.secrets["minio"]["secret_key"]
except KeyError:
    st.error("LỖI CẤU HÌNH: Không tìm thấy 'minio' secrets. Vui lòng thêm các khóa MinIO vào Streamlit Secrets.")
    st.stop()

# ĐÃ CẬP NHẬT ĐƯỜNG DẪN TÍN HIỆU ĐỂ ĐỌC FILE SO SÁNH CỦA PYSPARK
SIGNAL_PATH = "project2/signal/current_predictions_comparison/" 
REALTIME_DATA_PATH = "project2/crypto_ohlcv_1m.csv"

# Cấu hình S3FS cho Pandas/PyArrow
FS_KWARTS = {
    'key': MINIO_ACCESS_KEY,
    'secret': MINIO_SECRET_KEY,
    'client_kwargs': {
        'endpoint_url': MINIO_ENDPOINT
    }
}
S3A_SIGNAL_URI = f"s3a://{SIGNAL_PATH}"
S3A_REALTIME_URI = f"s3a://{REALTIME_DATA_PATH}" 

# --- 1. HÀM TẢI GIÁ REAL-TIME (DÙNG MINIO CSV) ---
@st.cache_data(ttl=300) # Cache 5 phút (300 giây) - Phù hợp với chu kỳ cào 5 phút
def load_realtime_prices_from_minio():
    """Tải và xử lý dữ liệu giá mới nhất từ file CSV/Parquet trên MinIO."""
    
    # Cột 0: timestamp, Cột 1: coin, Cột 2: price_usd, Cột 3: volume, Cột 4: market_cap
    COLUMNS_STANDARDIZED = ['timestamp', 'coin', 'price_usd', 'market_cap_usd', 'volume_24h_usd', 'change_24h_pct'] 
    
    try:
        # Tải file CSV
        df_raw = pd.read_csv(
            S3A_REALTIME_URI, 
            storage_options=FS_KWARTS,
            header=None, # Đọc không có header
            engine='python'
        )
        
        # Chỉ lấy 6 cột cần thiết (timestamp, coin, price_usd, volume, market_cap, change_pct)
        df_raw = df_raw.iloc[:, :len(COLUMNS_STANDARDIZED)]
        df_raw.columns = COLUMNS_STANDARDIZED
        
        # --- 2. CHUYỂN ĐỔI VÀ LỌC DỮ LIỆU ---
        df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'], utc=True, errors='coerce')
        df_raw['price_usd'] = pd.to_numeric(df_raw['price_usd'], errors='coerce')
        df_raw = df_raw.dropna(subset=['timestamp', 'price_usd', 'coin']) 
        
        if df_raw.empty:
            raise ValueError("DataFrame rỗng sau khi lọc giá trị thiếu.")
        
        # Lấy bản ghi mới nhất cho mỗi coin
        df_raw = df_raw.sort_values(by='timestamp', ascending=False)
        df_latest = df_raw.groupby('coin', as_index=False).first()

        # --- 3. ĐỊNH DẠNG KẾT QUẢ ---
        prices = {}
        for index, row in df_latest.iterrows():
            symbol = row['coin'].upper()
            prices[symbol] = {
                'price_usd': row['price_usd'],
                'market_cap_usd': row['market_cap_usd'],
                'volume_24h_usd': row['volume_24h_usd'],
                'change_24h_pct': row['change_24h_pct']
            }
        
        last_update_time = df_raw['timestamp'].max()
        return prices, last_update_time if pd.notna(last_update_time) else datetime.now(timezone.utc)
        
    except Exception as e:
        st.error(f"LỖI: Không thể tải giá realtime từ MinIO. Chi tiết: '{e}'")
        return {}, datetime.now(timezone.utc)

# --- 2. HÀM TẢI TÍN HIỆU DỰ ĐOÁN (DÙNG MINIO) ---
@st.cache_data(ttl=300) # Cache 5 phút
def load_last_known_signals():
    """Tải tín hiệu LR/DT mới nhất từ file Parquet so sánh trên MinIO."""
    try:
        # Tải file Parquet từ MinIO
        df = pd.read_parquet(S3A_SIGNAL_URI, storage_options=FS_KWARTS) 
        
        signals = {}
        last_publish_time = None

        if not df.empty:
            df['coin'] = df['coin'].astype(str).str.upper() 
            # Đảm bảo sử dụng cột signal_lr và signal_dt
            df = df.sort_values(by='timestamp_publish', ascending=False).drop_duplicates(subset=['coin'], keep='first')
            
            for index, row in df.iterrows():
                signals[row['coin']] = {
                    # LƯU KẾT QUẢ CỦA CẢ HAI MÔ HÌNH VÀO DICT
                    'signal_lr': row.get('signal_lr', 'N/A'),
                    'signal_dt': row.get('signal_dt', 'N/A'),
                    'prediction_lr': row.get('prediction_lr', 0),
                    'prediction_dt': row.get('prediction_dt', 0),
                    'timestamp_data': pd.to_datetime(row['timestamp_data']),
                    'timestamp_publish': pd.to_datetime(row['timestamp_publish'])
                }
            
            last_publish_time = df['timestamp_publish'].max()
        
        return signals, last_publish_time
        
    except Exception as e:
        st.warning(f"CẢNH BÁO: Không thể tải tín hiệu từ MinIO. Chi tiết: {e}")
        return {}, None


# --- KHỞI TẠO VÀ HIỂN THỊ DASHBOARD ---

# Cấu hình Page Title và Layout
st.set_page_config(
    page_title="Crypto Trading Signal Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Tải dữ liệu
realtime_data, realtime_time = load_realtime_prices_from_minio()
signal_data, last_model_update = load_last_known_signals()

# --- TIÊU ĐỀ CHÍNH ---
st.title("🤖 Last Known Prediction Signal - LR vs DT")
st.markdown("---")

# --------------------------
# KHU VỰC CHỌN MÔ HÌNH
# --------------------------
st.sidebar.header("Tùy Chọn Hiển Thị")
selected_model = st.sidebar.radio(
    "Chọn Mô Hình Dự Đoán",
    ('Logistic Regression (LR)', 'Decision Tree (DT)'),
    index=0 
)
# Xác định key signal và key prediction sẽ được sử dụng
SIGNAL_KEY = 'signal_lr' if selected_model == 'Logistic Regression (LR)' else 'signal_dt'
PREDICTION_KEY = 'prediction_lr' if selected_model == 'Logistic Regression (LR)' else 'prediction_dt'


# Hiển thị trạng thái cập nhật
col_info_1, col_info_2 = st.columns([1, 1])

# Hiển thị trạng thái cập nhật giá (Từ MinIO)
col_info_1.metric(
    label="Cập nhật Giá Mới Nhất",
    value=realtime_time.strftime("%H:%M:%S")
)


# --- KHU VỰC HIỂN THỊ TÍN HIỆU ---
st.header(f"Trạng thái Dự báo Tín hiệu (Mô hình: {selected_model})")
st.markdown("Tín hiệu dự đoán xu hướng giá 5 phút tiếp theo (UP/DOWN) dựa trên mô hình ML batch gần nhất.")

crypto_list = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA']
cols = st.columns(len(crypto_list))

for i, coin in enumerate(crypto_list):
    with cols[i]:
        st.subheader(coin)
        
        price = realtime_data.get(coin, {}).get('price_usd', 0)
        change_pct = realtime_data.get(coin, {}).get('change_24h_pct', 0)
        signal_info = signal_data.get(coin, {})
        
        # Lấy tín hiệu dựa trên lựa chọn của người dùng
        signal = signal_info.get(SIGNAL_KEY, 'N/A')
        prediction_value = signal_info.get(PREDICTION_KEY, 0) # Giá trị prediction thô (xác suất)
        
        # Định dạng hiển thị tín hiệu
        if signal == 'UP':
            color = "green"
            icon = "▲"
            st.markdown(f"<p style='font-size: 24px; color: {color};'>**{icon} {signal}**</p>", unsafe_allow_html=True)
        elif signal == 'DOWN':
            color = "red"
            icon = "▼"
            st.markdown(f"<p style='font-size: 24px; color: {color};'>**{icon} {signal}**</p>", unsafe_allow_html=True)
        else:
            color = "gray"
            icon = "N/A"
            st.markdown(f"<p style='font-size: 24px; color: {color};'>**{icon} {signal}**</p>", unsafe_allow_html=True)

        # Hiển thị Giá và Độ mạnh dự đoán (Nếu có)
        st.metric(label="Giá Hiện tại (USD)", value=f"${price:,.2f}", delta=f"{change_pct:,.2f}% (24H)")




# --- KHU VỰC BẢNG TÓM TẮT THỊ TRƯỜNG (Giá mới nhất) ---
st.markdown("---")
st.header("Dữ liệu Giá Thị trường (Từ MinIO)")

# Tạo DataFrame cho bảng tóm tắt
if realtime_data:
    summary_data = []
    for coin, info in realtime_data.items():
        summary_data.append({
            'Coin': coin,
            'Giá (USD)': info['price_usd'],
            'Vốn hoá Thị trường': info.get('market_cap_usd', 0),
            'Volume 24h': info.get('volume_24h_usd', 0),
            'Change 24h (%)': info.get('change_24h_pct', 0)
        })
    df_summary = pd.DataFrame(summary_data)
    
    # Định dạng các cột số học
    df_summary['Giá (USD)'] = df_summary['Giá (USD)'].apply(lambda x: f"${x:,.2f}")
    df_summary['Vốn hoá Thị trường'] = df_summary['Vốn hoá Thị trường'].apply(lambda x: f"${x:,.0f}")
    df_summary['Volume 24h'] = df_summary['Volume 24h'].apply(lambda x: f"${x:,.0f}")
    df_summary['Change 24h (%)'] = df_summary['Change 24h (%)'].apply(lambda x: f"{x:,.2f}%")
    
    st.dataframe(df_summary, use_container_width=True, hide_index=True)
else:
    st.info("Không có dữ liệu giá thị trường để hiển thị.")




