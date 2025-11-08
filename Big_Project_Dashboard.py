import pandas as pd
import requests
from datetime import datetime
import s3fs
import json
import streamlit as st # Đảm bảo import streamlit ở đầu file

# --- KIỂM TRA THƯ VIỆN BẮT BUỘC ---
try:
    import s3fs
except ImportError:
    st.error("LỖI: Thiếu thư viện 's3fs'. Vui lòng cài đặt bằng lệnh: pip install s3fs")
    st.stop()
# -----------------------------------

# --- CẤU HÌNH MINIO (S3A) ---
MINIO_ENDPOINT = "http://54.66.147.230:9000"
MINIO_ACCESS_KEY = "Karbi"
MINIO_SECRET_KEY = "Khuong@1701"
SIGNAL_PATH = "project2/signal/current_predictions/" # Đường dẫn Tín hiệu ML
REALTIME_DATA_PATH = "project2/crypto_ohlcv_1m.csv"    # Đường dẫn file CSV Giá Real-time

# Cấu hình S3FS cho Pandas/PyArrow
FS_KWARTS = {
    'key': MINIO_ACCESS_KEY,
    'secret': MINIO_SECRET_KEY,
    'client_kwargs': {
        'endpoint_url': MINIO_ENDPOINT
    }
}
S3A_SIGNAL_URI = f"s3a://{SIGNAL_PATH}"
S3A_REALTIME_URI = f"s3a://{REALTIME_DATA_PATH}" # URI mới

# --- 1. HÀM TẢI GIÁ REAL-TIME (DÙNG MINIO CSV) ---
@st.cache_data(ttl=300) # Cache 5 phút (300 giây) - Phù hợp với chu kỳ cào 5 phút
def load_realtime_prices_from_minio():
    """Tải và xử lý dữ liệu giá mới nhất từ file CSV/Parquet trên MinIO."""
    
    # Danh sách tên cột chuẩn sau khi đọc
    COLUMNS_STANDARDIZED = ['timestamp', 'coin', 'price_usd', 'market_cap_usd', 'volume_24h_usd']
    
    try:
        # Đọc toàn bộ file CSV thô. Dùng header=None để Pandas gán tên cột mặc định (0, 1, 2...)
        df_raw = pd.read_csv(
            S3A_REALTIME_URI, 
            storage_options=FS_KWARTS,
            header=None, # Đọc không có header
            engine='python'
        )
        
        # Vì file CSV của bạn không có header chuẩn, chúng ta giả định VỊ TRÍ cột:
        # 0: timestamp, 1: coin, 2: price_usd, 3: market_cap_usd, 4: volume_24h_usd
        # Lấy tối đa 5 cột đầu tiên và gán tên chuẩn
        df_raw = df_raw.iloc[:, :len(COLUMNS_STANDARDIZED)]
        df_raw.columns = COLUMNS_STANDARDIZED
        
        # --- 2. CHUYỂN ĐỔI VÀ LỌC DỮ LIỆU ---
        
        # Chuyển đổi timestamp và lọc các giá trị NaN
        df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'], utc=True, errors='coerce')
        df_raw = df_raw.dropna(subset=['timestamp', 'price_usd', 'coin']) # Loại bỏ các hàng thiếu
        
        if df_raw.empty:
            raise ValueError("DataFrame rỗng sau khi lọc giá trị thiếu.")
        
        df_raw = df_raw.sort_values(by='timestamp', ascending=False)
        
        # Lấy bản ghi MỚI NHẤT (first) cho MỖI COIN
        df_latest = df_raw.groupby('coin', as_index=False).first()

        # --- 3. ĐỊNH DẠNG KẾT QUẢ ---
        prices = {}
        for index, row in df_latest.iterrows():
            symbol = row['coin'].upper()
            prices[symbol] = {
                'price_usd': row['price_usd'],
                'market_cap_usd': row['market_cap_usd'], 
                'volume_24h_usd': row['volume_24h_usd']
            }
        
        last_update_time = df_raw['timestamp'].max()
        return prices, last_update_time if pd.notna(last_update_time) else datetime.now()
        
    except Exception as e:
        # st.error sẽ hiển thị lỗi ra dashboard
        st.error(f"LỖI: Không thể tải giá realtime từ MinIO. Chi tiết: '{e}'")
        return {}, datetime.now()

# --- 2. HÀM TẢI TÍN HIỆU DỰ ĐOÁN (DÙNG MINIO) ---
@st.cache_data(ttl=300) # Cache 5 phút
def load_last_known_signals():
    """Tải tín hiệu UP/DOWN mới nhất từ file Parquet trên MinIO."""
    try:
        # Sử dụng s3fs để đọc file Parquet từ MinIO
        df = pd.read_parquet(S3A_SIGNAL_URI, storage_options=FS_KWARTS)
        
        # Định dạng lại dữ liệu tín hiệu
        signals = {}
        last_publish_time = None

        if not df.empty:
            # Lấy bản ghi mới nhất (thường chỉ có 5 hàng)
            df['coin'] = df['coin'].str.upper() # Đảm bảo coin là chữ hoa
            
            # Sắp xếp để đảm bảo lấy tín hiệu mới nhất nếu có nhiều bản ghi trong thư mục
            df = df.sort_values(by='timestamp_publish', ascending=False).drop_duplicates(subset=['coin'], keep='first')
            
            for index, row in df.iterrows():
                signals[row['coin']] = {
                    'signal': row['signal'],
                    'timestamp_data': pd.to_datetime(row['timestamp_data']),
                    'timestamp_publish': pd.to_datetime(row['timestamp_publish'])
                }
            
            last_publish_time = df['timestamp_publish'].max()
        
        return signals, last_publish_time
        
    except Exception as e:
        st.warning(f"CẢNH BÁO: Không thể tải tín hiệu từ MinIO. Đảm bảo PySpark đã chạy batch ML. Chi tiết: {e}")
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
st.title("🤖 Last Known Prediction Signal")
st.markdown("---")

# Hiển thị trạng thái cập nhật
col_info_1, col_info_2 = st.columns([1, 1])

# Hiển thị trạng thái cập nhật giá (Từ MinIO)
col_info_1.metric(
    label="Cập nhật Giá Mới Nhất (MinIO Data)",
    value=realtime_time.strftime("%H:%M:%S")
)

# --- KHU VỰC HIỂN THỊ TÍN HIỆU ---
st.header("Trạng thái Dự báo Tín hiệu (Last Known Signal)")
st.markdown("Tín hiệu dự đoán xu hướng giá 5 phút tiếp theo (UP/DOWN) dựa trên mô hình ML batch gần nhất.")

crypto_list = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA']
cols = st.columns(len(crypto_list))

for i, coin in enumerate(crypto_list):
    with cols[i]:
        st.subheader(coin)
        
        price = realtime_data.get(coin, {}).get('price_usd', 0)
        signal_info = signal_data.get(coin, {})
        signal = signal_info.get('signal', 'N/A')
        
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

        st.metric(label="Giá Hiện tại (USD)", value=f"${price:,.2f}")
        
        if signal_info:
            data_time = signal_info['timestamp_data'].strftime("%H:%M:%S")
            


# --- KHU VỰC BẢNG TÓM TẮT THỊ TRƯỜNG (Giá mới nhất) ---
st.markdown("---")
st.header("Dữ liệu Giá Thị trường (Từ MinIO)")

# Tạo DataFrame cho bảng tóm tắt
if realtime_data:
    summary_data = []
    for coin, info in realtime_data.items():
        summary_data.append({
            'Coin': coin,
            'Giá (USD)': f"${info['price_usd']:,.2f}",
            'Vốn hoá Thị trường': f"${info.get('market_cap_usd', 0):,}",
            'Volume 24h': f"${info.get('volume_24h_usd', 0):,}"
        })
    df_summary = pd.DataFrame(summary_data)
    st.dataframe(df_summary, use_container_width=True, hide_index=True)
else:
    st.info("Không có dữ liệu giá thị trường để hiển thị.")
