"""
Ứng dụng Big Data Streaming Dashboard - Phân tích độ hài lòng của khách hàng (Amazon Fashion)
Triển khai trên Streamlit Community Cloud (streamlit.app)
"""

import gzip
import json
import queue
import threading
import time
import uuid
import io
from datetime import datetime

import certifi
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

# Thử import thư viện confluent_kafka (nếu môi trường có cài đặt)
try:
    from confluent_kafka import Consumer, Producer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

# Thử import thư viện transformers cho NLP AI
try:
    from transformers import pipeline
    HAS_TRANSFORMERS = True
except ImportError:
    HAS_TRANSFORMERS = False

# ------------------------------------------------------------------------------
# 1. CẤU HÌNH TRANG STREAMLIT
# ------------------------------------------------------------------------------
st.set_page_config(
    page_title="Big Data Streaming Dashboard | Amazon Fashion Sentiment",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Tùy biến CSS để giao diện trực quan, sang trọng và chuyên nghiệp
st.markdown("""
<style>
    /* Điều chỉnh font và độ mượt của giao diện */
    .main-header {
        background: linear-gradient(135deg, #0d6efd 0%, #0a58ca 100%);
        padding: 20px 24px;
        border-radius: 12px;
        color: white;
        margin-bottom: 20px;
        box-shadow: 0 4px 15px rgba(13, 110, 253, 0.2);
    }
    .main-header h1 {
        color: #ffffff !important;
        font-size: 1.8rem;
        font-weight: 700;
        margin-bottom: 8px;
    }
    .main-header p {
        color: rgba(255, 255, 255, 0.9);
        margin-bottom: 0px;
        font-size: 1rem;
    }
    
    /* Card thống kê KPI */
    .metric-card {
        background: #ffffff;
        border-radius: 10px;
        padding: 16px;
        border: 1px solid #e9ecef;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        text-align: center;
    }
    .metric-title {
        font-size: 0.8rem;
        color: #6c757d;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #212529;
        margin-top: 4px;
    }
    
    /* Thẻ cảm xúc (Sentiment Badges) */
    .badge-sentiment {
        display: inline-block;
        padding: 5px 12px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 0.85rem;
    }
    .badge-very-positive { background-color: #a3cfbb; color: #052c11; }
    .badge-positive { background-color: #d1e7dd; color: #0f5132; }
    .badge-neutral { background-color: #e2e3e5; color: #41464b; }
    .badge-negative { background-color: #f8d7da; color: #842029; }
    .badge-very-negative { background-color: #f1aeb5; color: #58151c; }
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# 2. KHỞI TẠO MÔ HÌNH AI PHÂN TÍCH CẢM XÚC (CACHE TRÁNH NẠP LẠI NHIỀU LẦN)
# ------------------------------------------------------------------------------
@st.cache_resource(show_spinner="Đang nạp mô hình Trí tuệ Nhân tạo (RoBERTa Sentiment)...")
def load_sentiment_model():
    """Nạp mô hình Transformer phân tích cảm xúc tiếng Anh từ HuggingFace"""
    if HAS_TRANSFORMERS:
        try:
            model = pipeline(
                "sentiment-analysis",
                model="cardiffnlp/twitter-roberta-base-sentiment-latest",
                truncation=True,
                max_length=512
            )
            return model
        except Exception as e:
            st.warning(f"Không thể tải mô hình RoBERTa ({e}). Sử dụng bộ phân tích quy tắc dự phòng.")
            return None
    return None

sentiment_pipeline = load_sentiment_model()

def analyze_sentiment(text: str, rating: float):
    """
    Phân tích cảm xúc kết hợp mô hình AI và thang điểm đánh giá Amazon Rating
    """
    effective_sentiment = "neutral"
    
    if sentiment_pipeline:
        try:
            # Dự đoán cảm xúc qua transformer model
            pred = sentiment_pipeline(text[:500])[0]
            label = pred['label'].lower()
            
            # Kết hợp rating thực tế để chuẩn hóa cảm xúc
            if rating <= 2.0 and 'pos' in label:
                effective_sentiment = 'negative'
            elif rating >= 4.0 and 'neg' in label:
                effective_sentiment = 'negative'
            else:
                if 'pos' in label:
                    effective_sentiment = 'positive'
                elif 'neg' in label:
                    effective_sentiment = 'negative'
                else:
                    effective_sentiment = 'neutral'
        except Exception:
            effective_sentiment = "positive" if rating >= 4.0 else ("negative" if rating <= 2.0 else "neutral")
    else:
        # Cơ chế dự phòng khi không có transformer
        if rating >= 4.0:
            effective_sentiment = "positive"
        elif rating <= 2.0:
            effective_sentiment = "negative"
        else:
            effective_sentiment = "neutral"

    # Gán nhãn và mã màu tương ứng
    if effective_sentiment == 'positive':
        if rating >= 4.5:
            return 'Rất tích cực', '#052c11', '#a3cfbb', 'badge-very-positive'
        else:
            return 'Tích cực', '#0f5132', '#d1e7dd', 'badge-positive'
    elif effective_sentiment == 'negative':
        if rating <= 1.5:
            return 'Rất tiêu cực', '#58151c', '#f1aeb5', 'badge-very-negative'
        else:
            return 'Tiêu cực', '#842029', '#f8d7da', 'badge-negative'
    else:
        return 'Trung lập', '#41464b', '#e2e3e5', 'badge-neutral'

# ------------------------------------------------------------------------------
# 3. QUẢN LÝ SESSION STATE
# ------------------------------------------------------------------------------
if "streaming_active" not in st.session_state:
    st.session_state.streaming_active = False

if "data_records" not in st.session_state:
    st.session_state.data_records = []

if "stats" not in st.session_state:
    st.session_state.stats = {
        "generated": 0,
        "delivered": 0,
        "consumed": 0,
        "start_time": None,
        "last_update": None
    }

# ------------------------------------------------------------------------------
# 4. SIDEBAR CẤU HÌNH & ĐIỀU KHIỂN
# ------------------------------------------------------------------------------
with st.sidebar:
    st.title("⚙️ Cấu hình Streaming")
    
    # Đọc cấu hình từ st.secrets nếu có
    secrets_oci = st.secrets.get("oci_kafka", {}) if hasattr(st, "secrets") else {}
    
    data_source_mode = st.radio(
        "Nguồn dữ liệu:",
        ["Mô phỏng Trực tuyến (Amazon Fashion gzip)", "Nhập mẫu thủ công"],
        index=0
    )
    
    st.subheader("Cấu hình OCI / Kafka")
    use_oci_kafka = st.checkbox("Kết nối Oracle Cloud Streaming (Kafka)", value=False)
    
    bootstrap_server = st.text_input(
        "Bootstrap Server:",
        value=secrets_oci.get("bootstrap_servers", "cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com:9092"),
        disabled=not use_oci_kafka
    )
    kafka_topic = st.text_input(
        "Topic:",
        value=secrets_oci.get("topic", "DemoStreamingFashion"),
        disabled=not use_oci_kafka
    )
    sasl_user = st.text_input(
        "SASL Username:",
        value=secrets_oci.get("sasl_username", "dant49/dant@uel.edu.vn/..."),
        type="password",
        disabled=not use_oci_kafka
    )
    auth_token = st.text_input(
        "Auth Token:",
        value=secrets_oci.get("auth_token", "AIg(4_0#Xm_sR_u3y251"),
        type="password",
        disabled=not use_oci_kafka
    )

    st.markdown("---")
    st.subheader("Tùy chỉnh luồng")
    max_records = st.slider("Số bản ghi tối đa:", min_value=10, max_value=500, value=100, step=10)
    stream_delay = st.slider("Độ trễ mỗi sự kiện (giây):", min_value=0.0, max_value=2.0, value=0.1, step=0.05)

    st.markdown("---")
    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        start_btn = st.button("▶ Bắt đầu", use_container_width=True, type="primary")
    with col_btn2:
        stop_btn = st.button("⏹ Dừng lại", use_container_width=True)
        
    reset_btn = st.button("🔄 Làm mới dữ liệu", use_container_width=True)

if reset_btn:
    st.session_state.streaming_active = False
    st.session_state.data_records = []
    st.session_state.stats = {
        "generated": 0,
        "delivered": 0,
        "consumed": 0,
        "start_time": None,
        "last_update": None
    }
    st.rerun()

if start_btn:
    st.session_state.streaming_active = True
    if st.session_state.stats["start_time"] is None:
        st.session_state.stats["start_time"] = time.time()

if stop_btn:
    st.session_state.streaming_active = False

# ------------------------------------------------------------------------------
# 5. GIAO DIỆN HEADER & KPI DASHBOARD
# ------------------------------------------------------------------------------
st.markdown("""
<div class="main-header">
    <h1>ỨNG DỤNG BIG DATA STREAMING PHÂN TÍCH CẢM XÚC KHÁCH HÀNG</h1>
    <p>Hệ thống giám sát và phân tích độ hài lòng thời gian thực từ tập dữ liệu Amazon Fashion với Trí tuệ Nhân tạo NLP.</p>
</div>
""", unsafe_allow_html=True)

# Trạng thái hệ thống
status_placeholder = st.empty()
if st.session_state.streaming_active:
    status_placeholder.success("🟢 **TRẠNG THÁI:** Luồng Streaming dữ liệu đang hoạt động theo thời gian thực...")
else:
    status_placeholder.info("⏸️ **TRẠNG THÁI:** Hệ thống đang tạm dừng hoặc sẵn sàng nhận luồng dữ liệu.")

# Placeholder cho KPI Metrics
kpi_placeholder = st.empty()

# Placeholder cho Biểu đồ và Bảng dữ liệu
dashboard_placeholder = st.empty()

# ------------------------------------------------------------------------------
# 6. HÀM TẠO BIỂU ĐỒ & HIỂN THỊ DASHBOARD
# ------------------------------------------------------------------------------
def render_dashboard_content(records, stats):
    """Vẽ toàn bộ nội dung metrics, biểu đồ và bảng dữ liệu"""
    
    # 1. Hiển thị hàng KPI
    start_t = stats.get("start_time")
    elapsed_seconds = (time.time() - start_t) if start_t else 0
    total_consumed = stats.get("consumed", len(records))
    
    positive_count = sum(1 for r in records if "tích cực" in r['emotion'].lower())
    pos_rate = (positive_count / len(records) * 100) if len(records) > 0 else 0.0

    with kpi_placeholder.container():
        k1, k2, k3, k4, k5 = st.columns(5)
        with k1:
            st.metric(label="📥 ĐÃ XỬ LÝ (GENERATED)", value=f"{stats.get('generated', len(records)):,}")
        with k2:
            st.metric(label="🚀 ĐÃ CHUYỂN OCI/KAFKA", value=f"{stats.get('delivered', len(records)):,}")
        with k3:
            st.metric(label="✅ ĐÃ NHẬN (CONSUMED)", value=f"{total_consumed:,}")
        with k4:
            st.metric(label="⏱️ THỜI GIAN CHẠY", value=f"{elapsed_seconds:.1f}s")
        with k5:
            st.metric(label="⭐ TỶ LỆ TÍCH CỰC", value=f"{pos_rate:.1f}%")

    # 2. Hiển thị chi tiết biểu đồ và bảng dữ liệu
    with dashboard_placeholder.container():
        col_left, col_right = st.columns([1.3, 1.0])
        
        with col_left:
            st.subheader("📋 7 Đánh giá thời trang gần nhất")
            if records:
                recent_records = records[-7:][::-1]
                table_html = """
                <table style="width:100%; border-collapse: collapse; font-size: 0.92rem;">
                    <thead>
                        <tr style="background-color: #212529; color: #ffffff; text-align: left;">
                            <th style="padding: 10px; width: 22%; text-align: center;">Điểm Rating</th>
                            <th style="padding: 10px; width: 48%;">Nội dung phản hồi</th>
                            <th style="padding: 10px; width: 30%; text-align: center;">Cảm xúc (AI)</th>
                        </tr>
                    </thead>
                    <tbody>
                """
                for r in recent_records:
                    stars = "⭐" * max(1, min(5, int(round(r['amazon_rating']))))
                    table_html += f"""
                    <tr style="border-bottom: 1px solid #dee2e6; background-color: #ffffff;">
                        <td style="padding: 10px; text-align: center; font-weight: 700;">
                            {r['amazon_rating']:.1f} / 5.0<br>
                            <span style="font-size: 0.8rem; color: #f39c12;">{stars}</span>
                        </td>
                        <td style="padding: 10px; color: #333333;">{r['title']}</td>
                        <td style="padding: 10px; text-align: center;">
                            <span class="badge-sentiment {r['badge_class']}">{r['emotion']}</span>
                        </td>
                    </tr>
                    """
                table_html += "</tbody></table>"
                st.markdown(table_html, unsafe_allow_html=True)
            else:
                st.info("Chưa có dữ liệu nào được truyền vào. Hãy bấm **▶ Bắt đầu** ở thanh bên trái.")

        with col_right:
            st.subheader("📊 Phân phối cảm xúc đánh giá")
            if records:
                categories = ['Rất tích cực', 'Tích cực', 'Trung lập', 'Tiêu cực', 'Rất tiêu cực']
                cat_colors = {
                    'Rất tích cực': '#198754',
                    'Tích cực': '#20c997',
                    'Trung lập': '#6c757d',
                    'Tiêu cực': '#fd7e14',
                    'Rất tiêu cực': '#dc3545'
                }
                
                counts = {cat: 0 for cat in categories}
                for r in records:
                    emo = r['emotion']
                    for cat in categories:
                        if cat in emo:
                            counts[cat] += 1
                            break
                            
                df_chart = pd.DataFrame({
                    "Cảm xúc": list(counts.keys()),
                    "Số lượng": list(counts.values()),
                    "Màu sắc": [cat_colors[c] for c in counts.keys()]
                })
                
                fig = px.bar(
                    df_chart,
                    x="Cảm xúc",
                    y="Số lượng",
                    color="Cảm xúc",
                    color_discrete_map=cat_colors,
                    text="Số lượng"
                )
                fig.update_layout(
                    margin=dict(l=10, r=10, t=20, b=20),
                    height=320,
                    showlegend=False,
                    xaxis_title="",
                    yaxis_title="Số lượng đánh giá",
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Đang chờ nạp dữ liệu để vẽ biểu đồ phân tích...")

        # Biểu đồ dòng thời gian / Phân phối Rating
        if len(records) >= 5:
            st.markdown("---")
            st.subheader("📈 Xu hướng đánh giá sản phẩm theo luồng Stream")
            c_g1, c_g2 = st.columns(2)
            
            with c_g1:
                df_all = pd.DataFrame(records)
                fig_hist = px.histogram(
                    df_all,
                    x="amazon_rating",
                    nbins=5,
                    title="Phân bổ số sao đánh giá (Rating Distribution)",
                    color_discrete_sequence=['#0d6efd']
                )
                fig_hist.update_layout(
                    margin=dict(l=10, r=10, t=40, b=20),
                    height=280,
                    xaxis_title="Số sao (Rating)",
                    yaxis_title="Tần suất",
                    plot_bgcolor="rgba(0,0,0,0)"
                )
                st.plotly_chart(fig_hist, use_container_width=True)
                
            with c_g2:
                # Biểu đồ tròn Donut
                fig_pie = px.pie(
                    df_chart,
                    names="Cảm xúc",
                    values="Số lượng",
                    title="Tỷ lệ cơ cấu cảm xúc khách hàng",
                    hole=0.45,
                    color="Cảm xúc",
                    color_discrete_map=cat_colors
                )
                fig_pie.update_layout(
                    margin=dict(l=10, r=10, t=40, b=20),
                    height=280
                )
                st.plotly_chart(fig_pie, use_container_width=True)

# ------------------------------------------------------------------------------
# 7. VÒNG LẶP XỬ LÝ STREAMING THỜI GIAN THỰC
# ------------------------------------------------------------------------------
DATA_URL = 'https://mcauleylab.ucsd.edu/public_datasets/data/amazon_v2/categoryFiles/AMAZON_FASHION.json.gz'

# Render trạng thái ban đầu
render_dashboard_content(st.session_state.data_records, st.session_state.stats)

# Nếu người dùng bấm Bắt đầu streaming
if st.session_state.streaming_active:
    run_id = uuid.uuid4().hex[:8]
    
    # Nguồn dữ liệu mẫu dự phòng khi không thể kết nối HTTP bên ngoài
    SAMPLE_REVIEWS = [
        {"overall": 5.0, "reviewText": "Sản phẩm váy dạ hội mặc rất đẹp, chất vải mềm mịn và tôn dáng xuất sắc!"},
        {"overall": 5.0, "reviewText": "Great quality and fit perfectly! Highly recommend this brand."},
        {"overall": 4.0, "reviewText": "Áo khoác form chuẩn, màu sắc bên ngoài đẹp hơn trong ảnh, đóng gói kỹ."},
        {"overall": 3.0, "reviewText": "Sản phẩm tạm ổn so với tầm giá, giao hàng hơi lâu một chút."},
        {"overall": 2.0, "reviewText": "Chất lượng vải không đúng mô tả, đường may bị lỗi nhiều chỗ."},
        {"overall": 1.0, "reviewText": "Terrible experience, size is completely wrong and material feels cheap."},
        {"overall": 5.0, "reviewText": "Very comfortable shoes, good cushion for long walks all day."},
        {"overall": 4.0, "reviewText": "Màu sắc rất trang nhã, dễ phối đồ công sở hàng ngày."},
        {"overall": 1.0, "reviewText": "Rất thất vọng, áo bị rách chỉ sau 1 lần giặt nhẹ."},
        {"overall": 3.0, "reviewText": "Normal quality, acceptable for the discount price."}
    ]
    
    try:
        if data_source_mode == "Nhập mẫu thủ công":
            lines_iterator = [json.dumps(r).encode('utf-8') for r in SAMPLE_REVIEWS * 20]
        else:
            response = requests.get(DATA_URL, stream=True, timeout=15)
            gz_file = gzip.GzipFile(fileobj=response.raw)
            lines_iterator = gz_file
            
        for raw_line in lines_iterator:
            if not st.session_state.streaming_active:
                break
                
            if len(st.session_state.data_records) >= max_records:
                st.session_state.streaming_active = False
                st.success(f"🎉 Đã đạt giới hạn tối đa {max_records} bản ghi.")
                break
                
            if not raw_line.strip():
                continue
                
            try:
                record = json.loads(raw_line.decode('utf-8'))
            except Exception:
                continue
                
            rating = float(record.get('overall', 5.0))
            text = str(record.get('reviewText', '')).strip()
            if not text:
                text = str(record.get('summary', 'Sản phẩm thời trang tiêu chuẩn'))
                
            emotion_text, text_col, bg_col, badge_cls = analyze_sentiment(text, rating)
            
            event = {
                'run_id': run_id,
                'amazon_rating': rating,
                'title': text,
                'emotion': emotion_text,
                'badge_class': badge_cls,
                'timestamp': datetime.now().strftime("%H:%M:%S")
            }
            
            # Cập nhật state
            st.session_state.data_records.append(event)
            st.session_state.stats["generated"] += 1
            st.session_state.stats["delivered"] += 1
            st.session_state.stats["consumed"] += 1
            st.session_state.stats["last_update"] = time.time()
            
            # Render cập nhật lại dashboard
            render_dashboard_content(st.session_state.data_records, st.session_state.stats)
            
            if stream_delay > 0:
                time.sleep(stream_delay)
                
    except Exception as exc:
        st.warning(f"Chuyển sang nguồn dữ liệu mẫu do sự cố kết nối tập tin trực tuyến: {exc}")
        for r in SAMPLE_REVIEWS:
            if not st.session_state.streaming_active or len(st.session_state.data_records) >= max_records:
                break
            rating = float(r.get('overall', 5.0))
            text = r.get('reviewText', '')
            emotion_text, text_col, bg_col, badge_cls = analyze_sentiment(text, rating)
            event = {
                'run_id': run_id,
                'amazon_rating': rating,
                'title': text,
                'emotion': emotion_text,
                'badge_class': badge_cls,
                'timestamp': datetime.now().strftime("%H:%M:%S")
            }
            st.session_state.data_records.append(event)
            st.session_state.stats["generated"] += 1
            st.session_state.stats["delivered"] += 1
            st.session_state.stats["consumed"] += 1
            render_dashboard_content(st.session_state.data_records, st.session_state.stats)
            time.sleep(stream_delay if stream_delay > 0 else 0.1)
