# 📊 Ứng dụng Big Data Streaming Dashboard (Amazon Fashion Sentiment)

Ứng dụng phân tích độ hài lòng của khách hàng và phân loại cảm xúc đánh giá sản phẩm theo thời gian thực sử dụng Streamlit, HuggingFace Transformer và Oracle Cloud Streaming (Kafka).

---

## 🚀 Hướng dẫn Triển khai lên Streamlit Community Cloud (`streamlit.app`)

### Bước 1: Đẩy mã nguồn lên GitHub
1. Khởi tạo Git trong thư mục dự án (nếu chưa có):
```bash
git init
git add .
git commit -m "feat: hoàn thiện ứng dụng Streamlit Big Data Streaming"
```

2. Tạo một Repository mới trên [GitHub](https://github.com/new).
3. Đẩy code lên GitHub:
```bash
git branch -M main
git remote add origin https://github.com/<tai-khoan-github>/<ten-repo>.git
git push -u origin main
```

---

### Bước 2: Deploy lên Streamlit Community Cloud
1. Truy cập [share.streamlit.io](https://share.streamlit.io/) và đăng nhập bằng tài khoản GitHub.
2. Bấm nút **"Create app"** (hoặc **"New app"**).
3. Chọn các thông số triển khai:
   - **Repository:** Chọn repository vừa đẩy lên ở Bước 1.
   - **Branch:** `main`
   - **Main file path:** `app.py`
   - **App URL (tùy chọn):** Đặt tên miền con mong muốn (ví dụ: `fashion-sentiment-streaming.streamlit.app`).

---

### Bước 3: Cấu hình Secrets (Tùy chọn - Dành cho OCI Kafka)
Nếu bạn muốn kết nối an toàn với Oracle Cloud Streaming mà không để lộ mật khẩu trong code:
1. Trong giao diện quản lý App trên Streamlit Cloud, vào mục **App Settings** -> **Secrets**.
2. Dán nội dung cấu hình từ file `.streamlit/secrets.toml.example` vào:
```toml
[oci_kafka]
bootstrap_servers = "cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com:9092"
topic = "DemoStreamingFashion"
sasl_username = "dant49/dant@uel.edu.vn/ocid1.streampool.oc1.sa-saopaulo-1.amaaaaaai6jti5aa4m3x5a53b3n4uk6smo2cs6wi7vnues4kmrsoy5pr6mcq"
auth_token = "AIg(4_0#Xm_sR_u3y251"
```
3. Bấm **Save**. Ứng dụng sẽ tự động tải lại và nhận cấu hình bí mật.

---

## 💻 Chạy thử nghiệm ở máy cục bộ (Local)

1. Cài đặt các thư viện phụ thuộc:
```bash
pip install -r requirements.txt
```

2. Chạy ứng dụng Streamlit:
```bash
streamlit run app.py
```

3. Mở trình duyệt tại địa chỉ: `http://localhost:8501`

---

## 📁 Cấu trúc thư mục dự án

```text
├── app.py                         # File mã nguồn chính của ứng dụng Streamlit
├── requirements.txt               # Khai báo các thư viện Python
├── packages.txt                   # Khai báo thư viện Linux C (librdkafka-dev)
├── README.md                      # Hướng dẫn chi tiết triển khai và sử dụng
├── .streamlit/
│   ├── config.toml                # Cấu hình Theme và hiển thị giao diện
│   └── secrets.toml.example       # Mẫu cấu hình bảo mật tài khoản Kafka/OCI
└── Demo_BigData_Streaming.ipynb   # File notebook gốc
```
