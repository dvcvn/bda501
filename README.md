# Movie Recommendation System

Hệ thống đề xuất phim sử dụng Apache Spark và Kafka để xử lý dữ liệu real-time.

## Cấu trúc dự án

```
bda501/
├── config/
│   └── kafka_config.json
├── models/
│   └── als_model/
├── consume_recommendations.py
├── train_model.py
└── README.md
```

## Yêu cầu hệ thống

- Python 3.8+
- Apache Spark 3.5.0
- Apache Kafka
- PySpark
- findspark

## Cài đặt

1. Cài đặt các dependencies:
```bash
pip install pyspark==3.5.0 findspark
```

2. Cấu hình Kafka:
- Tạo file `config/kafka_config.json` với nội dung:
```json
{
    "bootstrap_servers": "localhost:9092",
    "input_topic": "user_requests",
    "output_topic": "movie_recommendations"
}
```

## Cách sử dụng

### 1. Train model

```bash
python train_model.py
```

Model sẽ được lưu vào thư mục `models/als_model/`.

### 2. Chạy consumer

```bash
python consume_recommendations.py
```

Consumer sẽ:
- Đọc user IDs từ Kafka topic `user_requests`
- Tạo recommendations cho mỗi user
- Gửi recommendations về Kafka topic `movie_recommendations`

## Cấu trúc dữ liệu

### Input (Kafka message)
```json
{
    "userId": 123
}
```

### Output (Kafka message)
```json
{
    "userId": 123,
    "recommendations": [
        {
            "movieId": 456,
            "rating": 4.5
        },
        {
            "movieId": 789,
            "rating": 4.2
        }
    ]
}
```

## Xử lý lỗi

Hệ thống có các cơ chế xử lý lỗi:
- Retry khi không thể kết nối Kafka
- Logging chi tiết các lỗi
- Graceful shutdown khi nhận signal

## Monitoring

- Logging được cấu hình ở mức INFO
- Các thông tin quan trọng được log:
  - Số lượng user IDs trong mỗi batch
  - Thời gian xử lý batch
  - Lỗi khi xử lý recommendations

## Lưu ý

- Đảm bảo Kafka server đang chạy trước khi start consumer
- Model phải được train trước khi chạy consumer
- Cấu hình Kafka phải chính xác trong file config 