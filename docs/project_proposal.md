# Đề Xuất Đề Tài Cơ Sở Dữ Liệu Phân Tán

**Hạn nộp:** Theo thông báo của giảng viên  
**Mã đề tài & nhóm đánh giá:** #40: Log Pruning and Checkpointing - Category 4

## 1. Thông Tin Đề Tài

**Hình thức:** Đề tài cuối kỳ cá nhân

**Sinh viên thực hiện:** Nguyễn Ngọc Duy

**MSSV:** N23DCCN015

**Tên đề tài:** Global Checkpointing and Safe Log Pruning for High-Frequency Trading Transactions

## 2. Mục Tiêu Và Phát Biểu Bài Toán

### Lý do thực hiện

Các hệ thống giao dịch chứng khoán tần suất cao tạo ra lượng log rất lớn trong thời gian ngắn. Trong môi trường cơ sở dữ liệu phân tán, một transaction có thể liên quan đến nhiều site khác nhau. Nếu xóa log quá sớm, hệ thống có thể mất khả năng recovery khi một node crash.

Đề tài giải quyết câu hỏi:

```text
Làm sao xác định điểm an toàn toàn cục để xóa log trên nhiều site
mà không làm mất khả năng phục hồi sau crash?
```

### Logic cốt lõi

Project cài đặt các cơ chế chính:

- **Two-Phase Commit (2PC):** Coordinator gửi `PREPARE`, participant vote `VOTE_COMMIT` hoặc `VOTE_ABORT`, sau đó Coordinator quyết định `GLOBAL_COMMIT` hoặc `GLOBAL_ABORT`.
- **Durable logging:** mỗi site ghi log JSONL riêng để phục vụ recovery.
- **Local checkpoint:** mỗi participant tạo checkpoint từ durable log của chính nó.
- **Global checkpoint:** hệ thống gom local checkpoint và tính safe point:

```text
global_safe_point = min(
    NodeA.last_checkpointed_gseq,
    NodeB.last_checkpointed_gseq,
    NodeC.last_checkpointed_gseq
)
```

- **Safe log pruning:** chỉ xóa log đã final và không nằm trong `protected_tx_ids`.
- **Recovery:** các transaction ở `READY` / in-doubt được phục hồi thành `COMMIT` hoặc `ABORT` dựa trên global decision của Coordinator.

## 3. Mô Tả Dataset

**Nguồn dữ liệu:** Dataset được sinh bằng script của project:

```bash
python scripts/generate_dataset.py --records 100000
```

**Đường dẫn dataset:**

```text
data/transactions_100k.jsonl
data/dataset_summary.json
```

**Kích thước:** 100,000 transaction records, khoảng 16 MB.

**Schema:**

| Trường       | Ý nghĩa                                   |
| ------------ | ----------------------------------------- |
| `tx_id`      | Mã transaction, ví dụ `TX000001`          |
| `account_id` | Mã tài khoản giao dịch                    |
| `symbol`     | Mã cổ phiếu, ví dụ `AAPL`, `MSFT`, `NVDA` |
| `side`       | Loại lệnh: `BUY` hoặc `SELL`              |
| `quantity`   | Số lượng cổ phiếu                         |
| `price`      | Giá giao dịch                             |
| `timestamp`  | Thời điểm giao dịch                       |

Ví dụ record:

```json
{
  "tx_id": "TX000001",
  "account_id": "ACC0282",
  "symbol": "MSFT",
  "side": "BUY",
  "quantity": 20,
  "price": 414.51,
  "timestamp": "2026-05-16T10:00:00.000121+00:00"
}
```

**Chiến lược phân tán dữ liệu:**

Dataset không được chia thành nhiều file vật lý riêng. Thay vào đó, mỗi transaction được Coordinator gửi đến ba participant site mô phỏng: NodeA, NodeB, NodeC. Mỗi site có durable log riêng:

```text
logs/NodeA.log
logs/NodeB.log
logs/NodeC.log
```

Như vậy, dữ liệu đầu vào là cùng một workload, còn trạng thái xử lý và recovery log được phân tán theo từng site.

## 4. Kiến Trúc Hệ Thống

**Các node mô phỏng:**

Hệ thống mô phỏng 4 site:

- `Coordinator`
- `NodeA`
- `NodeB`
- `NodeC`

Sơ đồ:

```text
                 +----------------+
                 |  Coordinator   |
                 +--------+-------+
                          |
          ---------------------------------
          |               |               |
      +---v---+       +---v---+       +---v---+
      | NodeA |       | NodeB |       | NodeC |
      +-------+       +-------+       +-------+
```

**Tầng giao tiếp:**

- Workload chính dùng Python method calls để mô phỏng giao tiếp Coordinator -> Participants.
- Demo crash dùng `multiprocessing.Process` và `multiprocessing.Queue` để mô phỏng các site chạy trong process riêng.
- Communication delay được mô phỏng bằng `time.sleep()` trong participant node.

**Lưu trữ vật lý:**

- Dataset: JSONL file trong `data/`.
- Durable logs: JSONL files trong `logs/`.
- Checkpoint snapshots: JSON files trong `snapshots/`.
- Metrics: JSON và CSV files trong `metrics/`.
- Global transaction table: `data/global_tx_table.json`.

## 5. Công Nghệ Và Kế Hoạch Cài Đặt

**Ngôn ngữ lập trình:** Python 3

**Môi trường triển khai:** Mô phỏng trên một laptop / localhost

**Thư viện sử dụng:**

- Python standard library
- `multiprocessing`
- `json`
- `csv`
- `pathlib`
- `argparse`

Không cần DBMS thật hoặc framework web vì trọng tâm là mô phỏng thuật toán reliability trong cơ sở dữ liệu phân tán.

**Kế hoạch cài đặt:**

1. Sinh dataset 100,000 giao dịch HFT.
2. Cài đặt model state, message, log record.
3. Cài đặt Coordinator và Participant theo 2PC.
4. Ghi durable JSONL log cho từng site.
5. Tạo local checkpoint từ durable log.
6. Tạo global checkpoint và tính `global_safe_point`.
7. Cài đặt log pruning dựa trên safe point và protected transactions.
8. Cài đặt crash scenario sau `READY`.
9. Cài đặt recovery tổng thể cho các transaction in-doubt.
10. Ghi metrics về disk space saved và recovery result.

## 6. Metric Thành Công Và Phân Tích

### Metric định lượng

Metric chính của đề tài:

```text
Disk space saved after each checkpointing cycle
```

Công thức:

```text
saved_bytes = before_bytes - after_bytes
saved_percent = saved_bytes / before_bytes * 100
```

Output metric:

```text
metrics/prune_checkpoint_<id>_summary.json
metrics/checkpoint_metrics.csv
```

Metric recovery:

```text
metrics/recovery_summary.json
```

Các trường quan trọng:

- `total_in_doubt_before`
- `total_resolved`
- `total_unresolved`
- `total_remaining_in_doubt`

### Kịch bản lỗi

Failure chính:

```text
NodeB crash sau khi ghi READY nhưng trước khi nhận global decision.
```

Kịch bản:

1. Coordinator gửi `PREPARE`.
2. NodeA vote commit.
3. NodeB ghi `READY`, gửi `VOTE_COMMIT`, rồi process crash.
4. NodeC vote abort.
5. Coordinator quyết định `GLOBAL_ABORT`.
6. Checkpoint đánh dấu transaction của NodeB là protected.
7. Log pruning không xóa READY log.
8. RecoveryManager đọc durable log và global transaction table.
9. NodeB ghi `ABORT`.
10. Sau recovery, không còn transaction in-doubt.

Script demo:

```bash
python scripts/run_multiprocessing_failure_demo.py --checkpoint-id 100 --tx-index 1001
```

### Bằng chứng mong đợi

Video demo 3-5 phút sẽ chứng minh:

- sinh dataset 100,000 transaction;
- chạy workload 2PC;
- tạo local checkpoint và global checkpoint;
- tính `global_safe_point`;
- prune log và đo disk space saved;
- recovery các transaction in-doubt;
- mô phỏng NodeB process crash bằng `multiprocessing`;
- NodeB phục hồi đúng từ durable log.

## 7. Các Mốc Thực Hiện

**Mốc 1 (Week 5): Chuẩn bị môi trường và dataset**

- Tạo repository GitHub.
- Sinh dataset 100,000 transaction.
- Thiết kế schema transaction.
- Tạo cấu trúc thư mục `src/`, `scripts/`, `docs/`, `logs/`, `metrics/`, `snapshots/`.

**Mốc 2 (Week 8): Cài đặt thuật toán chính**

- Cài đặt 2PC Coordinator và Participant.
- Ghi durable JSONL log.
- Cài đặt local checkpoint và global checkpoint.
- Tính `global_safe_point`.

**Mốc 3 (Week 12): Xử lý lỗi và đo metric**

- Cài đặt safe log pruning.
- Ghi metrics `saved_bytes` và `saved_percent`.
- Mô phỏng crash sau `READY`.
- Cài đặt recovery tổng thể cho các transaction in-doubt.
- Thêm demo `multiprocessing`.
- Hoàn thiện README, design document, analysis report và video demo.
