# Đề Xuất Đề Tài Cơ Sở Dữ Liệu Phân Tán

**Mã đề tài & nhóm đánh giá:** #40 - Log Pruning and Checkpointing, Category 4

**Hình thức:** Đề tài cuối kỳ cá nhân

**Sinh viên:** Nguyễn Ngọc Duy

**MSSV:** N23DCCN015

## 1. Project Identity

**Tên đề tài:** Global Checkpointing and Safe Log Pruning for High-Frequency
Trading Transactions

## 2. Objective & Problem Statement

Hệ thống giao dịch tần suất cao tạo ra lượng log lớn. Trong cơ sở dữ liệu phân
tán, xóa log quá sớm có thể làm mất thông tin cần thiết để recovery một
transaction đang `READY`.

Project giải quyết câu hỏi:

```text
Làm sao xác định safe point để xóa log trên tất cả site
mà không làm mất khả năng recovery?
```

Logic cốt lõi:

- Two-Phase Commit (2PC) cho atomic commitment.
- Durable log riêng cho Coordinator, NodeA, NodeB và NodeC.
- Local checkpoint dùng contiguous final prefix.
- Global checkpoint lấy minimum giữa các local safe point.
- Protected set giữ transaction active hoặc in-doubt.
- Recovery đọc durable log và global decision của Coordinator.

## 3. Dataset Specification

**Nguồn:** sinh bằng script của project:

```powershell
python scripts/generate_dataset.py --records 100000
```

**Kích thước:** 100.000 transaction, lưu tại
`data/transactions_100k.jsonl`.

**Schema chính:**

| Trường | Ý nghĩa |
| --- | --- |
| `tx_id` | Mã transaction |
| `account_id` | Mã tài khoản |
| `symbol` | Mã cổ phiếu |
| `side` | `BUY` hoặc `SELL` |
| `quantity` | Số lượng |
| `price` | Giá |
| `timestamp` | Thời điểm giao dịch |

Mỗi transaction được Coordinator gửi đến ba participant. Dữ liệu đầu vào dùng
chung, nhưng trạng thái và durable log được lưu riêng theo từng site.

## 4. System Architecture

Hệ thống gồm bốn site mô phỏng:

```text
Coordinator
   |-- NodeA
   |-- NodeB
   `-- NodeC
```

- Workload chính dùng Python method call để chạy 2PC tuần tự.
- Failure demo dùng `multiprocessing.Process` và `multiprocessing.Queue`.
- NodeB process chết thật bằng `os._exit(2)`.
- Storage sử dụng JSONL log, JSON snapshot và CSV/JSON metric.

## 5. Tech Stack & Implementation Plan

**Ngôn ngữ:** Python 3

**Triển khai:** một laptop / localhost

**Thư viện:** Python standard library, gồm `multiprocessing`, `json`, `csv`,
`pathlib`, `argparse`.

Các bước cài đặt:

1. Sinh dataset 100.000 transaction.
2. Cài đặt state machine và giao thức 2PC.
3. Ghi durable log cho từng site.
4. Tạo local và global checkpoint.
5. Tính contiguous safe prefix và global safe point.
6. Prune log an toàn và ghi metric.
7. Mô phỏng process crash sau `READY`.
8. Recovery tự động từ durable log và global transaction table.

## 6. Success Metrics & Analysis

Metric chính:

```text
saved_bytes = before_bytes - after_bytes
saved_percent = saved_bytes / before_bytes * 100
```

Output:

```text
metrics/prune_checkpoint_<id>_summary.json
metrics/checkpoint_metrics.csv
```

Failure scenario:

```text
NodeB ghi READY và vote commit
NodeB process crash trước khi nhận global decision
Coordinator quyết định GLOBAL_ABORT
Checkpoint bảo vệ READY log
NodeB restart và tự recovery thành ABORT
```

Bằng chứng thành công:

- `process_exitcodes.NodeB = 2`;
- `nodeb_alive_after_crash = false`;
- READY log vẫn tồn tại sau pruning;
- không còn transaction in-doubt sau recovery;
- global safe point bị giới hạn bởi checkpoint của NodeB.

## 7. Project Milestones

**Week 5:** hoàn thành dataset, schema và cấu trúc repository.

**Week 8:** hoàn thành 2PC, durable log, local/global checkpoint.

**Week 12:** hoàn thành pruning metric, crash recovery, tài liệu và video.
