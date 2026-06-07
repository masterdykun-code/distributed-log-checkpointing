# Log Pruning and Global Checkpointing

Đề tài #40 mô phỏng global checkpoint, safe log pruning và recovery cho
100.000 giao dịch High-Frequency Trading.

Repository: https://github.com/masterdykun-code/distributed-log-checkpointing

## Chức năng chính

- Mô phỏng Coordinator và ba participant: NodeA, NodeB, NodeC.
- Chạy Two-Phase Commit (2PC) với `READY`, `COMMIT`, `ABORT`, `END`.
- Ghi durable log JSONL riêng cho từng site.
- Tạo local checkpoint và global checkpoint.
- Tính:

```text
global_safe_point = min(
    NodeA.last_checkpointed_gseq,
    NodeB.last_checkpointed_gseq,
    NodeC.last_checkpointed_gseq
)
```

- Prune log an toàn và đo dung lượng tiết kiệm.
- Mô phỏng NodeB process crash thật sau `READY`, sau đó tự động recovery.

## Cấu trúc

```text
data/
  transactions_100k.jsonl             Dataset 100.000 transaction
  dataset_summary.json                Thống kê dataset
  global_tx_table.json                Quyết định toàn cục của Coordinator

src/
  coordinator.py                      Điều phối 2PC
  node.py                             Participant và local checkpoint
  log_manager.py                      Durable log và log pruning
  checkpoint_manager.py               Global checkpoint và safe point
  recovery_manager.py                 Recovery transaction in-doubt
  models.py                           State, message và metadata

scripts/
  generate_dataset.py                 Sinh dataset
  run_workload.py                     Chạy workload 2PC
  run_checkpoint_demo.py              Tạo local checkpoint
  run_global_checkpoint.py            Tạo global checkpoint
  run_log_pruning.py                  Prune log và ghi metric
  run_multiprocessing_failure_demo.py Demo process crash và tự recovery

docs/
  project_proposal.md                 Project proposal
  design.md                           Tài liệu thiết kế
  analysis_report.md                  Báo cáo phân tích lý thuyết
```

`logs/`, `metrics/` và `snapshots/` chứa artifact được sinh khi chạy.

## Cài đặt

Project chỉ dùng Python standard library.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Quy trình chính

### 1. Sinh dataset

```powershell
python scripts/generate_dataset.py --records 100000
```

### 2. Chạy workload

```powershell
python scripts/run_workload.py --limit 1000 --reset --fast --abort-rate 0.1
```

- `--abort-rate 0.1`: khoảng 10% transaction có participant vote abort.
- `--fast`: tắt delay để chạy nhanh.
- Workload xử lý tuần tự; transaction sau bắt đầu khi transaction trước đã
  nhận global decision.

### 3. Tạo checkpoint

```powershell
python scripts/run_checkpoint_demo.py --checkpoint-id 1
python scripts/run_global_checkpoint.py --checkpoint-id 1
```

Local checkpoint phân biệt:

- `observed_max_gseq`: giao dịch lớn nhất site đã quan sát.
- `contiguous_final_gseq`: prefix liên tục đã `COMMIT` hoặc `ABORT`.
- `last_checkpointed_gseq`: safe prefix được dùng để tính global safe point.
- `in_doubt_tx_ids`: transaction còn ở `READY`.

Với workload bình thường 1.000 transaction:

```text
NodeA = 1000
NodeB = 1000
NodeC = 1000
global_safe_point = 1000
```

### 4. Prune log

```powershell
python scripts/run_log_pruning.py --checkpoint-id 1 --include-coordinator
```

Một record chỉ bị xóa khi:

```text
gseq <= global_safe_point
transaction đã final
transaction không nằm trong protected_tx_ids
```

Metric được lưu tại:

```text
metrics/prune_checkpoint_1_summary.json
metrics/checkpoint_metrics.csv
```

Kết quả kiểm thử workload 1.000 transaction:

```text
total_saved_bytes = 4.334.686
total_saved_percent = 99,97%
```

## Demo crash và recovery

Sau workload 1.000 transaction, chạy:

```powershell
python scripts/run_multiprocessing_failure_demo.py --checkpoint-id 100 --tx-index 1001
```

Luồng demo:

```text
NodeA vote commit
NodeB ghi READY, gửi VOTE_COMMIT rồi process chết bằng os._exit(2)
NodeC vote abort
Coordinator ghi GLOBAL_ABORT
NodeB không thể nhận thêm message khi process đã chết
Checkpoint bảo vệ READY log của NodeB
Pruning không xóa transaction protected
RecoveryManager tự động áp dụng GLOBAL_ABORT cho NodeB
```

Kết quả mong đợi:

```text
NodeA safe point = 1001
NodeB safe point = 1000
NodeC safe point = 1001
global_safe_point = 1000

process_exitcodes.NodeB = 2
nodeb_alive_after_crash = false
nodeb_ready_log_preserved_after_pruning = true
remaining_in_doubt_tx_ids = []
```

Demo độc lập:

```powershell
python scripts/run_multiprocessing_failure_demo.py --checkpoint-id 100 --tx-index 1 --reset
```

## Kiểm thử

```powershell
python -m compileall src scripts tests
python -m unittest discover -s tests -v
```

Ba test kiểm tra:

- local high-watermark không giảm sau pruning;
- safe prefix dừng trước transaction chưa final;
- global safe point bằng minimum của các local checkpoint.

## Tài liệu nộp

- [Project proposal](docs/project_proposal.md)
- [Tài liệu thiết kế](docs/design.md)
- [Báo cáo phân tích](docs/analysis_report.md)
