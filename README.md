# Log Pruning and Global Checkpointing

## Đề Tài

**Log Pruning and Checkpointing: High-Frequency Trading**

Repository GitHub: https://github.com/masterdykun-code/distributed-log-checkpointing

Đề tài mô phỏng cơ chế **global checkpointing** và **safe log pruning** trong hệ cơ sở dữ liệu phân tán. Hệ thống dùng dữ liệu giao dịch chứng khoán tần suất cao để tạo log giao dịch, tạo checkpoint, tính **safe point**, sau đó xóa những log đã an toàn mà không làm mất khả năng phục hồi sau lỗi.

## Mục Tiêu

- Sinh dataset gồm 100,000 transaction logs.
- Mô phỏng một hệ phân tán gồm một Coordinator và ba site: NodeA, NodeB, NodeC.
- Cài đặt các trạng thái quan trọng của Two-Phase Commit: `READY`, `COMMIT`, `ABORT`, `END`.
- Ghi durable log dạng JSONL cho từng site.
- Tạo local checkpoint tại từng node.
- Tạo global checkpoint và tính `global_safe_point`.
- Xóa log an toàn dựa trên safe point và protected transactions.
- Đo dung lượng đĩa tiết kiệm được sau mỗi checkpointing cycle.
- Demo lỗi NodeB crash sau trạng thái `READY` và phục hồi từ log.

## Kiến Trúc Hệ Thống

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

Các thành phần chính:

- `Coordinator`: quản lý Two-Phase Commit, quyết định `GLOBAL_COMMIT` hoặc `GLOBAL_ABORT`, lưu global transaction table.
- `NodeA`, `NodeB`, `NodeC`: mô phỏng các participant site, ghi log, tạo checkpoint, phục hồi trạng thái.
- `LogManager`: ghi, đọc và prune durable JSONL logs.
- `GlobalCheckpointManager`: gom local checkpoint, tính safe point, tạo global checkpoint.
- `Recovery`: phục hồi node từ durable log khi có transaction ở trạng thái `READY`.

## Cấu Trúc Thư Mục

```text
data/
  transactions_100k.jsonl        Dataset 100,000 giao dịch
  dataset_summary.json           Thống kê dataset
  global_tx_table.json           Bảng quyết định toàn cục của Coordinator

docs/
  project_proposal.md            Đề xuất đề tài
  design.md                      Tài liệu thiết kế
  analysis_report.md             Phân tích liên hệ lý thuyết Özsu và Valduriez
  day_notes.md                   Ghi chú quá trình làm

logs/
  Coordinator.log                Log của Coordinator
  NodeA.log                      Log của NodeA
  NodeB.log                      Log của NodeB
  NodeC.log                      Log của NodeC

metrics/
  workload_summary.json          Kết quả chạy workload
  checkpoint_metrics.csv         Metric dung lượng tiết kiệm sau pruning
  failure_demo_summary.json      Kết quả demo failure recovery
  multiprocessing_failure_demo_summary.json
                                  Kết quả demo crash bằng multiprocessing

snapshots/
  *_checkpoint_*.json            Local và global checkpoint snapshots

scripts/
  generate_dataset.py            Sinh dataset
  run_workload.py                Chạy workload 2PC
  run_checkpoint_demo.py         Tạo local checkpoint
  run_global_checkpoint.py       Tạo global checkpoint
  run_log_pruning.py             Prune log theo safe point
  run_failure_demo.py            Demo NodeB crash và recovery
  run_multiprocessing_failure_demo.py
                                  Demo NodeB process crash bằng multiprocessing

src/
  coordinator.py                 Coordinator 2PC
  node.py                        Participant node
  log_manager.py                 Durable log manager
  checkpoint_manager.py          Global checkpoint manager
  models.py                      State, message, log record models
  recovery_manager.py            Thành phần phục hồi
```

## Cài Đặt

Project chỉ dùng Python standard library, không cần thư viện ngoài.

```bash
python --version
```

Nếu muốn dùng virtual environment:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Sinh Dataset

Chạy:

```bash
python scripts/generate_dataset.py --records 100000
```

Kết quả:

```text
data/transactions_100k.jsonl
data/dataset_summary.json
```

Dataset gồm 100,000 giao dịch chứng khoán tần suất cao. Mỗi record có dạng:

```json
{
  "tx_id": "TX000001",
  "account_id": "ACC0001",
  "symbol": "AAPL",
  "side": "BUY",
  "quantity": 100,
  "price": 187.25,
  "timestamp": "2026-05-16T10:00:00.001000+00:00"
}
```

## Chạy Kiểm Thử 2PC Đơn Giản

Trường hợp tất cả participant đều commit:

```bash
python -c "from src.coordinator import Coordinator; c=Coordinator(); c.clear_all_logs(); tx={'tx_id':'TX000101','account_id':'ACC0101','symbol':'AAPL','side':'BUY','quantity':100,'price':187.25,'timestamp':'test'}; result=c.execute_transaction(tx); print(result)"
```

Trường hợp một participant abort:

```bash
python -c "from src.coordinator import Coordinator; c=Coordinator(); c.clear_all_logs(); tx={'tx_id':'TX000102','account_id':'ACC0102','symbol':'TSLA','side':'SELL','quantity':50,'price':175.80,'timestamp':'test'}; result=c.execute_transaction(tx, can_commit_by_site={'NodeB': False}); print(result)"
```

## Chạy Workload

Chạy 1,000 giao dịch:

```bash
python scripts/run_workload.py --limit 1000 --reset --fast
```

Chạy 10,000 giao dịch:

```bash
python scripts/run_workload.py --limit 10000 --reset --fast --progress-every 1000
```

Chạy toàn bộ 100,000 giao dịch:

```bash
python scripts/run_workload.py --limit 100000 --reset --fast --abort-rate 0.1 --progress-every 10000
```

Kết quả được ghi vào:

```text
metrics/workload_summary.json
logs/Coordinator.log
logs/NodeA.log
logs/NodeB.log
logs/NodeC.log
```

## Tạo Local Checkpoint

Sau khi chạy workload, tạo local checkpoint cho từng participant:

```bash
python scripts/run_checkpoint_demo.py --checkpoint-id 1
```

Kết quả:

```text
snapshots/NodeA_checkpoint_1.json
snapshots/NodeB_checkpoint_1.json
snapshots/NodeC_checkpoint_1.json
metrics/local_checkpoint_1_summary.json
```

Mỗi local checkpoint lưu:

- `last_checkpointed_gseq`
- `active_tx_ids`
- `in_doubt_tx_ids`
- số lượng transaction state đã checkpoint
- thời điểm tạo checkpoint

## Tạo Global Checkpoint

Sau khi có local checkpoint, tạo global checkpoint:

```bash
python scripts/run_global_checkpoint.py --checkpoint-id 1
```

Kết quả:

```text
snapshots/global_checkpoint_1.json
metrics/global_checkpoint_1_summary.json
```

Safe point toàn cục được tính như sau:

```text
global_safe_point = min(
    NodeA.last_checkpointed_gseq,
    NodeB.last_checkpointed_gseq,
    NodeC.last_checkpointed_gseq
)
```

Ý nghĩa: hệ thống chỉ được prune log tại các vị trí không vượt quá mốc mà tất cả site đã checkpoint an toàn.

## Prune Log An Toàn

Sau khi có global checkpoint:

```bash
python scripts/run_log_pruning.py --checkpoint-id 1 --include-coordinator
```

Kết quả:

```text
metrics/prune_checkpoint_1_summary.json
metrics/checkpoint_metrics.csv
```

Một log record chỉ được xóa nếu thỏa tất cả điều kiện:

- `gseq <= global_safe_point`
- transaction đã đạt trạng thái cuối: `COMMIT`, `ABORT`, hoặc `END`
- transaction không còn active
- transaction không ở trạng thái `READY` / in-doubt
- transaction không nằm trong `protected_tx_ids`

Metric chính:

```text
saved_bytes = before_bytes - after_bytes
saved_percent = saved_bytes / before_bytes * 100
```

## Demo Failure Recovery

Kịch bản demo:

1. Coordinator gửi `PREPARE`.
2. NodeB ghi log `READY`.
3. NodeB crash trước khi nhận `GLOBAL_ABORT`.
4. Global checkpoint đánh dấu transaction đó là protected.
5. Log pruning không xóa READY log của NodeB.
6. NodeB restart và phục hồi từ durable log.
7. NodeB phát hiện transaction in-doubt.
8. NodeB hỏi Coordinator quyết định cuối cùng.
9. NodeB ghi `ABORT` và recovery hoàn tất.

Chạy demo:

```bash
python scripts/run_failure_demo.py --checkpoint-id 99
```

Kết quả:

```text
metrics/failure_demo_summary.json
snapshots/global_checkpoint_99.json
```

Trong kết quả cần kiểm tra:

```text
nodeb_ready_log_preserved_after_pruning = true
global_decision = ABORT
in_doubt_tx_ids contains TX_FAIL_001
```

## Demo Crash Bằng Multiprocessing

Script này tạo process riêng cho NodeA, NodeB và NodeC. NodeB ghi `READY` rồi process thoát ngay để mô phỏng crash thật hơn so với demo object.

Chạy:

```bash
python scripts/run_multiprocessing_failure_demo.py --checkpoint-id 100
```

Kết quả:

```text
metrics/multiprocessing_failure_demo_summary.json
snapshots/global_checkpoint_100.json
```

Trong kết quả cần kiểm tra:

```text
process_exitcodes.NodeB = 2
nodeb_ready_log_preserved_after_pruning = true
recovery_result.decisions_applied.TX_MP_FAIL_001 = ABORT
```

## Định Dạng Log

Mỗi site có một file log riêng dạng JSONL. Mỗi dòng là một JSON object.

Ví dụ:

```json
{
  "lsn": 1,
  "gseq": 1,
  "tx_id": "TX000001",
  "site": "NodeB",
  "role": "PARTICIPANT",
  "state": "READY",
  "event": "READY",
  "timestamp": "2026-05-16T10:00:00+00:00",
  "details": {}
}
```

Các trường quan trọng:

- `lsn`: log sequence number cục bộ của từng site.
- `gseq`: global sequence number do Coordinator cấp.
- `tx_id`: mã transaction.
- `site`: site ghi log.
- `role`: `COORDINATOR` hoặc `PARTICIPANT`.
- `state`: trạng thái hiện tại của transaction.
- `event`: sự kiện tạo ra log record.
- `details`: thông tin bổ sung.

## Ý Nghĩa Trạng Thái READY

`READY` là trạng thái quan trọng nhất trong recovery của Two-Phase Commit.

Khi participant ở trạng thái `READY`, nó đã vote commit nhưng chưa biết quyết định toàn cục. Nếu node crash tại thời điểm này, sau khi restart nó phải đọc durable log để biết mình đang in-doubt, sau đó hỏi Coordinator về quyết định cuối cùng.

Vì vậy, log của transaction ở trạng thái `READY` không được prune.

## Liên Hệ Với Lý Thuyết Özsu Và Valduriez

Project áp dụng các khái niệm reliability trong cơ sở dữ liệu phân tán:

- Distributed transaction processing.
- Two-Phase Commit.
- Coordinator và participant roles.
- In-doubt transaction tại trạng thái `READY`.
- Durable logging cho crash recovery.
- Checkpointing để giảm lượng log cần replay.
- Global checkpoint để tìm safe point chung giữa các site.
- Log pruning chỉ an toàn khi transaction đã final và không cần recovery nữa.

Quy tắc 2PC được dùng trong project:

- Coordinator chỉ quyết định `GLOBAL_COMMIT` nếu tất cả participant vote commit.
- Coordinator quyết định `GLOBAL_ABORT` nếu có ít nhất một participant vote abort hoặc bị lỗi.
- Participant không tự quyết định commit toàn cục.
- Participant ở trạng thái `READY` phải giữ log cho đến khi biết quyết định cuối cùng.

## Gợi Ý Quay Video Demo

Video 3-5 phút nên gồm các phần:

1. Giới thiệu đề tài và dataset 100,000 records.
2. Chạy workload để tạo distributed logs.
3. Tạo local checkpoint và global checkpoint.
4. Hiển thị `global_safe_point`.
5. Chạy log pruning và hiển thị disk space saved.
6. Chạy failure demo NodeB crash sau `READY`.
7. Chứng minh READY log không bị xóa và NodeB recovery đúng.

## Lưu Ý

Đây là project mô phỏng phục vụ môn Cơ sở dữ liệu phân tán, không phải hệ thống giao dịch chứng khoán thật. Các script hiện tại mô phỏng nhiều site trên cùng laptop, ghi durable log ra file và mô phỏng crash trong failure demo để chứng minh quy tắc recovery và safe pruning.
