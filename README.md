# Log Pruning and Global Checkpointing

## Đề tài

**Log Pruning and Checkpointing: High-Frequency Trading**

Repository GitHub: https://github.com/masterdykun-code/distributed-log-checkpointing

Project mô phỏng cơ chế **global checkpointing**, **safe point** và **log pruning** trong hệ cơ sở dữ liệu phân tán. Workload được sinh từ dataset 100,000 giao dịch chứng khoán tần suất cao. Hệ thống dùng Two-Phase Commit (2PC), durable log, checkpoint và recovery để chứng minh rằng log chỉ được xóa khi không còn cần cho phục hồi sau crash.

## Mục tiêu

- Sinh dataset 100,000 transaction.
- Mô phỏng một Coordinator và ba participant site: NodeA, NodeB, NodeC.
- Cài đặt các trạng thái 2PC quan trọng: `READY`, `COMMIT`, `ABORT`, `END`.
- Ghi durable log JSONL cho từng site.
- Tạo local checkpoint và global checkpoint.
- Tính `global_safe_point = min(last_checkpointed_gseq của các site)`.
- Prune log an toàn và đo `saved_bytes`, `saved_percent`.
- Mô phỏng crash sau `READY` và recovery bằng `RecoveryManager`.
- Dùng `multiprocessing` để demo NodeB là process riêng bị crash.

## Cấu trúc thư mục

```text
data/
  transactions_100k.jsonl        Dataset 100,000 transaction
  dataset_summary.json           Thống kê dataset
  global_tx_table.json           Bảng quyết định toàn cục, sinh khi chạy demo

logs/
  .gitkeep                       Giữ thư mục trên Git
  *.log                          Durable logs, sinh khi chạy demo

metrics/
  .gitkeep                       Giữ thư mục trên Git
  *.json, *.csv                  Metrics sinh khi chạy demo

snapshots/
  .gitkeep                       Giữ thư mục trên Git
  *.json                         Checkpoint snapshots sinh khi chạy demo

docs/
  project_proposal.md            Đề xuất đề tài
  design.md                      Tài liệu thiết kế
  analysis_report.md             Báo cáo phân tích lý thuyết

scripts/
  generate_dataset.py                 Sinh dataset 100,000 transaction dạng JSONL
  run_workload.py                     Chạy workload 2PC từ dataset, hỗ trợ abort-rate và crash-rate
  run_checkpoint_demo.py              Tạo local checkpoint cho NodeA, NodeB, NodeC
  run_global_checkpoint.py            Tạo global checkpoint và tính global_safe_point
  run_delayed_checkpoint_demo.py      Demo site chậm giới hạn safe point bằng multiprocessing
  run_log_pruning.py                  Prune log an toàn và ghi metric dung lượng tiết kiệm
  run_recovery_demo.py                Recovery tổng thể cho NodeA, NodeB, NodeC sau pruning
  run_failure_demo.py                 Demo recovery ở mức object khi NodeB crash sau READY
  run_multiprocessing_failure_demo.py Demo NodeB process crash bằng multiprocessing

src/
  coordinator.py                      Cài đặt Coordinator của giao thức 2PC
  node.py                             Cài đặt ParticipantNode, xử lý PREPARE/COMMIT/ABORT
  log_manager.py                      Ghi, đọc, checkpoint summary và prune durable JSONL log
  checkpoint_manager.py               Gom local checkpoint, tính safe point và protected transactions
  recovery_manager.py                 Phục hồi participant từ READY/in-doubt sang COMMIT hoặc ABORT
  models.py                           Định nghĩa state, message, log record và checkpoint metadata

tests/
  test_checkpointing.py               Kiểm thử high-watermark và phép min của safe point
```

`logs/*.log`, `metrics/*` và `snapshots/*` là artifact sinh từ demo nên được ignore trong Git. Repo chỉ giữ các thư mục này bằng `.gitkeep`.

## Cài đặt

Project chỉ dùng Python standard library.

```bash
python --version
```

Nếu dùng virtual environment:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 1. Sinh dataset

```bash
python scripts/generate_dataset.py --records 100000
```

Output:

```text
data/transactions_100k.jsonl
data/dataset_summary.json
```

## 2. Chạy workload 2PC

Chạy 1,000 transaction từ dataset thật:

```bash
python scripts/run_workload.py --limit 1000 --reset --fast --abort-rate 0.1 --crash-rate 0.01
```

Ý nghĩa:

- `--abort-rate 0.1`: khoảng 10% transaction có một participant vote abort.
- `--crash-rate 0.01`: khoảng 1% transaction có một participant ghi `READY` rồi crash trước global decision.
- `--fast`: tắt communication delay để quay video nhanh hơn.

Output chính:

```text
logs/Coordinator.log
logs/NodeA.log
logs/NodeB.log
logs/NodeC.log
metrics/workload_summary.json
data/global_tx_table.json
```

Nếu muốn chạy toàn bộ dataset:

```bash
python scripts/run_workload.py --limit 100000 --reset --fast --abort-rate 0.1 --crash-rate 0.001 --progress-every 10000
```

## 3. Tạo local checkpoint

```bash
python scripts/run_checkpoint_demo.py --checkpoint-id 1
```

Output:

```text
snapshots/NodeA_checkpoint_1.json
snapshots/NodeB_checkpoint_1.json
snapshots/NodeC_checkpoint_1.json
metrics/local_checkpoint_1_summary.json
```

Mỗi local checkpoint lưu:

- `last_checkpointed_gseq`
- `observed_max_gseq`
- `previous_high_watermark`
- `active_tx_ids`
- `in_doubt_tx_ids`
- `log_size_before`

Nếu workload có crash sau `READY`, checkpoint sẽ có `in_doubt_tx_count > 0`.

`last_checkpointed_gseq` là high-watermark không giảm. Giá trị này được
lưu riêng trong `snapshots/<site>_checkpoint_state.json`, nên sau khi log cũ
đã bị prune, checkpoint tiếp theo không bị lùi về `gseq` thấp hơn. Khi chạy
workload với `--reset`, high-watermark cũng được reset về 0.

## 4. Tạo global checkpoint

```bash
python scripts/run_global_checkpoint.py --checkpoint-id 1
```

Output:

```text
snapshots/global_checkpoint_1.json
metrics/global_checkpoint_1_summary.json
```

Safe point toàn cục:

```text
global_safe_point = min(
    NodeA.last_checkpointed_gseq,
    NodeB.last_checkpointed_gseq,
    NodeC.last_checkpointed_gseq
)
```

Trong workload đồng bộ 2PC, nếu cả ba node đã xử lý đến 1000 thì `global_safe_point = 1000`. Nếu một site checkpoint thấp hơn, global safe point sẽ bị giới hạn bởi site đó.

Global checkpoint cũng gom:

```text
protected_tx_ids = active_tx_ids union in_doubt_tx_ids
```

Các transaction trong `protected_tx_ids` không được prune.

## 5. Demo site xử lý chậm

Demo này dùng ba `multiprocessing.Process` đọc cùng dataset thật. NodeB được
cấu hình delay lớn hơn, sau đó checkpoint được yêu cầu khi các process vẫn
đang xử lý:

```bash
python scripts/run_delayed_checkpoint_demo.py --limit 1000 --slow-site NodeB --slow-delay 0.005 --checkpoint-after 1 --checkpoint-id 50
```

Kết quả được đo từ log thực tế, không hard-code tiến độ. Ví dụ:

```text
NodeA.last_checkpointed_gseq = 199
NodeB.last_checkpointed_gseq = 84
NodeC.last_checkpointed_gseq = 198
global_safe_point = min(199, 84, 198) = 84
```

Output:

```text
logs/delayed_site_demo/
metrics/delayed_site_demo/
metrics/delayed_site_demo_summary.json
snapshots/delayed_site_demo/
```

Demo dùng thư mục riêng nên không ghi đè log của workload chính. Mục đích của
nó là chứng minh site chậm nhất giới hạn safe point; `--abort-rate` và
`--crash-rate` vẫn được kiểm thử trong `run_workload.py`.

## 6. Prune log an toàn

```bash
python scripts/run_log_pruning.py --checkpoint-id 1 --include-coordinator
```

Output:

```text
metrics/prune_checkpoint_1_summary.json
metrics/checkpoint_metrics.csv
```

Một log record chỉ được xóa nếu:

- `gseq <= global_safe_point`
- transaction đã final: `COMMIT`, `ABORT`, hoặc `END`
- transaction không nằm trong `protected_tx_ids`
- transaction không còn ở `READY` / in-doubt

Metric chính:

```text
saved_bytes = before_bytes - after_bytes
saved_percent = saved_bytes / before_bytes * 100
```

## 7. Recovery tổng thể sau pruning

Sau khi pruning, các transaction đang `READY` / in-doubt vẫn được giữ lại trong log vì chúng nằm trong `protected_tx_ids`. Lệnh sau phục hồi toàn bộ participant bằng cách đọc durable log và `data/global_tx_table.json`:

```bash
python scripts/run_recovery_demo.py --fail-on-unresolved
```

Output:

```text
metrics/recovery_summary.json
```

Ý nghĩa các trường chính:

- `total_in_doubt_before`: tổng số transaction in-doubt trước recovery.
- `total_resolved`: số transaction đã được đưa về `COMMIT` hoặc `ABORT`.
- `total_unresolved`: số transaction chưa có global decision để recovery.
- `total_remaining_in_doubt`: số transaction còn in-doubt sau recovery.

Khi demo thành công, giá trị quan trọng là:

```text
total_remaining_in_doubt = 0
```

Điều này chứng minh rằng pruning không xóa các log cần cho recovery.

## 8. Demo crash bằng multiprocessing

Script này dùng transaction thật từ `data/transactions_100k.jsonl`. Mặc định `--tx-index 1001`, phù hợp khi trước đó workload demo đã chạy 1000 transaction.

```bash
python scripts/run_multiprocessing_failure_demo.py --checkpoint-id 100 --tx-index 1001
```

Luồng demo mặc định:

```text
NodeA vote commit
NodeB ghi READY, gửi VOTE_COMMIT, rồi process crash
NodeC vote abort
Coordinator quyết định GLOBAL_ABORT
Checkpoint đánh dấu transaction của NodeB là in-doubt/protected
Pruning không xóa READY log
RecoveryManager đọc global_tx_table và ghi ABORT cho NodeB
```

Output:

```text
metrics/multiprocessing_failure_demo_summary.json
snapshots/global_checkpoint_100.json
```

Cần kiểm tra:

```text
process_exitcodes.NodeB = 2
nodeb_ready_log_preserved_after_pruning = true
recovery_result.remaining_in_doubt_tx_ids = []
```

Nếu muốn chạy crash demo độc lập, thêm `--reset`:

```bash
python scripts/run_multiprocessing_failure_demo.py --checkpoint-id 100 --tx-index 1 --reset
```

## Kịch bản quay demo đề xuất

```bash
python scripts/generate_dataset.py --records 100000
python scripts/run_workload.py --limit 1000 --reset --fast --abort-rate 0.1 --crash-rate 0.01
python scripts/run_checkpoint_demo.py --checkpoint-id 1
python scripts/run_global_checkpoint.py --checkpoint-id 1
python scripts/run_delayed_checkpoint_demo.py --limit 1000 --slow-site NodeB --slow-delay 0.005 --checkpoint-after 1 --checkpoint-id 50
python scripts/run_log_pruning.py --checkpoint-id 1 --include-coordinator
python scripts/run_recovery_demo.py --fail-on-unresolved
python scripts/run_multiprocessing_failure_demo.py --checkpoint-id 100 --tx-index 1001
```

Khi trình bày, tập trung vào 4 tiêu chí trong rubric:

- **State Accuracy**: `READY`, `COMMIT`, `ABORT`, `END` đi đúng transition 2PC.
- **Failure Handling**: NodeB crash sau `READY`, recovery từ durable log.
- **Log Management**: log JSONL có `lsn`, `gseq`, `tx_id`, `site`, `state`, `event`.
- **Textbook Alignment**: 2PC chỉ commit khi tất cả vote commit; `READY` là in-doubt; durable log cần cho recovery.

## Kiểm thử

```bash
python -m compileall src scripts tests
python -m unittest discover -s tests -v
```

Unit test kiểm tra hai thuộc tính chính:

- local checkpoint high-watermark không giảm sau pruning;
- global safe point bằng giá trị nhỏ nhất giữa các local checkpoint.

## Liên hệ lý thuyết

Project chọn 2PC vì phù hợp với bài toán reliability:

- Coordinator quyết định `GLOBAL_COMMIT` nếu tất cả participant vote commit.
- Coordinator quyết định `GLOBAL_ABORT` nếu có participant vote abort hoặc lỗi.
- Participant ở `READY` không được tự commit/abort nếu chưa biết global decision.
- 3PC có thêm pha `PRECOMMIT` để giảm blocking, nhưng project tập trung vào checkpointing, safe pruning và recovery với 2PC.
