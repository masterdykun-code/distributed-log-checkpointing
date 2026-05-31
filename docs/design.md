# Tài liệu thiết kế

## 1. Tổng quan

Project mô phỏng cơ chế reliability trong cơ sở dữ liệu phân tán cho workload giao dịch chứng khoán tần suất cao. Hệ thống gồm một Coordinator và ba participant site: NodeA, NodeB, NodeC.

Mục tiêu thiết kế:

- xử lý giao dịch phân tán bằng Two-Phase Commit (2PC);
- ghi durable log cho từng site;
- tạo local checkpoint và global checkpoint;
- tính safe point để prune log an toàn;
- bảo vệ transaction đang `READY` / in-doubt;
- phục hồi node bị crash từ durable log và global transaction table.

## 2. Giao thức 2PC

Coordinator điều phối mỗi transaction theo 2 phase.

Phase 1: Prepare/Vote

```text
Coordinator -> PREPARE -> participants
Participant -> VOTE_COMMIT hoặc VOTE_ABORT
```

Phase 2: Global Decision

```text
Coordinator -> GLOBAL_COMMIT nếu tất cả vote commit
Coordinator -> GLOBAL_ABORT nếu có vote abort hoặc participant lỗi
Participant -> COMMIT hoặc ABORT theo quyết định toàn cục
```

## 3. State machine

Coordinator:

```text
INIT -> WAIT -> COMMIT -> END
INIT -> WAIT -> ABORT  -> END
```

Participant:

```text
INIT  -> READY
INIT  -> ABORT
READY -> COMMIT
READY -> ABORT
```

`READY` là trạng thái quan trọng nhất. Participant đã ghi durable log và đã vote commit, nhưng chưa biết global decision. Nếu participant crash tại đây, transaction bị in-doubt và phải được recovery.

## 4. Durable log

Mỗi site có một file log riêng trong `logs/`:

```text
Coordinator.log
NodeA.log
NodeB.log
NodeC.log
```

Mỗi dòng là một JSON object gồm các trường chính:

- `lsn`: local log sequence number;
- `gseq`: global sequence number do Coordinator cấp;
- `tx_id`: mã transaction;
- `site`: site ghi log;
- `role`: `COORDINATOR` hoặc `PARTICIPANT`;
- `state`: `READY`, `COMMIT`, `ABORT`, `END`, ...;
- `event`: sự kiện tạo log record;
- `details`: thông tin bổ sung.

Log được flush và fsync sau mỗi append để mô phỏng durable log cho crash recovery.

## 5. Local checkpoint

Local checkpoint được tạo bằng cách quét durable log của từng participant.

Mỗi local checkpoint gồm:

- `last_checkpointed_gseq`;
- `active_tx_ids`;
- `in_doubt_tx_ids`;
- `log_size_before`;
- `state_by_tx_count`.

`in_doubt_tx_ids` được lấy từ các transaction có latest state là `READY`.

## 6. Global checkpoint và safe point

Global checkpoint đọc metadata từ local checkpoint của NodeA, NodeB và NodeC.

Safe point toàn cục:

```text
global_safe_point = min(
    NodeA.last_checkpointed_gseq,
    NodeB.last_checkpointed_gseq,
    NodeC.last_checkpointed_gseq
)
```

Lý do lấy min: hệ thống chỉ an toàn đến mốc mà tất cả site đều đã checkpoint. Nếu một site checkpoint thấp hơn, log sau mốc đó có thể vẫn cần cho site này khi recovery.

Trong demo workload đồng bộ 2PC, sau khi xử lý 1000 transaction, thường cả ba site cùng có:

```text
NodeA.last_checkpointed_gseq = 1000
NodeB.last_checkpointed_gseq = 1000
NodeC.last_checkpointed_gseq = 1000
global_safe_point = 1000
```

Nếu workload có crash sau `READY`, `global_safe_point` vẫn có thể là 1000, nhưng transaction in-doubt sẽ nằm trong `protected_tx_ids` và không bị prune.

## 7. Protected transactions

Global checkpoint tạo:

```text
protected_tx_ids = active_tx_ids union in_doubt_tx_ids
```

Transaction trong `protected_tx_ids` không được xóa log, kể cả khi `gseq <= global_safe_point`.

Quy tắc này bảo vệ các transaction đang `READY`, vì chúng cần durable log để recovery.

## 8. Log pruning

Một log record chỉ được prune nếu thỏa tất cả điều kiện:

```text
gseq <= global_safe_point
transaction đã final: COMMIT, ABORT, hoặc END
transaction không nằm trong protected_tx_ids
```

Metric được ghi sau mỗi pruning cycle:

```text
before_bytes
after_bytes
saved_bytes
saved_percent
```

## 9. Recovery Manager

`RecoveryManager` phục hồi participant sau crash theo các bước:

1. Đọc durable log của participant.
2. Tìm các transaction có latest state là `READY`.
3. Đọc `data/global_tx_table.json`.
4. Nếu global decision là `COMMIT`, ghi log `COMMIT`.
5. Nếu global decision là `ABORT`, ghi log `ABORT`.
6. Nếu chưa có decision, giữ transaction trong `READY` và tiếp tục bảo vệ log.

`scripts/run_recovery_demo.py` dùng `RecoveryManager` để recovery tổng thể cho NodeA, NodeB và NodeC sau bước pruning.

Lệnh chạy:

```bash
python scripts/run_recovery_demo.py --fail-on-unresolved
```

Script ghi kết quả vào:

```text
metrics/recovery_summary.json
```

Kết quả mong đợi:

```text
total_in_doubt_before > 0
total_resolved = total_in_doubt_before
total_remaining_in_doubt = 0
```

## 10. Crash demo bằng multiprocessing

`scripts/run_multiprocessing_failure_demo.py` dùng `multiprocessing` để tạo process riêng cho NodeA, NodeB và NodeC.

Demo mặc định dùng transaction thật từ dataset:

```text
NodeA vote commit
NodeB ghi READY, gửi VOTE_COMMIT, rồi process crash
NodeC vote abort
Coordinator ghi GLOBAL_ABORT
NodeA và NodeC nhận GLOBAL_ABORT
NodeB chưa nhận global decision nên ở READY / in-doubt
Checkpoint đánh dấu transaction của NodeB là protected
Pruning không xóa READY log của NodeB
RecoveryManager ghi ABORT cho NodeB
```

Lệnh chạy:

```bash
python scripts/run_multiprocessing_failure_demo.py --checkpoint-id 100 --tx-index 1001
```

## 11. Hướng demo chính

Demo chính không dùng metadata synthetic. Tất cả checkpoint chính được tạo từ log thật sinh ra bởi workload và crash demo.

```bash
python scripts/generate_dataset.py --records 100000
python scripts/run_workload.py --limit 1000 --reset --fast --abort-rate 0.1 --crash-rate 0.01
python scripts/run_checkpoint_demo.py --checkpoint-id 1
python scripts/run_global_checkpoint.py --checkpoint-id 1
python scripts/run_log_pruning.py --checkpoint-id 1 --include-coordinator
python scripts/run_recovery_demo.py --fail-on-unresolved
python scripts/run_multiprocessing_failure_demo.py --checkpoint-id 100 --tx-index 1001
```

## 12. Liên hệ 2PC/3PC

Project cài đặt 2PC. 2PC có thể bị blocking khi participant ở `READY` mà chưa biết global decision. Vì vậy durable log và recovery protocol là bắt buộc.

3PC thêm pha `PRECOMMIT` để giảm blocking trong một số giả định về failure và network. Project không cài đặt 3PC vì trọng tâm đề tài là global checkpointing, safe point và log pruning.
