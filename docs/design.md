# Tài Liệu Thiết Kế

## 1. Kiến trúc

Project mô phỏng một Coordinator và ba participant site:

```text
Coordinator
   |-- NodeA
   |-- NodeB
   `-- NodeC
```

Mỗi site có durable log riêng. Workload chính chạy 2PC tuần tự; failure demo
dùng process riêng để mô phỏng NodeB crash thật.

## 2. Two-Phase Commit

Pha 1:

```text
Coordinator gửi PREPARE
Participant ghi READY và VOTE_COMMIT
hoặc ghi ABORT và VOTE_ABORT
```

Pha 2:

```text
Tất cả vote commit -> GLOBAL_COMMIT
Có vote abort hoặc lỗi -> GLOBAL_ABORT
Participant áp dụng quyết định và gửi ACK
Coordinator ghi END
```

State machine:

```text
Coordinator: INIT -> WAIT -> COMMIT/ABORT -> END
Participant: INIT -> READY -> COMMIT/ABORT
Participant: INIT -> ABORT
```

`READY` là trạng thái in-doubt: participant đã vote commit nhưng chưa biết
global decision.

## 3. Durable Log

Mỗi record JSONL gồm:

```text
lsn, gseq, tx_id, site, role, state, event, timestamp, details
```

- `gseq`: thứ tự toàn cục của transaction.
- `lsn`: thứ tự record trong log của một site.
- Log được flush và `fsync` sau mỗi lần ghi.

## 4. Local Checkpoint

Local checkpoint quét durable log và lưu:

```text
observed_max_gseq
previous_high_watermark
contiguous_final_gseq
last_checkpointed_gseq
active_tx_ids
in_doubt_tx_ids
```

Safe prefix là đoạn liên tục mà mọi transaction đều đã `COMMIT` hoặc `ABORT`.
Ví dụ site đã quan sát đến 1001 nhưng transaction 1001 còn `READY`:

```text
observed_max_gseq = 1001
last_checkpointed_gseq = 1000
```

High-watermark được đọc lại từ các snapshot
`snapshots/<site>_checkpoint_<id>.json`, nên không cần file state riêng và
không bị giảm sau pruning.

## 5. Global Checkpoint

```text
global_safe_point = min(local safe point của mọi site)
```

Global checkpoint đồng thời tạo:

```text
protected_tx_ids = active_tx_ids union in_doubt_tx_ids
```

Không được bỏ qua một site bị crash. Hệ thống dùng checkpoint bền vững gần
nhất của site đó làm giới hạn.

## 6. Log Pruning

Một record chỉ được prune khi:

```text
gseq <= global_safe_point
transaction đã final
transaction không thuộc protected_tx_ids
```

Metric của mỗi cycle:

```text
before_bytes
after_bytes
saved_bytes
saved_percent
```

## 7. Crash Và Recovery

Failure demo tạo process riêng cho NodeA, NodeB và NodeC:

```text
NodeA vote commit
NodeB ghi READY, gửi VOTE_COMMIT rồi os._exit(2)
NodeC vote abort
Coordinator ghi GLOBAL_ABORT
NodeB không nhận được global decision
```

Checkpoint tại transaction 1001:

```text
NodeA = 1001
NodeB = 1000, in_doubt = TX001001
NodeC = 1001
global_safe_point = 1000
```

Pruning giữ READY log của NodeB. `RecoveryManager` sau đó:

1. đọc durable log;
2. tìm transaction `READY`;
3. đọc `data/global_tx_table.json`;
4. ghi `COMMIT` hoặc `ABORT` theo global decision.

Recovery được gọi tự động trong failure demo, không cần thao tác thủ công.

## 8. Artifact

```text
logs/*.log                         Durable logs
snapshots/*_checkpoint_*.json     Local/global checkpoints
metrics/checkpoint_metrics.csv    Disk-space metric
metrics/multiprocessing_failure_demo_summary.json
```

Thiết kế ưu tiên tính đúng và khả năng quan sát thay vì triển khai một DBMS
hoàn chỉnh.
