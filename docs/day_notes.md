# Ghi Chú Quá Trình Làm Project

## Ngày 1

### Project làm về gì?

Project mô phỏng cơ chế **log pruning** và **global checkpointing** trong hệ cơ sở dữ liệu phân tán.

Hệ thống gồm:

- Coordinator;
- NodeA;
- NodeB;
- NodeC.

Mỗi transaction phải được xử lý nhất quán trên tất cả node.

### Vì sao cần log?

Log cần cho crash recovery. Nếu một node crash, node đó có thể đọc log để biết trạng thái cuối cùng của từng transaction.

### Các trạng thái quan trọng

- `INIT`: transaction bắt đầu.
- `READY`: participant đã sẵn sàng commit và đang chờ quyết định toàn cục.
- `COMMIT`: transaction commit thành công.
- `ABORT`: transaction bị hủy.

### Vì sao READY quan trọng?

`READY` nguy hiểm vì participant đã hứa có thể commit, nhưng chưa biết quyết định cuối cùng từ Coordinator.

Nếu node crash ở trạng thái `READY`, khi restart nó phải đọc durable log và hỏi Coordinator về quyết định cuối cùng.

### Checkpointing là gì?

Checkpointing lưu một snapshot ổn định của trạng thái hệ thống. Sau checkpoint, một số log cũ có thể được xóa nếu đã an toàn.

### Safe point là gì?

Safe point là mốc toàn cục mà các log trước hoặc bằng mốc đó có thể được xóa mà không làm mất khả năng recovery.

Một log chỉ được prune nếu:

- `gseq <= global_safe_point`;
- transaction đã `COMMIT` hoặc `ABORT`;
- transaction không còn active;
- transaction không ở `READY` / in-doubt.

### Ghi chú triển khai

Project mô phỏng nhiều site trên cùng một laptop. Hệ thống cũng mô phỏng:

- độ trễ truyền thông bằng `time.sleep()`;
- lỗi node crash trong demo lỗi;
- recovery bằng cách restart node và đọc durable log.

---

## Ngày 2

### Việc đã làm

Tạo đề xuất đề tài.

Proposal mô tả hệ thống gồm Coordinator, NodeA, NodeB và NodeC.

---

## Ngày 3

### Kiến thức học được

Tìm hiểu state machine của giao thức **Two-Phase Commit**.

### Trạng thái của Coordinator

- `INIT`
- `WAIT`
- `COMMIT`
- `ABORT`
- `END`

Coordinator gửi `PREPARE`, chờ vote, sau đó quyết định `GLOBAL_COMMIT` hoặc `GLOBAL_ABORT`.

### Trạng thái của Participant

- `INIT`
- `READY`
- `COMMIT`
- `ABORT`

Participant vào trạng thái `READY` sau khi vote commit.

### Vì sao READY quan trọng?

`READY` là trạng thái in-doubt. Nếu node crash ở trạng thái này, node phải recovery từ durable log và hỏi Coordinator về quyết định cuối cùng.

### File liên quan

- `src/models.py`
- `docs/design.md`

---

## Ngày 4

### Việc đã làm

Tạo script sinh dataset.

Script sinh dữ liệu giao dịch chứng khoán tần suất cao ở định dạng JSONL.

### File dataset

```text
data/transactions_100k.jsonl
```

---

## Ngày 5

### Việc đã làm

Cài đặt `LogManager`.

`LogManager` ghi durable JSONL log record cho từng site.

### File liên quan

- `src/log_manager.py`
- `README.md`

### Vì sao log quan trọng?

Log cần cho crash recovery. Nếu node crash, node có thể đọc log để dựng lại trạng thái mới nhất của từng transaction.

### Các trường trong log

- `lsn`
- `gseq`
- `tx_id`
- `site`
- `role`
- `state`
- `event`
- `timestamp`
- `details`

### Khái niệm quan trọng

Transaction ở `READY` là in-doubt transaction. Transaction ở `READY` không được prune vì vẫn có thể cần cho recovery.

### Quy tắc pruning ban đầu

Một log record chỉ được prune nếu:

- `gseq <= global_safe_point`;
- `tx_id` không protected;
- transaction đã đạt trạng thái cuối `COMMIT` hoặc `ABORT`.

---

## Ngày 6

### Việc đã làm

Cài đặt class `ParticipantNode`.

### File liên quan

- `src/node.py`
- `docs/design.md`

### Trách nhiệm của ParticipantNode

Participant node có thể:

- xử lý `PREPARE`;
- ghi log `READY`;
- vote `COMMIT` hoặc `ABORT`;
- xử lý `GLOBAL_COMMIT`;
- xử lý `GLOBAL_ABORT`;
- tạo local checkpoint;
- recovery từ durable log.

### Ý tưởng quan trọng

Participant không tự quyết định kết quả toàn cục. Coordinator mới là thành phần quyết định `GLOBAL_COMMIT` hoặc `GLOBAL_ABORT`.

### Recovery test

Kiểm tra một NodeB mới có thể đọc durable log và phát hiện transaction ở trạng thái `READY`.

---

## Ngày 7

### Việc đã làm

Cài đặt `Coordinator` cho giao thức Two-Phase Commit.

### File liên quan

- `src/coordinator.py`
- `README.md`
- `docs/design.md`

### Trách nhiệm của Coordinator

Coordinator:

- cấp global sequence number;
- gửi `PREPARE`;
- thu thập vote;
- quyết định `GLOBAL_COMMIT` hoặc `GLOBAL_ABORT`;
- gửi quyết định cuối cùng;
- ghi coordinator log;
- lưu global transaction table.

### Commit rule

Coordinator chỉ quyết định `GLOBAL_COMMIT` nếu tất cả participant vote commit.

### Abort rule

Coordinator quyết định `GLOBAL_ABORT` nếu có participant vote abort hoặc bị lỗi.

### Ý tưởng quan trọng

`VOTE_COMMIT` không giống `COMMIT`. Participant gửi `VOTE_COMMIT` thì vào `READY` và vẫn phải chờ quyết định cuối cùng từ Coordinator.

---

## Ngày 8

### Việc đã làm

Cài đặt workload runner.

Workload runner đọc transaction từ:

```text
data/transactions_100k.jsonl
```

Sau đó chạy các transaction bằng Coordinator và ba participant node.

### File liên quan

- `src/log_manager.py`
- `src/coordinator.py`
- `scripts/run_workload.py`
- `README.md`

### Vì sao bước này quan trọng?

Bước này tạo distributed logs cho nhiều transaction. Các log này được dùng cho:

- global checkpointing;
- safe point calculation;
- log pruning;
- đo disk space saved.

### Ý tưởng quan trọng

Dataset không phải recovery log. Dataset chỉ là workload input.

Recovery logs được sinh ra trong quá trình chạy và lưu tại:

```text
logs/Coordinator.log
logs/NodeA.log
logs/NodeB.log
logs/NodeC.log
```

---

## Ngày 9

### Việc đã làm

Cài đặt local checkpointing cho từng participant node.

### File liên quan

- `src/log_manager.py`
- `src/node.py`
- `scripts/run_checkpoint_demo.py`
- `README.md`

### Local checkpoint là gì?

Local checkpoint là snapshot ổn định do một node tạo ra.

Mỗi node tạo checkpoint riêng:

- `NodeA_checkpoint_1.json`
- `NodeB_checkpoint_1.json`
- `NodeC_checkpoint_1.json`

### Checkpoint chứa gì?

Mỗi checkpoint chứa:

- `checkpoint_id`
- `site`
- `last_checkpointed_gseq`
- `active_tx_ids`
- `in_doubt_tx_ids`
- `state_by_tx_count`
- `timestamp`

### Vì sao checkpointing quan trọng?

Checkpointing giảm lượng log cần replay khi recovery.

Sau một global checkpoint an toàn, log cũ trước safe point có thể được prune nếu không còn cần cho recovery.

### Ý tưởng quan trọng

Local checkpoint riêng lẻ chưa đủ để xóa log. Log chỉ được prune sau khi Coordinator hoặc GlobalCheckpointManager tính được safe point toàn cục.

---

## Ngày 10

### Việc đã làm

Cài đặt global checkpointing và safe point calculation.

### File liên quan

- `src/checkpoint_manager.py`
- `scripts/run_global_checkpoint.py`
- `README.md`

### Global checkpoint là gì?

Global checkpoint được tạo bằng cách gom local checkpoint metadata từ tất cả participant node.

Hệ thống có ba local checkpoint:

- NodeA checkpoint;
- NodeB checkpoint;
- NodeC checkpoint.

Global checkpoint kết hợp các checkpoint này và tính safe point toàn cục.

### Global safe point

```text
global_safe_point = min(
    NodeA.last_checkpointed_gseq,
    NodeB.last_checkpointed_gseq,
    NodeC.last_checkpointed_gseq
)
```

### Protected transactions

Protected transactions là các transaction không được prune.

Chúng bao gồm:

- active transactions;
- `READY` / in-doubt transactions.

### Kết quả checkpoint 1

```text
NodeA last_checkpointed_gseq = 100000
NodeB last_checkpointed_gseq = 100000
NodeC last_checkpointed_gseq = 100000

global_safe_point = 100000
protected_tx_count = 0
```

### Ý tưởng quan trọng

Local checkpoint không đủ cho log pruning. Log chỉ được prune an toàn sau khi global safe point được tính.

---

## Ngày 11

### Việc đã làm

Cài đặt safe log pruning dựa trên global safe point.

### File liên quan

- `src/log_manager.py`
- `scripts/run_log_pruning.py`
- `README.md`

### Input của pruning

```text
snapshots/global_checkpoint_1.json
```

Các giá trị quan trọng:

```text
global_safe_point = 100000
protected_tx_count = 0
active_tx_count = 0
in_doubt_tx_count = 0
```

### Quy tắc pruning

Một log record chỉ được prune nếu:

- `gseq <= global_safe_point`;
- transaction ở trạng thái cuối `COMMIT`, `ABORT`, hoặc `END`;
- transaction không protected;
- transaction không active;
- transaction không ở `READY` / in-doubt.

### Metric

```text
saved_bytes = before_bytes - after_bytes
saved_percent = saved_bytes / before_bytes * 100
```

### Vì sao an toàn?

Log chỉ được prune sau khi global checkpoint xác định safe point.

Transaction `READY` / in-doubt được bảo vệ khỏi pruning, nên recovery correctness vẫn được giữ.

---

## Ngày 12

### Việc đã làm

Cài đặt failure handling demo.

### Kịch bản

NodeB crash sau khi ghi `READY` nhưng trước khi nhận quyết định toàn cục.

### Vì sao quan trọng?

`READY` là trạng thái in-doubt.

Nếu participant crash ở `READY`, khi restart nó phải đọc durable log và hỏi Coordinator về quyết định cuối cùng.

### Luồng demo

1. Coordinator gửi `PREPARE`.
2. NodeA ghi `READY`.
3. NodeB ghi `READY`.
4. NodeB crash sau `READY`.
5. Coordinator quyết định `GLOBAL_ABORT`.
6. Local checkpoint được tạo.
7. Global checkpoint đánh dấu `TX_FAIL_001` là protected.
8. Thử prune log.
9. READY log của NodeB vẫn được giữ lại.
10. NodeB restart và recovery từ log.
11. NodeB hỏi Coordinator về quyết định cuối cùng.
12. NodeB ghi `ABORT`.

### Kết quả quan trọng

Hệ thống không prune transaction `READY` / in-doubt.

Điều này bảo toàn crash recovery correctness.

---

## Ngày 13

### Việc đã làm

Cài đặt `RecoveryManager` thành module recovery riêng.

### File liên quan

- `src/recovery_manager.py`
- `scripts/run_failure_demo.py`
- `docs/design.md`

### RecoveryManager làm gì?

`RecoveryManager`:

- đọc durable log của participant;
- dựng lại trạng thái mới nhất của transaction;
- phát hiện transaction ở `READY`;
- đọc `data/global_tx_table.json`;
- ghi quyết định cuối cùng `COMMIT` hoặc `ABORT`;
- trả summary recovery cho demo lỗi.

### Kết quả kiểm tra

Demo lỗi xác nhận:

```text
NodeB READY log exists after pruning: True
Decision applied by RecoveryManager: ABORT
remaining_in_doubt_tx_ids: []
Demo lỗi hoàn tất thành công.
```

---

## Ngày 14

### Việc đã làm

Thêm demo crash bằng `multiprocessing`.

### File liên quan

- `scripts/run_multiprocessing_failure_demo.py`
- `README.md`

### Ý tưởng

Demo mới tạo process riêng cho:

- NodeA;
- NodeB;
- NodeC.

NodeB ghi durable log `READY`, sau đó process thoát ngay với exit code khác 0. Điều này mô phỏng việc NodeB crash sau `READY` nhưng trước khi nhận quyết định toàn cục.

### Luồng demo

1. Coordinator ghi `BEGIN_COMMIT`.
2. Coordinator gửi `PREPARE` đến các process participant.
3. NodeB ghi `READY` rồi process thoát.
4. Coordinator không nhận được vote từ NodeB và quyết định `GLOBAL_ABORT`.
5. NodeA và NodeC nhận `GLOBAL_ABORT`.
6. Hệ thống tạo local checkpoint và global checkpoint.
7. Transaction của NodeB được đánh dấu protected.
8. Log pruning không xóa READY log của NodeB.
9. `RecoveryManager` phục hồi NodeB và ghi `ABORT`.

### Lệnh chạy

```bash
python scripts/run_multiprocessing_failure_demo.py --checkpoint-id 100
```

### Kết quả cần kiểm tra

```text
process_exitcodes.NodeB = 2
nodeb_ready_log_preserved_after_pruning = true
recovery_result.decisions_applied.TX_MP_FAIL_001 = ABORT
```
