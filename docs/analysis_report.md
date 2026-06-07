# Báo Cáo Phân Tích

## 1. Mục tiêu

Đề tài xác định safe point để xóa transaction log trên các site phân tán mà
không làm mất khả năng recovery. Phân tích dựa trên các khái niệm reliability,
2PC, site failure và recovery protocol trong *Principles of Distributed
Database Systems, 4th Edition* của M. Tamer Özsu và Patrick Valduriez.

## 2. Atomic Commitment Và 2PC

Một distributed transaction phải có kết quả all-or-nothing trên mọi site.
Project dùng Two-Phase Commit:

```text
Phase 1: PREPARE -> VOTE_COMMIT / VOTE_ABORT
Phase 2: GLOBAL_COMMIT / GLOBAL_ABORT
```

Quy tắc global commit:

- chỉ commit khi tất cả participant vote commit;
- nếu có vote abort hoặc participant lỗi thì global abort.

Quy tắc này được thể hiện trong `Coordinator.execute_transaction()`. Các state
chính là:

```text
Coordinator: WAIT -> COMMIT/ABORT -> END
Participant: READY -> COMMIT/ABORT
```

## 3. READY Và In-Doubt

Theo lý thuyết 2PC, participant ở `READY` đã ghi durable log và vote commit,
nhưng chưa biết global decision. Nó không được tự commit vì site khác có thể
vote abort, cũng không được tự abort vì đã cam kết khả năng commit.

Do đó:

- latest state `READY` được xem là in-doubt;
- transaction này được đưa vào `protected_tx_ids`;
- log của nó không được prune;
- sau restart, participant phải đọc quyết định của Coordinator.

Đây là lý do pruning không thể chỉ kiểm tra `gseq <= global_safe_point`.

## 4. Global Checkpoint Và Safe Point

Mỗi site tính contiguous final prefix:

```text
last_checkpointed_gseq =
    gseq lớn nhất mà mọi transaction trước đó đều final
```

Global safe point:

```text
global_safe_point = min(
    NodeA.last_checkpointed_gseq,
    NodeB.last_checkpointed_gseq,
    NodeC.last_checkpointed_gseq
)
```

Phép minimum là lựa chọn bảo thủ: hệ thống chỉ xóa log đến vị trí mà tất cả
site đều đã có trạng thái phục hồi an toàn.

Workload bình thường 1.000 transaction cho kết quả:

```text
NodeA = 1000
NodeB = 1000
NodeC = 1000
global_safe_point = 1000
```

Trong failure demo:

```text
NodeA = 1001
NodeB = 1000
NodeC = 1001
global_safe_point = 1000
```

NodeB đã quan sát transaction 1001 nhưng chỉ có safe prefix đến 1000 vì
`TX001001` còn `READY`.

## 5. Safe Log Pruning

Project prune record khi đồng thời thỏa:

```text
gseq <= global_safe_point
transaction đã COMMIT, ABORT hoặc END
transaction không active
transaction không READY/in-doubt
transaction không thuộc protected_tx_ids
```

Các điều kiện này bảo đảm log cần cho recovery vẫn được giữ lại.

Metric theo yêu cầu đề tài:

```text
saved_bytes = before_bytes - after_bytes
saved_percent = saved_bytes / before_bytes * 100
```

Với workload 1.000 transaction, cycle kiểm thử ghi nhận:

```text
total_before_bytes = 4.336.102
total_after_bytes = 1.416
total_saved_bytes = 4.334.686
total_saved_percent = 99,97%
```

## 6. Process Crash Và Recovery

Failure demo dùng `multiprocessing` để tạo process riêng. NodeB:

```text
ghi READY
gửi VOTE_COMMIT
process chết bằng os._exit(2)
```

Bằng chứng:

```text
process_exitcodes.NodeB = 2
nodeb_alive_after_crash = false
```

Khi process đã chết, NodeB không thể nhận transaction hoặc global decision
mới. Coordinator vẫn ghi durable global decision. Checkpoint bảo vệ
`TX001001`, và pruning xác nhận READY log không bị xóa.

`RecoveryManager` đọc log của NodeB và global transaction table, sau đó ghi
`ABORT` theo quyết định toàn cục. Kết quả:

```text
nodeb_ready_log_preserved_after_pruning = true
remaining_in_doubt_tx_ids = []
```

Điều này đáp ứng tiêu chí failure handling: recovery diễn ra tự động và dữ
liệu không bị hỏng sau crash.

## 7. 2PC Và 3PC

Özsu và Valduriez chỉ ra 2PC có thể blocking khi participant ở `READY` nhưng
không lấy được quyết định từ Coordinator. Project không loại bỏ blocking; nó
bảo vệ log cần thiết và hoàn tất transaction khi Coordinator decision có thể
đọc lại.

3PC thêm pha `PRECOMMIT` để giảm blocking trong một số giả định, nhưng làm
tăng message và forced log write. Đề tài tập trung vào checkpoint, pruning và
recovery nên 2PC phù hợp hơn.

## 8. Đánh Giá Theo Rubric

- **State Accuracy:** có `READY`, `COMMIT`, `ABORT`, `END` và transition 2PC.
- **Failure Handling:** NodeB process crash thật và tự recovery.
- **Log Management:** durable JSONL log có đủ LSN, GSEQ, state và event.
- **Textbook Alignment:** áp dụng global commit rule, in-doubt state, recovery
  từ durable decision theo lý thuyết Özsu và Valduriez.

Project đáp ứng đúng trọng tâm đề tài: global checkpoint, safe point, log
pruning, disk-space metric và recovery correctness.
