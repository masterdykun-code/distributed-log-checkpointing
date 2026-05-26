# Tài Liệu Thiết Kế

## 1. Tổng Quan Hệ Thống

Project mô phỏng cơ chế đảm bảo độ tin cậy trong hệ cơ sở dữ liệu phân tán cho dữ liệu giao dịch chứng khoán tần suất cao.

Hệ thống gồm:

- Một Coordinator.
- Ba participant site:
  - NodeA
  - NodeB
  - NodeC

Các site được mô phỏng trên cùng một máy tính. Hệ thống tập trung vào logic giao dịch phân tán, durable logging, checkpointing, log pruning và recovery sau lỗi.

---

## 2. Giao Thức Giao Dịch

Hệ thống dùng phiên bản đơn giản của giao thức **Two-Phase Commit (2PC)**.

Coordinator chịu trách nhiệm đưa ra quyết định toàn cục cho mỗi transaction.

Participant chỉ thực hiện thao tác cục bộ, ghi log, gửi vote và làm theo quyết định cuối cùng từ Coordinator.

---

## 3. State Machine Của Coordinator

Nhánh commit:

```text
INIT
  |
  | gửi PREPARE
  v
WAIT
  |
  | tất cả participant vote COMMIT
  v
COMMIT
  |
  | nhận đủ ACK
  v
END
```

Nhánh abort:

```text
INIT
  |
  | gửi PREPARE
  v
WAIT
  |
  | có participant vote ABORT hoặc bị lỗi
  v
ABORT
  |
  | nhận ACK từ các participant còn hoạt động
  v
END
```

---

## 4. State Machine Của Participant

Nhánh commit:

```text
INIT
  |
  | nhận PREPARE và có thể commit
  v
READY
  |
  | nhận GLOBAL_COMMIT
  v
COMMIT
```

Nhánh abort trước READY:

```text
INIT
  |
  | nhận PREPARE nhưng không thể commit
  v
ABORT
```

Nhánh abort sau READY:

```text
INIT
  |
  | nhận PREPARE và có thể commit
  v
READY
  |
  | nhận GLOBAL_ABORT
  v
ABORT
```

---

## 5. Ý Nghĩa Các Trạng Thái

`INIT`: transaction vừa bắt đầu.

`WAIT`: Coordinator đã gửi `PREPARE` và đang chờ vote từ participant.

`READY`: participant đã vote commit và đang chờ quyết định toàn cục. Đây là trạng thái **in-doubt**. Nếu participant crash ở trạng thái này, nó phải đọc durable log khi restart và hỏi Coordinator về quyết định cuối cùng.

`COMMIT`: transaction đã commit thành công.

`ABORT`: transaction đã bị hủy.

`END`: Coordinator đã hoàn tất transaction sau khi ghi quyết định cuối cùng và xử lý ACK.

---

## 6. Các Transition Hợp Lệ Của Participant

```text
INIT  -> READY
INIT  -> ABORT
READY -> COMMIT
READY -> ABORT
```

Các transition không hợp lệ:

```text
COMMIT -> READY
ABORT  -> COMMIT
READY  -> INIT
```

---

## 7. Các Transition Hợp Lệ Của Coordinator

```text
INIT   -> WAIT
WAIT   -> COMMIT
WAIT   -> ABORT
COMMIT -> END
ABORT  -> END
```

---

## 8. Vì Sao READY Phải Được Bảo Vệ

`READY` là trạng thái quan trọng nhất đối với recovery.

Khi participant ở `READY`, nó đã ghi durable log và đã vote commit, nhưng chưa biết quyết định toàn cục là commit hay abort. Participant không được tự ý đổi quyết định.

Vì vậy, log liên quan đến transaction ở `READY` không được prune cho đến khi transaction đó đạt trạng thái cuối cùng `COMMIT` hoặc `ABORT`.

---

## 9. Trách Nhiệm Của Participant Node

Mỗi participant node chịu trách nhiệm:

- nhận `PREPARE`;
- ghi log `READY` nếu có thể commit;
- trả về `VOTE_COMMIT` hoặc `VOTE_ABORT`;
- nhận `GLOBAL_COMMIT` hoặc `GLOBAL_ABORT`;
- ghi log `COMMIT` hoặc `ABORT`;
- tạo local checkpoint;
- phục hồi trạng thái transaction từ durable log.

Participant không tự quyết định commit toàn cục. Nó chỉ làm theo quyết định do Coordinator gửi.

---

## 10. Quy Tắc Recovery Của Participant

Khi participant restart sau crash, nó đọc durable log để dựng lại trạng thái mới nhất của từng transaction.

Nếu tìm thấy transaction ở trạng thái `READY`, transaction đó được xem là **in-doubt transaction**.

In-doubt transaction không được prune cho đến khi participant biết quyết định cuối cùng từ Coordinator.

---

## 11. Trách Nhiệm Của Coordinator

Coordinator chịu trách nhiệm đưa ra quyết định toàn cục cho transaction.

Với mỗi transaction, Coordinator:

1. Cấp global sequence number (`gseq`).
2. Ghi `BEGIN_COMMIT` vào coordinator log.
3. Gửi `PREPARE` đến tất cả participant.
4. Thu thập `VOTE_COMMIT` hoặc `VOTE_ABORT`.
5. Quyết định `GLOBAL_COMMIT` nếu tất cả participant vote commit.
6. Quyết định `GLOBAL_ABORT` nếu có participant vote abort hoặc bị lỗi.
7. Gửi quyết định cuối cùng đến các participant còn hoạt động.
8. Lưu quyết định cuối cùng vào global transaction table.

---

## 12. Global Transaction Table

Coordinator lưu quyết định toàn cục của mỗi transaction tại:

```text
data/global_tx_table.json
```

Bảng này được dùng trong quá trình recovery. Nếu participant crash ở trạng thái `READY`, sau khi restart nó có thể tra quyết định cuối cùng từ bảng này.

---

## 13. Recovery Manager

`RecoveryManager` chịu trách nhiệm phục hồi trạng thái của participant sau crash.

Các bước recovery:

1. Đọc durable JSONL log của participant.
2. Dựng lại trạng thái mới nhất của từng transaction.
3. Phát hiện các transaction vẫn ở trạng thái `READY`.
4. Xem các transaction `READY` là in-doubt transaction.
5. Đọc global transaction table của Coordinator.
6. Nếu quyết định cuối cùng là `COMMIT`, ghi log `COMMIT`.
7. Nếu quyết định cuối cùng là `ABORT`, ghi log `ABORT`.
8. Nếu chưa có quyết định cuối cùng, giữ transaction ở `READY`.

`RecoveryManager` không xóa log `READY`. Các transaction `READY` vẫn được bảo vệ cho đến khi participant biết quyết định toàn cục cuối cùng.

---

## 14. Demo Crash Bằng Multiprocessing

Ngoài demo recovery theo logic object, project có thêm script mô phỏng crash bằng `multiprocessing`.

Trong demo này:

- NodeA, NodeB và NodeC chạy trong các process riêng.
- Coordinator gửi `PREPARE` qua queue.
- NodeB ghi durable log `READY`.
- NodeB process thoát ngay sau khi ghi `READY`.
- Coordinator xem NodeB là participant không phản hồi và quyết định `GLOBAL_ABORT`.
- Checkpointing đánh dấu transaction của NodeB là protected.
- Log pruning không xóa READY log.
- `RecoveryManager` phục hồi NodeB dựa trên log và global transaction table.

Script demo:

```bash
python scripts/run_multiprocessing_failure_demo.py --checkpoint-id 100
```

---

## 15. Local Checkpoint

Mỗi participant tạo local checkpoint từ durable log của chính nó.

Local checkpoint chứa:

- `checkpoint_id`;
- `site`;
- `last_checkpointed_gseq`;
- `active_tx_ids`;
- `in_doubt_tx_ids`;
- số lượng transaction state;
- thời điểm tạo checkpoint.

Local checkpoint chỉ phản ánh trạng thái của một site. Một local checkpoint riêng lẻ chưa đủ để xóa log trên toàn hệ thống.

---

## 16. Global Checkpoint Và Safe Point

Global checkpoint được tạo bằng cách gom metadata từ local checkpoint của NodeA, NodeB và NodeC.

Safe point toàn cục được tính như sau:

```text
global_safe_point = min(
    NodeA.last_checkpointed_gseq,
    NodeB.last_checkpointed_gseq,
    NodeC.last_checkpointed_gseq
)
```

Hệ thống chọn giá trị nhỏ nhất để đảm bảo không site nào bị prune vượt quá mốc mà nó đã checkpoint an toàn.

---

## 17. Quy Tắc Prune Log

Một log record chỉ được prune nếu thỏa tất cả điều kiện:

- `gseq <= global_safe_point`;
- transaction đã ở trạng thái cuối: `COMMIT`, `ABORT`, hoặc `END`;
- transaction không còn active;
- transaction không ở trạng thái `READY` / in-doubt;
- transaction không nằm trong `protected_tx_ids`.

Quy tắc này đảm bảo hệ thống tiết kiệm dung lượng log nhưng vẫn giữ lại log cần thiết cho crash recovery.
