# Đề Xuất Đề Tài

## 1. Tên Đề Tài

**Log Pruning and Global Checkpointing for High-Frequency Trading Transactions**

**Cắt Tỉa Log Và Global Checkpointing Cho Giao Dịch Chứng Khoán Tần Suất Cao**

---

## 2. Môn Học

Cơ sở dữ liệu phân tán.

---

## 3. Bài Toán

Các hệ thống giao dịch chứng khoán tần suất cao tạo ra số lượng log rất lớn trong thời gian ngắn. Trong môi trường cơ sở dữ liệu phân tán, một transaction có thể liên quan đến nhiều site khác nhau. Vì vậy, việc xóa log cũ mà không kiểm tra cẩn thận có thể gây lỗi recovery, đặc biệt khi một transaction chưa đạt quyết định cuối cùng trên tất cả site.

Đề tài tập trung vào việc cài đặt cơ chế **global checkpointing** để xác định một **safe point**. Đây là mốc mà tại đó các log cũ có thể được xóa trên các site phân tán mà không làm mất khả năng phục hồi sau crash.

Hệ thống mô phỏng một môi trường phân tán gồm một Coordinator và ba participant site: NodeA, NodeB, NodeC. Mỗi site có durable log riêng. Coordinator quản lý distributed transaction bằng Two-Phase Commit, tạo global checkpoint, tính safe point và yêu cầu prune các log đã an toàn.

---

## 4. Động Lực Thực Hiện

Trong hệ cơ sở dữ liệu phân tán, độ tin cậy là yêu cầu quan trọng. Một transaction không được commit ở một site nhưng abort ở site khác. Điều này đặc biệt quan trọng trong bối cảnh giao dịch chứng khoán, nơi một transaction có thể ảnh hưởng đến nhiều thành phần như order, account balance và trading ledger.

Log là dữ liệu cần thiết để phục hồi sau lỗi, nhưng giữ toàn bộ log mãi mãi sẽ tốn dung lượng đĩa. Vì vậy, checkpointing và log pruning giúp giảm dung lượng lưu trữ trong khi vẫn bảo toàn khả năng recovery.

---

## 5. Mục Tiêu

Các mục tiêu chính:

1. Mô phỏng một hệ cơ sở dữ liệu phân tán gồm nhiều site trên một laptop.
2. Mô phỏng Coordinator, NodeA, NodeB và NodeC.
3. Cài đặt các trạng thái cơ bản của Two-Phase Commit: `READY`, `COMMIT`, `ABORT`, `END`.
4. Sinh dataset gồm 100,000 giao dịch chứng khoán tần suất cao.
5. Ghi durable transaction log ở định dạng JSONL.
6. Cài đặt local checkpoint tại từng participant.
7. Cài đặt global checkpoint và tính `global_safe_point`.
8. Xóa log an toàn dựa trên safe point và protected transaction.
9. Đo dung lượng đĩa tiết kiệm sau mỗi checkpointing cycle.
10. Mô phỏng lỗi node crash sau trạng thái `READY`.
11. Phục hồi node bị crash từ durable log và quyết định cuối cùng của Coordinator.

---

## 6. Dataset

Project dùng dataset được sinh tự động gồm 100,000 giao dịch chứng khoán tần suất cao.

Mỗi transaction gồm:

- transaction id;
- account id;
- stock symbol;
- side: `BUY` hoặc `SELL`;
- quantity;
- price;
- timestamp.

Ví dụ:

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

Dataset được lưu tại:

```text
data/transactions_100k.jsonl
data/dataset_summary.json
```

---

## 7. Kiến Trúc Hệ Thống

Hệ thống gồm bốn thành phần chính:

- Coordinator;
- NodeA;
- NodeB;
- NodeC.

Coordinator quản lý quyết định giao dịch toàn cục và tạo global checkpoint. Ba node còn lại mô phỏng các participant site trong hệ cơ sở dữ liệu phân tán.

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

Mỗi node có:

- trạng thái transaction cục bộ;
- durable log cục bộ;
- local checkpoint snapshot;
- logic recovery.

---

## 8. Công Nghệ Sử Dụng

Project sử dụng:

- Python 3;
- Python standard library;
- JSONL cho transaction logs;
- JSON cho checkpoint snapshots;
- CSV cho checkpoint metrics.

Các script mô phỏng nhiều site trên cùng một laptop. Độ trễ truyền thông được mô phỏng bằng `time.sleep()`. Demo lỗi mô phỏng crash bằng cách cho NodeB dừng sau khi đã ghi durable `READY` log.

---

## 9. Mô Hình Two-Phase Commit

Hệ thống dùng mô hình Two-Phase Commit đơn giản.

Participant states:

```text
INIT -> READY -> COMMIT
INIT -> READY -> ABORT
INIT -> ABORT
```

Coordinator states:

```text
INIT -> WAIT -> COMMIT -> END
INIT -> WAIT -> ABORT  -> END
```

Ý nghĩa các trạng thái:

- `READY`: participant đã đồng ý có thể commit và đang chờ quyết định toàn cục.
- `COMMIT`: transaction đã commit.
- `ABORT`: transaction đã bị hủy.
- `END`: Coordinator đã hoàn tất transaction.

`READY` là trạng thái quan trọng nhất vì participant đã vote commit nhưng chưa biết quyết định cuối cùng. Nếu node crash ở trạng thái này, nó phải đọc log khi restart và hỏi Coordinator.

---

## 10. Định Dạng Log

Mỗi site ghi durable log riêng.

Ví dụ log record:

```json
{
  "lsn": 12,
  "gseq": 45001,
  "tx_id": "TX045001",
  "site": "NodeB",
  "role": "PARTICIPANT",
  "state": "READY",
  "event": "READY",
  "timestamp": "2026-05-16T10:00:00.001000+00:00",
  "details": {}
}
```

Ý nghĩa các trường:

- `lsn`: local log sequence number.
- `gseq`: global sequence number do Coordinator cấp.
- `tx_id`: mã transaction.
- `site`: site ghi log.
- `role`: vai trò của site.
- `state`: trạng thái transaction.
- `event`: sự kiện tạo log.
- `timestamp`: thời điểm ghi log.
- `details`: dữ liệu bổ sung.

---

## 11. Thuật Toán Global Checkpointing

Quy trình global checkpointing:

1. Tạo local checkpoint tại NodeA, NodeB và NodeC.
2. Mỗi site đọc durable log để dựng trạng thái mới nhất.
3. Mỗi site trả về checkpoint metadata gồm `last_checkpointed_gseq`, `active_tx_ids`, `in_doubt_tx_ids`.
4. Global checkpoint manager gom metadata từ tất cả site.
5. Hệ thống tính `global_safe_point`.
6. Hệ thống xác định `protected_tx_ids`.
7. Log pruning chỉ xóa các log không còn cần cho recovery.

---

## 12. Định Nghĩa Safe Point

Safe point toàn cục là global sequence number nhỏ nhất đã được checkpoint bởi tất cả site:

```text
global_safe_point = min(
    NodeA.last_checkpointed_gseq,
    NodeB.last_checkpointed_gseq,
    NodeC.last_checkpointed_gseq
)
```

Một log record chỉ được prune nếu:

- `gseq <= global_safe_point`;
- transaction đã đạt trạng thái cuối `COMMIT`, `ABORT`, hoặc `END`;
- transaction không còn active;
- transaction không ở `READY` / in-doubt;
- transaction không nằm trong `protected_tx_ids`.

Quy tắc này ngăn hệ thống xóa log vẫn còn cần cho crash recovery.

---

## 13. Kịch Bản Failure

Kịch bản lỗi chính:

```text
NodeB crash sau khi ghi READY nhưng trước khi nhận GLOBAL_COMMIT hoặc GLOBAL_ABORT.
```

Hành vi mong đợi:

1. NodeB ghi `READY` vào durable log.
2. NodeB crash.
3. Coordinator quyết định `GLOBAL_ABORT` vì NodeB không còn phản hồi.
4. Global checkpoint đánh dấu transaction đó là protected.
5. Log pruning không xóa READY log của NodeB.
6. NodeB restart.
7. Recovery Manager đọc log và phát hiện transaction ở `READY`.
8. Recovery Manager hỏi quyết định cuối cùng từ `global_tx_table.json`.
9. NodeB ghi `COMMIT` hoặc `ABORT` theo quyết định của Coordinator.
10. Recovery hoàn tất mà không làm hỏng dữ liệu.

---

## 14. Metric Đánh Giá

Metric chính là dung lượng đĩa tiết kiệm sau mỗi checkpointing cycle.

```text
saved_bytes = log_size_before - log_size_after
saved_percent = saved_bytes / log_size_before * 100
```

Kết quả được lưu tại:

```text
metrics/checkpoint_metrics.csv
metrics/prune_checkpoint_<id>_summary.json
```

Ví dụ:

```csv
checkpoint_id,site,before_bytes,after_bytes,saved_bytes,saved_percent,global_safe_point,protected_tx_count
1,NodeA,12400000,7100000,5300000,42.7,45000,3
1,NodeB,11800000,8900000,2900000,24.5,45000,3
1,NodeC,12100000,7000000,5100000,42.1,45000,3
```

---

## 15. Kết Quả Mong Đợi

Project cần chứng minh được:

- sinh được dataset 100,000 giao dịch;
- thực thi distributed transaction bằng 2PC;
- ghi durable logs cho Coordinator và các participant;
- tạo local checkpoint và global checkpoint;
- tính được safe point;
- prune log an toàn;
- báo cáo dung lượng đĩa tiết kiệm;
- recovery đúng sau demo lỗi.

---

## 16. Deliverables

Các tài liệu và sản phẩm nộp:

- Đề xuất đề tài.
- Tài liệu thiết kế.
- Repository GitHub/GitLab.
- Báo cáo phân tích liên hệ lý thuyết Özsu và Valduriez.
- Checkpoint metrics.
- Video demo 3-5 phút cho crash và recovery.
- README hướng dẫn chạy project.

Repository GitHub:

```text
https://github.com/masterdykun-code/distributed-log-checkpointing
```

---

## 17. Phạm Vi Project

Đây là project mô phỏng, không phải hệ thống giao dịch chứng khoán thật.

Project tập trung vào các khái niệm reliability trong cơ sở dữ liệu phân tán:

- distributed transaction;
- Two-Phase Commit;
- durable logging;
- checkpointing;
- safe log pruning;
- crash recovery.

Mục tiêu chính là chứng minh tính đúng đắn của log pruning và recovery, không phải tối ưu hiệu năng giao dịch thực tế.

---

## 18. Liên Hệ Lý Thuyết

Project dựa trên các khái niệm từ sách **Principles of Distributed Database Systems** của M. Tamer Özsu và Patrick Valduriez.

Các khái niệm được áp dụng:

- Distributed transaction processing.
- Two-Phase Commit.
- Global commit rule.
- READY / in-doubt transaction.
- Durable logging.
- Recovery after site failure.
- Checkpointing.
- Safe log pruning after global checkpointing.

Project chứng minh rằng hệ thống vẫn giữ đúng atomicity khi một participant crash trong quá trình commit protocol, vì READY log được bảo vệ và recovery dựa trên quyết định cuối cùng của Coordinator.
