# Báo Cáo Phân Tích Lý Thuyết

## 1. Mục Tiêu Phân Tích

Đề tài **Log Pruning and Global Checkpointing: High-Frequency Trading** mô phỏng một hệ cơ sở dữ liệu phân tán có một Coordinator và ba participant site: NodeA, NodeB, NodeC. Mục tiêu chính là xác định **safe point** để xóa log cũ sau checkpoint mà vẫn bảo toàn khả năng phục hồi khi có lỗi.

Báo cáo này liên hệ thiết kế của project với các khái niệm reliability trong sách **Principles of Distributed Database Systems, 4th Edition** của M. Tamer Özsu và Patrick Valduriez, đặc biệt ở các phần:

- Mục 5.4: Distributed DBMS Reliability.
- Mục 5.4.1: Two-Phase Commit Protocol.
- Mục 5.4.3: Dealing with Site Failures.
- Mục 5.4.3.1: Termination and Recovery Protocols for 2PC.
- Mục 5.4.3.2: Three-Phase Commit Protocol.
- Mục 5.4.6: Architectural Considerations.

## 2. Distributed Reliability Và Atomic Commitment

Theo Özsu và Valduriez, reliability trong hệ cơ sở dữ liệu phân tán tập trung vào ba nhóm protocol: commit protocol, termination protocol và recovery protocol. Vấn đề cốt lõi là đảm bảo **atomic commitment**: một distributed transaction phải có hiệu ứng all-or-nothing trên tất cả site.

Trong project này:

- `Coordinator` đóng vai trò điều phối quyết định toàn cục.
- `NodeA`, `NodeB`, `NodeC` đóng vai trò participant.
- Mỗi site ghi durable log riêng trong `logs/*.log`.
- `global_tx_table.json` lưu quyết định toàn cục để participant có thể hỏi lại khi recovery.

Thiết kế này bám theo mô hình coordinator/participant trong phần 5.4 của sách. Điểm quan trọng là participant không tự quyết định kết quả cuối cùng của distributed transaction; quyết định cuối cùng thuộc về Coordinator.

## 3. Two-Phase Commit Trong Project

Mục 5.4.1 mô tả Two-Phase Commit (2PC) với hai pha:

1. **Prepare / Voting phase**: Coordinator gửi `PREPARE`, participant trả lời `VOTE_COMMIT` hoặc `VOTE_ABORT`.
2. **Decision phase**: Coordinator gửi `GLOBAL_COMMIT` hoặc `GLOBAL_ABORT` dựa trên kết quả vote.

Project cài đặt đúng mô hình này:

- Coordinator ghi `BEGIN_COMMIT`, gửi prepare và chuyển sang trạng thái `WAIT`.
- Participant nếu có thể commit thì ghi log `READY` và vote commit.
- Participant nếu không thể commit thì ghi abort và vote abort.
- Coordinator chỉ commit khi tất cả participant vote commit.
- Nếu có ít nhất một participant vote abort hoặc lỗi, Coordinator quyết định global abort.

Các state chính trong code:

```text
Coordinator:
INIT -> WAIT -> COMMIT -> END
INIT -> WAIT -> ABORT  -> END

Participant:
INIT -> READY -> COMMIT
INIT -> READY -> ABORT
INIT -> ABORT
```

Các state này được định nghĩa trong `src/models.py`, còn logic execute transaction nằm trong `src/coordinator.py` và `src/node.py`.

## 4. Global Commit Rule

Một quy tắc quan trọng trong 2PC là **global-commit rule**:

- Nếu có ít nhất một participant vote abort, transaction phải global abort.
- Chỉ khi tất cả participant vote commit, transaction mới được global commit.

Project áp dụng trực tiếp quy tắc này trong `Coordinator.execute_transaction()`. Biến `all_vote_commit` chỉ đúng khi toàn bộ participant trả về `VOTE_COMMIT` và không có prepare error. Nếu điều kiện này sai, Coordinator chọn `ABORT`.

Điều này giúp hệ thống tránh trường hợp một site commit trong khi site khác abort, tức là tránh vi phạm atomicity của distributed transaction.

## 5. Ý Nghĩa Của READY Và In-Doubt Transaction

Trong 2PC, `READY` là trạng thái nguy hiểm nhất của participant. Khi participant đã ghi `READY`, nó đã vote commit và không được đổi ý tùy tiện. Tuy nhiên, participant vẫn chưa biết quyết định cuối cùng của Coordinator.

Theo mục 5.4.3.1, nếu participant timeout hoặc crash khi đang ở `READY`, nó không thể tự commit vì có thể participant khác đã vote abort. Nó cũng không thể tự abort vì nó đã vote commit. Vì vậy transaction ở `READY` được xem là **in-doubt** và participant phải hỏi Coordinator hoặc các site khác để biết kết quả cuối cùng.

Project thể hiện điều này bằng các cơ chế:

- `LogManager.get_in_doubt_tx_ids()` tìm các transaction đang ở `READY`.
- Global checkpoint đưa các transaction `READY` vào `protected_tx_ids`.
- `LogManager.prune_logs()` không xóa log của transaction protected.
- Demo lỗi tạo tình huống NodeB crash sau khi ghi `READY`.
- Sau restart, NodeB đọc log, phát hiện transaction đang in-doubt, tra durable
  decision trong `data/global_tx_table.json` và ghi quyết định cuối cùng.

Đây là phần quan trọng nhất để chứng minh log pruning an toàn: nếu READY log bị xóa, NodeB có thể mất bằng chứng rằng nó từng vote commit, làm recovery sai.

## 6. Site Failure Và Recovery Trong 2PC

Mục 5.4.3.1 phân tích các tình huống site failure trong 2PC. Với participant failure, có ba trường hợp chính:

- Fail ở `INITIAL`: khi recovery có thể abort vì chưa vote commit.
- Fail ở `READY`: phải xử lý như timeout trong READY và cần biết quyết định cuối cùng.
- Fail ở `COMMIT` hoặc `ABORT`: đây là trạng thái kết thúc nên không cần quyết định lại.

Demo lỗi của project tập trung vào trường hợp quan trọng nhất:

```text
NodeB writes READY
NodeB crashes before final decision
Coordinator decides GLOBAL_ABORT
Global checkpoint protects TX001001
Pruning does not delete NodeB READY log
NodeB restarts and recovers TX001001 as in-doubt
NodeB reads the durable global decision
NodeB writes ABORT
```

Demo hiện tại dùng transaction thật từ dataset, mặc định là `TX001001` sau
workload 1.000 transaction. Kết quả được lưu trong
`metrics/multiprocessing_failure_demo_summary.json`. Trường:

```text
nodeb_ready_log_preserved_after_pruning = true
```

chứng minh rằng pruning không phá hỏng khả năng recovery.

## 7. Vì Sao 2PC Có Thể Blocking

Özsu và Valduriez chỉ ra rằng 2PC là một blocking protocol. Khi participant ở `READY` và Coordinator bị lỗi, participant không đủ thông tin để tự quyết định commit hay abort. Nó phải chờ quyết định cuối cùng hoặc recovery của Coordinator.

Project không cố loại bỏ hoàn toàn blocking. Thay vào đó, project tập trung vào yêu cầu của đề tài: không xóa log còn cần cho recovery. Vì vậy thiết kế chọn hướng bảo vệ các transaction in-doubt thay vì prune tất cả log trước safe point.

Điểm này giải thích vì sao pruning rule không chỉ dựa vào `gseq <= global_safe_point`, mà còn cần kiểm tra:

```text
transaction is not active
transaction is not READY / in-doubt
transaction is not protected
```

## 8. Vì Sao Không Dùng Three-Phase Commit

Mục 5.4.3.2 trình bày Three-Phase Commit (3PC) như một hướng giảm blocking bằng cách thêm trạng thái `PRECOMMIT`. Tuy nhiên, 3PC cần thêm vòng trao đổi message và forced log writes, làm tăng latency và chi phí giao tiếp.

Đề tài này tập trung vào:

- global checkpointing,
- safe point calculation,
- log pruning,
- disk space saved,
- recovery demo sau crash.

Vì vậy 2PC là lựa chọn hợp lý hơn cho project cuối kỳ: đủ để mô phỏng atomic commitment, có trạng thái `READY` rõ ràng để phân tích failure, và phù hợp với rubric yêu cầu READY/COMMIT/ABORT.

## 9. Log Management Và Recovery Information

Mục 5.4.6 nhấn mạnh rằng recovery trong distributed DBMS cần đọc log để biết trạng thái cuối cùng của transaction. Sách cũng thảo luận việc commit protocol record có thể được lưu trong database log hoặc distributed transaction log.

Project chọn cách đơn giản và dễ quan sát:

- Mỗi site có một durable JSONL log riêng.
- Mỗi log record có `lsn`, `gseq`, `tx_id`, `site`, `role`, `state`, `event`, `timestamp`, `details`.
- Log được flush và fsync sau khi ghi để mô phỏng durable logging.
- Recovery đọc log để dựng lại latest state theo từng transaction.

Cách này phù hợp với mục tiêu mô phỏng vì giảng viên có thể mở trực tiếp file log để kiểm tra state transition và recovery evidence.

## 10. Global Checkpoint Và Safe Point

Đề tài yêu cầu cài đặt Global Checkpointing algorithm và xác định safe point để xóa log trên nhiều distributed site.

Project định nghĩa safe point như sau:

```text
global_safe_point = min(
    NodeA.last_checkpointed_gseq,
    NodeB.last_checkpointed_gseq,
    NodeC.last_checkpointed_gseq
)
```

Ý nghĩa:

- Mỗi node chỉ biết chắc nó đã checkpoint đến `last_checkpointed_gseq` của riêng nó.
- Hệ thống chỉ an toàn khi chọn mốc nhỏ nhất giữa tất cả node.
- Nếu một node checkpoint chậm hơn, toàn hệ thống phải lấy node đó làm giới hạn.

Đây là cách bảo thủ nhưng an toàn. Nó phù hợp với tinh thần recovery trong hệ phân tán: không được xóa thông tin mà một site vẫn có thể cần để phục hồi.

High-watermark của local checkpoint được lưu ngoài phần transaction log có
thể prune. Vì vậy safe point không bị giảm giả tạo ở checkpoint tiếp theo chỉ
vì các log trước đó đã được xóa.

Project cũng có demo site chậm bằng `multiprocessing`. NodeB được cấu hình
processing delay cao hơn, checkpoint xảy ra khi ba process có tiến độ khác
nhau và safe point được tính từ tiến độ thật:

```text
NodeA = 199
NodeB = 84
NodeC = 198
global_safe_point = min(199, 84, 198) = 84
```

Kịch bản này chứng minh trực tiếp rằng site chậm nhất giới hạn phạm vi log có
thể prune trên toàn hệ thống.

## 11. Safe Log Pruning Rule

Project chỉ prune một log record khi tất cả điều kiện sau đúng:

```text
gseq <= global_safe_point
transaction final state is COMMIT, ABORT, or END
transaction is not active
transaction is not READY / in-doubt
transaction is not in protected_tx_ids
```

Quy tắc này bảo vệ atomicity và recovery correctness:

- `gseq <= global_safe_point` đảm bảo log nằm trước checkpoint toàn cục.
- Trạng thái cuối đảm bảo transaction đã kết thúc.
- Không active và không READY đảm bảo transaction không còn cần quyết định cuối cùng.
- `protected_tx_ids` giữ lại các transaction đặc biệt như NodeB crash sau READY.

## 12. Metric Disk Space Saved

Metric chính của đề tài là dung lượng đĩa tiết kiệm sau mỗi checkpointing cycle:

```text
saved_bytes = before_bytes - after_bytes
saved_percent = saved_bytes / before_bytes * 100
```

Project ghi metric vào:

```text
metrics/checkpoint_metrics.csv
metrics/prune_checkpoint_<id>_summary.json
```

Ví dụ sau checkpoint 1, project ghi nhận dung lượng log trước và sau pruning cho Coordinator, NodeA, NodeB, NodeC. Đây là bằng chứng định lượng cho yêu cầu "Disk space saved after each checkpointing cycle".

## 13. Đánh Giá Thiết Kế

Thiết kế hiện tại đạt các yêu cầu trọng tâm:

- Có dataset 100,000 giao dịch.
- Có 2PC state machine với READY, COMMIT, ABORT.
- Có durable JSONL logs cho từng site.
- Có local checkpoint và global checkpoint.
- Có safe point dựa trên minimum checkpointed gseq.
- Có high-watermark không giảm sau pruning.
- Có demo site chậm với tiến độ đo từ process thật.
- Có pruning metric.
- Có demo lỗi cho NodeB crash sau READY.
- Có recovery dựa trên durable log và quyết định từ Coordinator.

Giới hạn của project:

- Đây là mô phỏng trên một laptop, không phải DBMS thật.
- Workload chính dùng method call; các kịch bản delay và hard crash dùng
  `multiprocessing.Process`.
- Checkpoint chủ yếu lưu recovery metadata thay vì full database page state.

Các giới hạn này chấp nhận được trong phạm vi project cuối kỳ vì mục tiêu chính là chứng minh logic reliability, safe checkpointing và log pruning.

## 14. Kết Luận

Project áp dụng đúng các nguyên lý reliability của Özsu và Valduriez cho distributed transaction processing. Điểm mạnh nhất là xử lý đúng trạng thái `READY`: transaction ở READY được xem là in-doubt, được đưa vào protected set và không bị prune dù nằm trước safe point.

Nhờ đó, hệ thống có thể tiết kiệm dung lượng log sau checkpoint nhưng vẫn giữ lại các log cần thiết cho recovery. Đây chính là yêu cầu cốt lõi của đề tài **Log Pruning and Checkpointing: High-Frequency Trading**.
