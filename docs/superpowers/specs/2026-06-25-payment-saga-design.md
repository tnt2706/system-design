# Design Spec — Order + Payment qua Saga Orchestration

> **Loại tài liệu:** Design analysis / RFC (chỉ phân tích thiết kế, không code)
> **Ngày:** 2026-06-25
> **Trạng thái:** Draft chờ review
> **Mục đích:** Dự án học để nắm chắc distributed transaction bằng Saga, idempotency, outbox và double-entry ledger.

---

## 1. Context & Goals

Khi một nghiệp vụ trải trên nhiều microservice (mỗi service một DB), không còn một transaction ACID đơn bao trùm tất cả. Dự án này dựng lại bài toán **đặt hàng có thanh toán** xuyên 3 service để tự tay giải quyết tính nhất quán phân tán.

**Mục tiêu học (đo được):**
- Thiết kế & vận hành một **Saga orchestration** đầy đủ: happy path + compensation.
- Áp dụng **idempotency** cho mọi thao tác có side-effect tiền bạc.
- Áp dụng **transactional outbox** để loại bỏ bài toán dual-write.
- Dùng **double-entry ledger** làm nguồn sự thật cho tiền (không update balance trực tiếp).
- Hiểu rõ trade-off **Saga vs 2PC** và **orchestration vs choreography**.

**Không phải mục tiêu:** một hệ thống thanh toán production-grade. Đây là sandbox học tập.

---

## 2. Scope

**Trong phạm vi:**
- Luồng đặt hàng: `Reserve Inventory` → `Process Payment` → `Confirm Order`, có bù trừ khi lỗi.
- 3 service độc lập, mỗi service một Postgres riêng.
- Orchestrator nằm trong Order Service (Order sở hữu vòng đời saga).
- Mock payment gateway (có công tắc ép thành công/thất bại để test compensation).

**Ngoài phạm vi (YAGNI):**
- Payment gateway thật / tiền thật.
- Auth, user management, UI.
- Multi-region (chỉ nêu nguyên tắc ở mục 10, không triển khai).
- Choreography (để dành stretch — mục 12).

---

## 3. Non-Functional Requirements

| NFR | Yêu cầu |
|---|---|
| **Correctness** | Tuyệt đối: không double-charge, không mất/nhân đôi tiền, không trạng thái "đã trừ chưa cộng" treo vĩnh viễn. |
| **Consistency** | Eventual consistency giữa các service (chấp nhận trễ ngắn), nhưng **mỗi ví/ledger trong một service là strong consistency**. |
| **Resilience** | Chịu được: message trùng, message mất, service crash giữa saga, compensation lỗi. |
| **Observability** | Theo dõi được trạng thái từng saga, lý do compensate, message trong DLQ. |
| **Quy mô** | Nhỏ (học) — không cần tối ưu throughput; ưu tiên rõ ràng & đúng đắn. |

---

## 4. Architecture Overview

**Stack:** Go · RabbitMQ (command/reply) · PostgreSQL (một DB/ service) · Docker Compose.

**Thành phần:**
- **Order Service** — nhận `POST /orders`, tạo order `PENDING`, **chứa Saga Orchestrator** (state machine điều phối các bước). Sở hữu DB `orderdb`.
- **Inventory Service** — reserve/commit/release tồn kho. Sở hữu DB `inventorydb`.
- **Payment Service** — charge/refund qua mock gateway, ghi ledger tiền. Sở hữu DB `paymentdb`.
- **RabbitMQ** — broker command/reply giữa orchestrator và các service.

```
Client ── POST /orders ──▶ Order Service (Orchestrator)
                               │  saga_state (orderdb)
            commands/replies   │
        ┌──────────────────────┼──────────────────────┐
        ▼ (RabbitMQ)           ▼                       ▼
  Inventory Service      Payment Service          (Order self-confirm)
   inventorydb            paymentdb + ledger
```

**Quyết định kiến trúc:** Orchestrator đặt **trong** Order Service (không tạo service thứ 4). Lý do: đơn giản hơn, vẫn học đủ; Order là nơi tự nhiên sở hữu vòng đời đơn hàng. (Nếu cần tách để nhiều loại saga dùng chung thì mới nâng thành service riêng — YAGNI lúc này.)

---

## 5. Saga Design

### 5.1 Saga là một state machine có persist
Orchestrator lưu trạng thái saga vào `orderdb.saga_state` (saga_id, order_id, current_step, status, updated_at). Nhờ persist, sau khi crash có thể **resume**, hoặc theo **timeout** thì **compensate**.

### 5.2 Các bước (happy path)
| # | Bước | Command → Service | Reply | Hệ quả |
|---|---|---|---|---|
| 1 | Tạo order | (local) | — | order = `PENDING`, saga = `STARTED` |
| 2 | Reserve Inventory | `ReserveInventory` → Inventory | `OK` / `FAIL` | reservation = `RESERVED` |
| 3 | Process Payment | `ProcessPayment` → Payment | `OK` / `FAIL` | payment = `CHARGED`, ledger entries |
| 4 | Confirm | (local) + `CommitReservation` → Inventory | `OK` | order = `CONFIRMED`, saga = `COMPLETED` |

### 5.3 Compensation (chạy NGƯỢC thứ tự)
| Lỗi xảy ra ở | Hành động bù trừ |
|---|---|
| Bước 2 (Inventory FAIL) | Không có gì để bù → order = `CANCELLED`, saga = `ABORTED` |
| Bước 3 (Payment FAIL, đã reserve) | `ReleaseInventory` → order = `CANCELLED`, saga = `COMPENSATED` |
| Bước 4 fail sau khi đã charge | `RefundPayment` + `ReleaseInventory` → `CANCELLED` |

**Bất biến:** mọi command & compensation đều **idempotent** (mục 7.1) và **retriable**. Compensation thất bại → retry; nếu vẫn fail → đẩy DLQ + alert (không được im lặng bỏ qua tiền).

---

## 6. Data Models (mức field — không code)

**Order Service (`orderdb`):**
- `orders(id, status, total_amount, currency, created_at)` — status ∈ {PENDING, CONFIRMED, CANCELLED}.
- `saga_state(saga_id, order_id, current_step, status, last_error, updated_at)`.
- `outbox(id, aggregate_id, type, payload, created_at, published_at)`.

**Inventory Service (`inventorydb`):**
- `inventory(product_id, available, reserved)`.
- `reservations(reservation_id, order_id, product_id, qty, status, expires_at)` — status ∈ {RESERVED, COMMITTED, RELEASED}.
- `processed_messages(message_id, result)` — idempotency/inbox.
- `outbox(...)`.

**Payment Service (`paymentdb`):**
- `payments(payment_id, order_id, amount, currency, status)` — status ∈ {CHARGED, REFUNDED, FAILED}.
- `ledger_entries(id, account, txn_id, amount, direction, created_at)` — **append-only, double-entry** (mục 7.4).
- `processed_messages(message_id, result)` — idempotency.
- `outbox(...)`.

> Tiền lưu dạng **integer** (đơn vị nhỏ nhất, vd cent) — không dùng float.

---

## 7. Core Patterns (trọng tâm phân tích)

### 7.1 Idempotency
Mỗi command mang `message_id` (hoặc `saga_id` + `step`). Service ghi vào `processed_messages` trong cùng transaction với thay đổi nghiệp vụ. Nhận command trùng → trả `result` đã lưu, **không thực thi lại**. Chống double-charge khi at-least-once delivery của RabbitMQ gây trùng.

### 7.2 Transactional Outbox
Vấn đề dual-write: ghi DB **và** publish message là 2 thao tác, có thể lệch khi lỗi giữa chừng. Giải pháp: service ghi thay đổi nghiệp vụ + một dòng `outbox` trong **cùng một DB transaction**. Một **relay/poller** đọc bảng outbox và publish lên RabbitMQ, đánh dấu `published_at`. → message luôn khớp với thay đổi DB.

### 7.3 Saga State Persistence & Recovery
Orchestrator persist trạng thái sau mỗi bước. Khi khởi động lại: quét các saga đang dở (`status` chưa terminal) → tiếp tục hoặc compensate. Mỗi bước có **timeout**; quá hạn không nhận reply → coi như fail → compensate.

### 7.4 Double-entry Ledger (cho tiền)
Không `UPDATE balance`. Mỗi chuyển động tiền ghi ≥2 `ledger_entries` cân bằng (tổng = 0); số dư = `SUM(entries)`. Append-only → audit + reconciliation. Ví dụ charge 100: `(player, txn, -100)` + `(house, txn, +100)`.

### 7.5 Delivery semantics
RabbitMQ **at-least-once** + **manual ack**. Kết hợp idempotency (7.1) → đạt hiệu ứng **effectively exactly-once** ở tầng nghiệp vụ. Message lỗi nhiều lần → **Dead Letter Queue**.

---

## 8. Failure Modes & Handling

| Sự cố | Cơ chế xử lý |
|---|---|
| Message trùng (retry/redeliver) | `processed_messages` dedupe (7.1) |
| Message mất | Outbox relay + at-least-once + ack thủ công |
| Orchestrator crash giữa saga | `saga_state` persisted → resume khi khởi động |
| Service downstream không reply | Timeout bước → compensate |
| Compensation thất bại | Retry (idempotent) → nếu vẫn fail: DLQ + alert |
| Reservation mồ côi (saga chết im) | `reservations.expires_at` (TTL) → job tự release |
| Race trên cùng inventory/ví | Optimistic locking (version) hoặc atomic trong 1 transaction |
| Dual-write lệch | Transactional outbox (7.2) |

---

## 9. Messaging Design (RabbitMQ)

- **Command queues:** `inventory.commands`, `payment.commands` — orchestrator publish, service consume.
- **Reply:** mỗi command mang `reply_to` + `correlation_id (saga_id)`; orchestrator nghe `orchestrator.replies`.
- **DLX/DLQ:** mỗi queue gắn dead-letter exchange cho message fail quá số lần retry.
- **Manual ack:** consumer chỉ ack sau khi xử lý + ghi DB thành công (nếu crash trước ack → redeliver, idempotency lo phần trùng).

---

## 10. Money Correctness (liên hệ multi-region)

Dù dự án này một-region, nguyên tắc đúng tiền vẫn áp dụng (xem chi tiết trong `system-design-roadmap.html` mục "Tính đúng tiền trong game đa region"):
- Ledger double-entry là nguồn sự thật; số dư = tổng entries.
- Idempotency theo `txn_id`/`round_id` trên mọi thao tác tiền.
- Nếu mở rộng multi-region sau này: **home-region per wallet** (single-writer mỗi ví) để strong consistency không conflict, tránh tuyệt đối multi-master/LWW trên ví.
- Reconciliation định kỳ: `SUM(ledger) == payments` và đối soát với (mock) gateway.

---

## 11. Observability & Cách Verify (dự án học)

**Quan sát:** log có `saga_id`/`correlation_id` xuyên service; endpoint xem `saga_state`; theo dõi độ sâu DLQ.

**Kịch bản test (ép hệ thống gãy — phần học nhiều nhất):**
1. **Happy path:** order → confirmed, ledger cân bằng.
2. **Payment fail:** bật mock fail → kiểm inventory được release, order `CANCELLED`, không có ledger charge treo.
3. **Crash giữa saga:** kill Order Service sau bước Reserve → khởi động lại → saga resume/compensate đúng.
4. **Message trùng:** gửi lại cùng `ProcessPayment` → chỉ charge một lần (kiểm ledger).
5. **Downstream treo:** Payment không reply → timeout → compensate.
6. **Reconciliation:** chạy job → xác nhận không lệch.

---

## 12. Trade-offs & Alternatives Considered

| Quyết định | Đã chọn | Phương án khác & lý do loại |
|---|---|---|
| Quản lý transaction phân tán | **Saga** | 2PC: khoá lâu, coordinator là điểm chết, không hợp microservices |
| Kiểu Saga | **Orchestration** | Choreography: khó theo dõi luồng khi mới học (để stretch) |
| Broker | **RabbitMQ** | Kafka: nặng hơn, hợp event-stream hơn command/reply |
| Vị trí orchestrator | **Trong Order Service** | Service riêng: thêm phức tạp chưa cần (YAGNI) |
| Lưu tiền | **Double-entry ledger** | Update balance trực tiếp: mất audit, dễ race |
| Đảm bảo gửi | **At-least-once + idempotency** | Exactly-once thuần: phức tạp & đắt, không cần |

---

## 13. Future / Stretch

- Bản **choreography** (service phản ứng event của nhau) để so sánh với orchestration.
- Mở rộng **multi-region** theo home-region per wallet.
- Thay Postgres ledger bằng **NewSQL** (CockroachDB) nếu cần ghi-bất-kỳ-đâu nhất quán.
- Thêm **reserve → settle** 2 bước cho nghiệp vụ nhiều giai đoạn.

---

## 14. Glossary

- **Saga:** chuỗi transaction cục bộ + compensation, thay cho transaction phân tán.
- **Compensation:** hành động hoàn tác nghiệp vụ của một bước đã thành công.
- **Idempotency:** thực hiện nhiều lần cho kết quả như một lần.
- **Outbox:** ghi message vào bảng cùng transaction nghiệp vụ; relay publish sau.
- **Double-entry ledger:** sổ cái kép append-only, mỗi giao dịch cân bằng tổng = 0.

---

*Tham chiếu: file `system-design-roadmap.html` — các mục Saga (GĐ3), Idempotency (GĐ3), CDC/Outbox (GĐ3), Database chuyên sâu, và Tính đúng tiền trong game đa region.*
