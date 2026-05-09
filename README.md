## FlashService 資料庫 Schema 設計規範

本文件詳細說明 FlashService 專案的資料庫設計。為了應對「秒殺」高併發場景，本系統採用 CockroachDB 並遵循以下 Schema 設計原則。

---

## 🛠️ Schema 設計原則

1.  **分散式主鍵 (UUID)**
    *   避免使用遞增整數 (Auto-increment)，改用隨機 UUID 以防止分散式資料庫產生寫入熱點 (Write Hotspots)。
2.  **庫存與商品分離 (Inventory Separation)**
    *   將頻繁變動的「庫存數量」從「商品基本資訊」中抽離，降低更新庫存時的鎖定衝突。
3.  **資料庫級防超賣 (CHECK Constraint)**
    *   在 `inventory` 表設置 `CHECK (stock >= 0)` 約束，確保庫存永不為負數。

---

## schema 資料表定義

### 1. 使用者表 (`users`)
儲存買家的基本帳號資訊。

| 欄位名稱 | 類型 | 說明 |
| :--- | :--- | :--- |
| `user_id` | UUID | 唯一識別碼 (Primary Key) |
| `username` | VARCHAR | 使用者名稱 |
| `email` | VARCHAR | 電子信箱 (Unique) |
| `created_at` | TIMESTAMP | 帳號建立時間 |

### 2. 商品表 (`products`)
存放商品型錄資訊。

| 欄位名稱 | 類型 | 說明 |
| :--- | :--- | :--- |
| `product_id` | UUID | 商品唯一 ID (Primary Key) |
| `name` | VARCHAR | 商品名稱 |
| `price` | DECIMAL | 商品目前售價 |
| `description` | TEXT | 商品描述 |

### 3. 庫存表 (`inventory`)
秒殺系統核心，負責處理高頻庫存扣減。

| 欄位名稱 | 類型 | 說明 |
| :--- | :--- | :--- |
| `product_id` | UUID | 外鍵，對應 `products.product_id` |
| `stock` | INT | **剩餘庫存** (CHECK stock >= 0) |
| `updated_at` | TIMESTAMP | 最後更新時間 |

### 4. 訂單主檔 (`orders`)
記錄交易整體狀態。

| 欄位名稱 | 類型 | 說明 |
| :--- | :--- | :--- |
| `order_id` | UUID | 訂單唯一編號 (Primary Key) |
| `user_id` | UUID | 買家 ID |
| `status` | VARCHAR | 狀態 (PENDING, SUCCESS, FAILED) |
| `created_at` | TIMESTAMP | 下單時間 |

### 5. 訂單明細表 (`order_items`)
記錄單筆訂單內的商品清單。

| 欄位名稱 | 類型 | 說明 |
| :--- | :--- | :--- |
| `order_item_id` | UUID | 明細唯一 ID |
| `order_id` | UUID | 對應 `orders.order_id` |
| `product_id` | UUID | 購買商品 ID |
| `quantity` | INT | 購買數量 (> 0) |
| `price_at_purchase`| DECIMAL | **成交時的價格** (鎖定歷史價格) |

---

## D1 Kafka + KEDA 事件驅動基礎建設

本專案採用 **Kafka** 作為事件串流平台，搭配 **KEDA** 實現根據事件量自動擴縮容。以下為 D1 組負責的元件與操作說明。

---

### 一、Kafka Topic 規劃

| Topic 名稱 | Partitions | 用途 | 生產者 | 消費者 |
|---|---|---|---|---|
| `order.created` | 3 | 訂單建立事件 | Order Service | Notification Worker |
| `notification` | 3 | 通知待發送事件 | Order Service (轉發) | Notification Worker |
| `inventory.updated` | 3 | 庫存變動事件 | Inventory Service | Notification / Analytics |

> 每個 Topic 設定 3 個 Partitions，確保 Consumer Group 最多可平行擴展至 3 個 Consumer，達到 3 倍吞吐量。

---

### 二、Docker Compose 啟動方式

#### 1. 一鍵啟動全部服務

```bash
docker compose up --build -d
```

啟動順序由 Docker Compose 自動管理：

1. `db`（CockroachDB）healthcheck 通過
2. `db-init` 執行 Schema 初始化
3. `kafka` healthcheck 通過（Broker 就緒）
4. `kafka-init` 建立 Topics（3 個 partitions）
5. `order-service`、`user-service`、`notification-service` 啟動
6. `gateway`（Envoy）啟動

#### 2. 驗證 Kafka Topic 已建立

```bash
docker compose exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

預期輸出：
```
inventory.updated
notification
order.created
```

#### 3. 驗證 Order Service 的 Producer 可用

```bash
curl -X POST http://localhost/api/orders/order \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user-001","items":[{"product_id":"prod-001","quantity":1}]}'
```

預期回應：
```json
{"status":"ok","order_id":"order-...","message":"Order accepted, event published to Kafka"}
```

#### 4. 觀察 Notification Worker 消費訊息

```bash
docker compose logs -f notification-service
```

預期輸出：
```
[Notification] To: user-001, Order: order-..., Event: order.created, Msg: Order confirmed (simulated).
```

---

### 三、Kubernetes + KEDA 啟動方式

#### 1. 前置需求

- 已啟用的 Kubernetes 叢集（Docker Desktop、minikube、kind 均可）
- 已安裝 [KEDA](https://keda.sh/docs/2.14/deploy/)：

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm install keda kedacore/keda --namespace keda --create-namespace
```

#### 2. 部署基礎設施

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka.yaml
kubectl wait --for=condition=ready pod -l app=kafka -n flashservice --timeout=120s
kubectl apply -f k8s/kafka-init-job.yaml
kubectl wait --for=condition=complete job/kafka-init -n flashservice --timeout=120s
```

#### 3. 部署 Notification Worker

```bash
kubectl apply -f k8s/notification-service.yaml
```

#### 4. 部署 KEDA ScaledObject

```bash
kubectl apply -f k8s/keda-scaledobject.yaml
```

#### 5. 驗證 KEDA 是否生效

```bash
kubectl get scaledobject -n flashservice
kubectl get hpa -n flashservice
```

預期可見一個由 KEDA 自動生成的 HPA 資源，名稱類似 `keda-hpa-notification-service-scaler`。

---

### 四、觀察 Consumer Lag 與自動擴縮

#### 1. 持續觀察 Pod 數量變化

```bash
kubectl get pods -n flashservice -w
```

#### 2. 觀察 Consumer Lag（需進入 Kafka pod 或本機安裝 Kafka CLI）

```bash
kubectl exec -n flashservice deploy/kafka -- \
  kafka-consumer-groups.sh \
  --bootstrap-server kafka.flashservice.svc.cluster.local:9093 \
  --describe \
  --group notification-service
```

觀察重點欄位：
- `CURRENT-OFFSET`：Consumer 已處理到的位置
- `LOG-END-OFFSET`：Topic 最新的訊息位置
- `LAG`：尚未處理的訊息數量（`LOG-END-OFFSET - CURRENT-OFFSET`）

#### 3. 模擬壓力產生 Lag

使用 `curl` 在迴圈中發送多筆訂單：

```bash
for i in $(seq 1 100); do
  curl -X POST http://localhost/api/orders/order \
    -H "Content-Type: application/json" \
    -d "{\"user_id\":\"user-$i\",\"items\":[{\"product_id\":\"prod-001\",\"quantity\":1}]}"
done
```

#### 4. 預期的擴縮行為

| 階段 | Lag | Pod 數量 | 說明 |
|---|---|---|---|
| 壓測開始 | 0 → 上升 | 1 | 初始狀態 |
| Lag > 5 | 持續增加 | 1 → 3 → ... → 10 | KEDA 擴增，30 秒內反應 |
| 壓測停止 | 逐漸歸零 | 10 維持 | Consumer 消化中 |
| Lag = 0 | 0 | 10 → 1 | Cooldown 300 秒後縮減 |

注意：由於 `notification` topic 有 **3 個 partitions**，即使 KEDA 擴增到 10 個 pods，也只有 **3 個 pods** 能真正平行讀取訊息。其餘 7 個將處於閒置狀態。這正是 partition 數量決定 scaling 上限的具體展示。

---

### 五、常見問題（FAQ）

#### Q1: KEDA 沒有擴增 pod，怎麼辦？

1. 確認 Kafka consumer group 名稱與 ScaledObject 中的 `consumerGroup` 完全一致。
2. 確認 `bootstrapServers` 在 K8s 內部可解析（使用 Service DNS：`kafka.flashservice.svc.cluster.local:9093`）。
3. 使用 `kubectl logs -n keda deployment/keda-operator` 查看 scaler 錯誤訊息。
4. 調降 `lagThreshold` 到 `2` 或 `1`，確保少量 lag 也能觸發（測試用途）。

#### Q2: Consumer lag 沒有下降，但 pods 很多？

1. 確認 `partition` 數量是否足夠。若只有 1 個 partition，則只有 1 個 consumer 能讀取，其餘 pods 閒置。
2. 確認 Consumer 是否成功加入 group。查看 `kafka-consumer-groups.sh --describe` 的 `CONSUMER-ID` 欄位。
3. 確認 Consumer 沒有拋出未捕獲異常導致持續重啟。查看 `kubectl logs notification-service-xxx`。

#### Q3: Kafka auto-create topic 的 partition 數量不對？

`KAFKA_AUTO_CREATE_TOPICS_ENABLE` 預設會建立 1 個 partition。因此**不要依賴 auto-create**。`kafka-init` 服務會在啟動時主動以 `3 partitions` 建立所需 topics，確保正確的 partition 數量。

#### Q4: Docker Compose 與 Kubernetes 的 Kafka 連線位址差異？

| 環境 | Producer / Consumer 連線位址 |
|---|---|
| Docker Compose | `kafka:9093`（內部 Docker 網路） |
| Kubernetes | `kafka.flashservice.svc.cluster.local:9093`（K8s Service DNS） |

請確保各 service 的環境變數 `KAFKA_BROKERS` 依據目標環境設定正確的 bootstrap server。

---

## 技術元件負責人

| 組別 | 負責人 | 負責技術 |
|---|---|---|
| A1 | 晉綾 | 文件統籌、期中報告（深入介紹 Kafka 架構與原理）、整合全組 AI 協作紀錄、撰寫/維護 README |
| A2 | 品力 | 壓力測試與 NFR 實驗（導入 k6 或 Locust、撰寫高併發腳本、收集吞吐量/延遲/錯誤率數據並視覺化） |
| B1 | Robin | 前端介面開發（React/Next.js 前台：商品列表、購物車、訂單查詢；商家後台：商品上架、庫存設定、訂單列表）、前端路由與狀態管理 |
| B2 | 懷生 | User Service（註冊、登入、身份驗證）、Catalog Service（商品 CRUD）、定義前端 API 規格、透過 Envoy 串接 C 組服務 |
| C1 | 偉杰 | Order Service（下單邏輯、庫存扣減、與 Kafka Producer 銜接） |
| C2 | 柏慶 | Inventory Service（庫存管理、防超賣機制、inventory.updated producer） |
| D1 | 凱輝 | Kafka 事件串流、KEDA 自動擴縮容、Notification Worker、Topic 管理 |
| D2 | 至弘 | CockroachDB 叢集、Envoy Gateway、資料庫 Schema |
| D3 | 敬翰 | Docker Compose 基礎建設、Monorepo 骨架、CI/CD 整合 |