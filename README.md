# FlashService 資料庫 Schema 設計規範

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
