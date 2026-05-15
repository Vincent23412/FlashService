# FlashService

目前已調整為以下資源骨架：

`Client -> Native Ingress -> user-service / order-service`

`user-service -> PostgreSQL (users)`

`PostgreSQL WAL -> Debezium -> Kafka topic user-db-cdc -> order-service consumer -> CockroachDB user_snapshot`

`order-service -> Kafka topic email-job -> worker-service -> failed-dlq`

`KEDA -> 依 email-job consumer lag 擴縮 worker-service`

## 目錄

- [docker-compose.yml](/Users/vincent/Documents/GitHub/FlashService/docker-compose.yml)
- [db/user-init.sql](/Users/vincent/Documents/GitHub/FlashService/db/user-init.sql)
- [db/order-init.sql](/Users/vincent/Documents/GitHub/FlashService/db/order-init.sql)
- [db/user-connector.json](/Users/vincent/Documents/GitHub/FlashService/db/user-connector.json)
- [k8s/](/Users/vincent/Documents/GitHub/FlashService/k8s)

## Docker Compose 資源

- `user-db`: PostgreSQL 16，已開 `wal_level=logical`
- `user-db-init`: 建立 `users`
- `order-db`: CockroachDB
- `order-db-init`: 建立 `products`、`inventory`、`orders`、`order_items`、`user_snapshot`
- `kafka`: 單節點 Kafka
- `kafka-init`: 建立 `user-db-cdc`、`email-job`、`failed-dlq`
- `debezium-connect`: Kafka Connect + Debezium
- `debezium-init`: 註冊 PostgreSQL source connector
- `user-service`
- `order-service`
- `worker-service`
啟動：

```bash
docker compose up --build -d
```

本地 compose 沒有再放 API gateway，直接暴露：

- `http://localhost:8001` -> `user-service`
- `http://localhost:8002` -> `order-service`

## Kubernetes 資源

已新增這些 manifests：

- `k8s/namespace.yaml`
- `k8s/postgres.yaml`
- `k8s/user-db-init-job.yaml`
- `k8s/cockroach.yaml`
- `k8s/order-db-init-job.yaml`
- `k8s/kafka.yaml`
- `k8s/kafka-init-job.yaml`
- `k8s/debezium-connect.yaml`
- `k8s/debezium-connector-job.yaml`
- `k8s/user-service.yaml`
- `k8s/order-service.yaml`
- `k8s/worker-service.yaml`
- `k8s/ingress.yaml`
- `k8s/keda-scaledobject.yaml`

套用順序建議：

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/cockroach.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/user-db-init-job.yaml
kubectl apply -f k8s/order-db-init-job.yaml
kubectl apply -f k8s/kafka-init-job.yaml
kubectl apply -f k8s/debezium-connect.yaml
kubectl apply -f k8s/debezium-connector-job.yaml
kubectl apply -f k8s/user-service.yaml
kubectl apply -f k8s/order-service.yaml
kubectl apply -f k8s/worker-service.yaml
kubectl apply -f k8s/ingress.yaml
kubectl apply -f k8s/keda-scaledobject.yaml
```

## 目前狀態

- 已完成資源骨架與 topic / connector / ingress / scaler 對齊
- `worker-service` 已新增為新的 consumer service
- `order-service` 已改為發送 `email-job`，並預留 `user-db-cdc` consumer
- 這一版重點是「先把資源建起來」
- 完整業務流程、真正的使用者 CRUD、正式 Debezium payload mapping、重試策略與 email sender 仍可再補
