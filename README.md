# FlashService

## Architecture

```mermaid
flowchart LR
    Client[Client]
    Ingress[NGINX Ingress]

    subgraph UserService["User Service"]
        UserAPI[user-service]
        UserDB[(PostgreSQL\nusers)]
        UserAPI -->|CRUD users| UserDB
    end

    subgraph CDC["CDC Pipeline"]
        Debezium[Debezium Connect]
    end

    subgraph Kafka["Kafka"]
        UserCDC[Topic: user-db-cdc]
        EmailJob[Topic: email-job]
        DLQ[Topic: failed-dlq]
    end

    subgraph OrderService["Order Service"]
        OrderAPI[order-service]
        CDCConsumer[user snapshot consumer]
        OrderDB[(CockroachDB\nproducts\ninventory\norders\norder_items\nuser_snapshot)]

        OrderAPI -->|create/update/query orders| OrderDB
        CDCConsumer -->|UPSERT user_snapshot| OrderDB
    end

    subgraph WorkerService["Worker Service"]
        Worker[worker-service]
    end

    Client --> Ingress
    Ingress -->|/users| UserAPI
    Ingress -->|/orders| OrderAPI

    UserDB -->|WAL / logical replication| Debezium
    Debezium -->|publish CDC event| UserCDC
    UserCDC -->|consume| CDCConsumer

    OrderAPI -->|publish async email job| EmailJob
    EmailJob -->|consume| Worker
    Worker -->|log formatted order + recipient info| Worker
    Worker -->|failed message| DLQ
```

## File Structure

```text
.
├── docker-compose.yml
├── kind-config.yaml
├── db
│   ├── user-init.sql
│   ├── order-init.sql
│   └── user-connector.json
├── k8s
│   ├── namespace.yaml
│   ├── postgres.yaml
│   ├── user-db-init-job.yaml
│   ├── cockroach.yaml
│   ├── order-db-init-job.yaml
│   ├── kafka.yaml
│   ├── kafka-init-job.yaml
│   ├── debezium-connect.yaml
│   ├── debezium-connector-job.yaml
│   ├── user-service.yaml
│   ├── order-service.yaml
│   ├── worker-service.yaml
│   ├── ingress.yaml
│   └── keda-scaledobject.yaml
└── docs
    ├── user-service-api.md
    ├── order-service-api.md
    └── testing-guide.md
```

## 文件

- API 規格：
  - [User Service API](/Users/vincent/Documents/GitHub/FlashService/docs/user-service-api.md)
  - [Order Service API](/Users/vincent/Documents/GitHub/FlashService/docs/order-service-api.md)
- 測試流程：
  - [Testing Guide](/Users/vincent/Documents/GitHub/FlashService/docs/testing-guide.md)
