# FlashService Testing Guide

這份文件整理兩套測試方式：

- `Docker Compose`：本地直接測 service port
- `kind + ingress-nginx`：以 Kubernetes ingress 路徑測試

## Prerequisites

- Docker Desktop or compatible Docker runtime
- `kubectl`
- `kind` if you want to test Kubernetes mode

---

## Docker Compose

### 1. Start services

```bash
docker compose down
docker compose up --build -d
docker compose ps
```

確認至少有：

- `user-db`
- `order-db`
- `kafka`
- `debezium-connect`
- `user-service`
- `order-service`
- `worker-service`

### 2. Health checks

```bash
curl http://localhost:8001/health
curl http://localhost:8001/db/health
curl http://localhost:8002/health
curl http://localhost:8002/kafka/health
```

### 3. User API

Create user:

```bash
curl -X POST http://localhost:8001/users \
  -H "Content-Type: application/json" \
  -d '{
    "username": "vincent",
    "email": "vincent@example.com"
  }'
```

List users:

```bash
curl "http://localhost:8001/users?limit=20&offset=0"
```

Get one user:

```bash
curl http://localhost:8001/users/<USER_ID>
```

Update user:

```bash
curl -X PATCH http://localhost:8001/users/<USER_ID> \
  -H "Content-Type: application/json" \
  -d '{
    "username": "vincent-lin"
  }'
```

Delete user:

```bash
curl -X DELETE -i http://localhost:8001/users/<USER_ID>
```

### 4. Seed products and inventory

```bash
docker compose exec order-db cockroach sql --insecure --host=localhost -d defaultdb -e "
INSERT INTO products (product_id, name, description, price)
VALUES
  ('11111111-1111-1111-1111-111111111111', 'Keyboard', 'Mechanical keyboard', 1299.00),
  ('22222222-2222-2222-2222-222222222222', 'Mouse', 'Wireless mouse', 799.00)
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO inventory (product_id, stock)
VALUES
  ('11111111-1111-1111-1111-111111111111', 10),
  ('22222222-2222-2222-2222-222222222222', 20)
ON CONFLICT (product_id) DO NOTHING;
"
```

Verify seed:

```bash
docker compose exec order-db cockroach sql --insecure --host=localhost -d defaultdb -e "
SELECT p.product_id, p.name, p.price, i.stock
FROM products p
JOIN inventory i ON p.product_id = i.product_id
ORDER BY p.name;
"
```

### 5. Verify CDC sync

After `debezium-connect` is ready, update or create a user, then check:

```bash
docker compose exec order-db cockroach sql --insecure --host=localhost -d defaultdb -e "
SELECT *
FROM user_snapshot
WHERE user_id = '<USER_ID>';
"
```

### 6. Order API

Create order:

```bash
curl -X POST http://localhost:8002/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "<USER_ID>",
    "items": [
      {
        "product_id": "11111111-1111-1111-1111-111111111111",
        "quantity": 2
      },
      {
        "product_id": "22222222-2222-2222-2222-222222222222",
        "quantity": 1
      }
    ]
  }'
```

List orders:

```bash
curl "http://localhost:8002/orders?limit=20&offset=0"
curl "http://localhost:8002/orders?user_id=<USER_ID>&status_filter=PENDING"
```

Get one order:

```bash
curl http://localhost:8002/orders/<ORDER_ID>
```

Update order status:

```bash
curl -X PATCH http://localhost:8002/orders/<ORDER_ID> \
  -H "Content-Type: application/json" \
  -d '{
    "status": "CONFIRMED"
  }'
```

Delete order:

```bash
curl -X DELETE -i http://localhost:8002/orders/<ORDER_ID>
```

### 7. Verify worker log

```bash
docker compose logs -f worker-service
```

Expected behavior:

- order is persisted first
- email job is published to Kafka
- worker prints formatted recipient and order details
- `email-job` uses `10` partitions so KEDA can scale worker replicas up to `10` effectively

### 8. Verify oversell protection

```bash
curl -X POST http://localhost:8002/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "<USER_ID>",
    "items": [
      {
        "product_id": "11111111-1111-1111-1111-111111111111",
        "quantity": 999
      }
    ]
  }'
```

Expected response:

- HTTP `409`
- `insufficient inventory`

---

## kind + ingress-nginx

### 1. Create cluster

This project includes [kind-config.yaml](/Users/vincent/Documents/GitHub/FlashService/kind-config.yaml), which:

- maps host `80/443`
- labels the control-plane node with `ingress-ready=true`

```bash
kind delete cluster --name kind
kind create cluster --name kind --config kind-config.yaml
kind export kubeconfig --name kind
kubectl get nodes --show-labels | grep ingress-ready
```

### 2. Install ingress-nginx

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
kubectl wait --namespace ingress-nginx \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout=180s
kubectl get pod -n ingress-nginx -o wide
```

Controller should run on `kind-control-plane`.

### 3. Build and load local images

```bash
docker build -t flashservice/user-service:latest ./microservices/user-service
docker build -t flashservice/order-service:latest ./microservices/order-service
docker build -t flashservice/worker-service:latest ./microservices/worker-service

kind load docker-image flashservice/user-service:latest --name kind
kind load docker-image flashservice/order-service:latest --name kind
kind load docker-image flashservice/worker-service:latest --name kind
```

### 4. Deploy infrastructure and apps

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/cockroach.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/user-db-init-job.yaml
kubectl apply -f k8s/order-db-init-job.yaml
kubectl apply -f k8s/kafka-init-job.yaml
kubectl wait --for=condition=complete job/user-db-init -n flashservice --timeout=180s
kubectl wait --for=condition=complete job/order-db-init -n flashservice --timeout=180s
kubectl wait --for=condition=complete job/kafka-init -n flashservice --timeout=180s

kubectl apply -f k8s/debezium-connect.yaml
kubectl apply -f k8s/debezium-connector-job.yaml
kubectl apply -f k8s/user-service.yaml
kubectl apply -f k8s/order-service.yaml
kubectl apply -f k8s/worker-service.yaml
kubectl apply -f k8s/ingress.yaml
```

### 5. Verify pods

```bash
kubectl get pods -n flashservice
kubectl get ingress -n flashservice
```

Expected steady state:

- `debezium-connect` running
- `kafka`, `user-db`, `order-db` running
- `user-service`, `order-service`, `worker-service` running
- `email-job` topic created with `10` partitions

### 6. Test through ingress

Health endpoints:

```bash
curl http://localhost/users/health
curl http://localhost/users/db/health
curl http://localhost/orders/health
curl http://localhost/orders/kafka/health
```

User CRUD via ingress:

```bash
curl -X POST http://localhost/users \
  -H "Content-Type: application/json" \
  -d '{
    "username": "vincent",
    "email": "vincent@example.com"
  }'

curl "http://localhost/users?limit=20&offset=0"
curl http://localhost/users/<USER_ID>

curl -X PATCH http://localhost/users/<USER_ID> \
  -H "Content-Type: application/json" \
  -d '{
    "username": "vincent-lin"
  }'
```

Check CDC sync:

```bash
kubectl exec -n flashservice deploy/order-db -- \
  cockroach sql --insecure --host=localhost -d defaultdb \
  -e "SELECT * FROM user_snapshot WHERE user_id = '<USER_ID>';"
```

Seed products and inventory:

```bash
kubectl exec -n flashservice deploy/order-db -- \
  cockroach sql --insecure --host=localhost -d defaultdb -e "
INSERT INTO products (product_id, name, description, price)
VALUES
  ('11111111-1111-1111-1111-111111111111', 'Keyboard', 'Mechanical keyboard', 1299.00),
  ('22222222-2222-2222-2222-222222222222', 'Mouse', 'Wireless mouse', 799.00)
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO inventory (product_id, stock)
VALUES
  ('11111111-1111-1111-1111-111111111111', 10),
  ('22222222-2222-2222-2222-222222222222', 20)
ON CONFLICT (product_id) DO NOTHING;
"
```

Create order:

```bash
curl -X POST http://localhost/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "<USER_ID>",
    "items": [
      {
        "product_id": "11111111-1111-1111-1111-111111111111",
        "quantity": 2
      },
      {
        "product_id": "22222222-2222-2222-2222-222222222222",
        "quantity": 1
      }
    ]
  }'
```

List and get orders:

```bash
curl "http://localhost/orders?limit=20&offset=0"
curl "http://localhost/orders?user_id=<USER_ID>&status_filter=PENDING"
curl http://localhost/orders/<ORDER_ID>
```

Watch worker log:

```bash
kubectl logs -n flashservice deploy/worker-service --tail=100 -f
```

Oversell test:

```bash
curl -X POST http://localhost/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "<USER_ID>",
    "items": [
      {
        "product_id": "11111111-1111-1111-1111-111111111111",
        "quantity": 999
      }
    ]
  }'
```

Expected response:

- HTTP `409`

### 7. Useful troubleshooting

Pods:

```bash
kubectl get pods -n flashservice
kubectl get pods -n ingress-nginx -o wide
```

Logs:

```bash
kubectl logs -n flashservice deploy/debezium-connect --tail=200
kubectl logs -n flashservice deploy/order-service --tail=200
kubectl logs -n flashservice deploy/worker-service --tail=200
```

Ingress:

```bash
kubectl get ingress -n flashservice
kubectl describe ingress -n flashservice
```
