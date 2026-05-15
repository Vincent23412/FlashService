# Order Service API Spec

Base path:

- Kubernetes ingress: `/orders`
- Direct service access: `http://localhost:8002`

## Endpoints

### `POST /orders`

Create an order and enqueue an async email job. The API returns success after:

- order data is committed to CockroachDB
- email job is written to Kafka topic `email-job`

Request body:

```json
{
  "user_id": "2f04c4a4-d1f8-4d2d-a619-6eec3ab8adbb",
  "items": [
    {
      "product_id": "f3698ce6-9b58-4efe-a8b4-a17cc4c4a321",
      "quantity": 2
    },
    {
      "product_id": "f8e9e5be-8521-43f7-a06b-e2426da53cc1",
      "quantity": 1
    }
  ]
}
```

Response `201`:

```json
{
  "status": "accepted",
  "message": "Order persisted and email job queued",
  "order": {
    "order_id": "8cdfa8a5-d6b5-48b7-934a-344db2cf6744",
    "user": {
      "user_id": "2f04c4a4-d1f8-4d2d-a619-6eec3ab8adbb",
      "username": "vincent",
      "email": "vincent@example.com"
    },
    "status": "PENDING",
    "created_at": "2026-05-15T11:00:00.000000",
    "items": [
      {
        "order_item_id": "3d7208f7-69e6-43c0-8fd8-cf0eb8a9cbf1",
        "product_id": "f3698ce6-9b58-4efe-a8b4-a17cc4c4a321",
        "product_name": "Keyboard",
        "quantity": 2,
        "price_at_purchase": "1299.00",
        "subtotal": "2598.00",
        "created_at": "2026-05-15T11:00:00.000000"
      }
    ],
    "summary": {
      "item_count": 1,
      "total_quantity": 2,
      "total_amount": "2598.00"
    }
  }
}
```

Errors:

- `404`: `user_snapshot` or product not found
- `409`: insufficient inventory
- `502`: Kafka publish failed

Kafka email payload includes:

- recipient info: `user_id`, `username`, `email`
- order info: `order_id`, status, items, total amount

### `GET /orders`

List orders.

Query parameters:

- `user_id` optional UUID
- `status_filter` optional, one of `PENDING`, `CONFIRMED`, `CANCELLED`
- `limit` default `100`, range `1..500`
- `offset` default `0`

Response `200`:

```json
{
  "items": [
    {
      "order_id": "8cdfa8a5-d6b5-48b7-934a-344db2cf6744",
      "user": {
        "user_id": "2f04c4a4-d1f8-4d2d-a619-6eec3ab8adbb",
        "username": "vincent",
        "email": "vincent@example.com"
      },
      "status": "PENDING",
      "created_at": "2026-05-15T11:00:00.000000"
    }
  ],
  "limit": 100,
  "offset": 0
}
```

### `GET /orders/{order_id}`

Get full order detail including buyer snapshot and order items.

Response `200`:

```json
{
  "order_id": "8cdfa8a5-d6b5-48b7-934a-344db2cf6744",
  "user": {
    "user_id": "2f04c4a4-d1f8-4d2d-a619-6eec3ab8adbb",
    "username": "vincent",
    "email": "vincent@example.com"
  },
  "status": "PENDING",
  "created_at": "2026-05-15T11:00:00.000000",
  "items": [],
  "summary": {
    "item_count": 0,
    "total_quantity": 0,
    "total_amount": "0"
  }
}
```

Errors:

- `404`: order not found

### `PATCH /orders/{order_id}`

Update order status only.

Request body:

```json
{
  "status": "CONFIRMED"
}
```

Allowed values:

- `PENDING`
- `CONFIRMED`
- `CANCELLED`

Response `200`: same shape as `GET /orders/{order_id}`

### `DELETE /orders/{order_id}`

Delete an order and restore inventory for its items.

Response `204` with empty body.

Errors:

- `404`: order not found

## Notes

- `create order` requires `user_snapshot` to exist first, which is populated from `user-db-cdc`.
- Product price is snapshotted into `order_items.price_at_purchase`.
- Inventory is decremented on create and restored on delete.
- Oversell protection is enforced at the database write step with conditional stock deduction: `UPDATE inventory ... WHERE stock >= requested_quantity`.
