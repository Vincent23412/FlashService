import os
from typing import Optional
from uuid import UUID

import psycopg2
from psycopg2 import errors
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, ConfigDict, Field
from fastapi import FastAPI, HTTPException, Response, status

app = FastAPI()
DB_URL = os.getenv("DB_URL")


class UserCreate(BaseModel):
    username: str = Field(min_length=1, max_length=50)
    email: str = Field(min_length=3, max_length=100)


class UserUpdate(BaseModel):
    username: Optional[str] = Field(default=None, min_length=1, max_length=50)
    email: Optional[str] = Field(default=None, min_length=3, max_length=100)


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    user_id: UUID
    username: str
    email: str
    created_at: str
    updated_at: str


def get_db_connection():
    if not DB_URL:
        raise HTTPException(status_code=500, detail="DB_URL is not set")
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)


def serialize_user(row: dict) -> dict:
    return {
        "user_id": str(row["user_id"]),
        "username": row["username"],
        "email": row["email"],
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
    }


@app.get("/health")
def health():
    return {"status": "ok", "service": "user-service"}


@app.get("/db/health")
def db_health():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                _ = cur.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"DB check failed: {exc}") from exc

    return {"status": "ok", "service": "user-service", "db": "ok"}


@app.post("/users", status_code=status.HTTP_201_CREATED)
def create_user(payload: UserCreate):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (username, email)
                    VALUES (%s, %s)
                    RETURNING user_id, username, email, created_at, updated_at
                    """,
                    (payload.username, payload.email),
                )
                row = cur.fetchone()
    except errors.UniqueViolation as exc:
        raise HTTPException(status_code=409, detail="email already exists") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Create user failed: {exc}") from exc

    return serialize_user(row)


@app.get("/users")
def list_users(limit: int = 100, offset: int = 0):
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must be >= 0")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT user_id, username, email, created_at, updated_at
                    FROM users
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (limit, offset),
                )
                rows = cur.fetchall()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"List users failed: {exc}") from exc

    return {
        "items": [serialize_user(row) for row in rows],
        "limit": limit,
        "offset": offset,
    }


@app.get("/users/{user_id}")
def get_user(user_id: UUID):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT user_id, username, email, created_at, updated_at
                    FROM users
                    WHERE user_id = %s
                    """,
                    (str(user_id),),
                )
                row = cur.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Get user failed: {exc}") from exc

    if row is None:
        raise HTTPException(status_code=404, detail="user not found")

    return serialize_user(row)


@app.patch("/users/{user_id}")
def update_user(user_id: UUID, payload: UserUpdate):
    updates = payload.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="no fields to update")

    assignments = []
    values = []
    for field, value in updates.items():
        assignments.append(f"{field} = %s")
        values.append(value)
    assignments.append("updated_at = now()")
    values.append(str(user_id))

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE users
                    SET {", ".join(assignments)}
                    WHERE user_id = %s
                    RETURNING user_id, username, email, created_at, updated_at
                    """,
                    values,
                )
                row = cur.fetchone()
    except errors.UniqueViolation as exc:
        raise HTTPException(status_code=409, detail="email already exists") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Update user failed: {exc}") from exc

    if row is None:
        raise HTTPException(status_code=404, detail="user not found")

    return serialize_user(row)


@app.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(user_id: UUID):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE user_id = %s", (str(user_id),))
                deleted_rows = cur.rowcount
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Delete user failed: {exc}") from exc

    if deleted_rows == 0:
        raise HTTPException(status_code=404, detail="user not found")

    return Response(status_code=status.HTTP_204_NO_CONTENT)
