import hashlib
import json
from uuid import UUID
from fastapi import FastAPI, Depends, Header, HTTPException, Response
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from app.db import get_session
from app.models import Payment, Outbox
from app.schemas import PaymentCreate, PaymentAccepted, PaymentRead
from app.security import require_api_key

app = FastAPI(title="Async Payment Processing API", version="1.1.0")

@app.get("/health")
async def health():
    return {"status": "ok"}

def fingerprint(body: PaymentCreate) -> str:
    canonical = json.dumps(body.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

@app.post("/api/v1/payments", response_model=PaymentAccepted, status_code=202,
          dependencies=[Depends(require_api_key)])
async def create_payment(
    body: PaymentCreate,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    session: AsyncSession = Depends(get_session),
):
    key = (idempotency_key or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")
    if len(key) > 255:
        raise HTTPException(status_code=400, detail="Idempotency-Key must be at most 255 characters")
    request_fingerprint = fingerprint(body)

    existing = await session.scalar(select(Payment).where(Payment.idempotency_key == key))
    if existing:
        if existing.request_fingerprint != request_fingerprint:
            raise HTTPException(status_code=409, detail="Idempotency-Key was already used with a different request body")
        return PaymentAccepted(payment_id=existing.id, status=existing.status, created_at=existing.created_at)

    payment = Payment(
        amount=body.amount, currency=body.currency, description=body.description,
        metadata_json=body.metadata, status="pending", idempotency_key=key,
        request_fingerprint=request_fingerprint, webhook_url=str(body.webhook_url),
    )
    session.add(payment)
    await session.flush()  # Получаем payment.id до создания записи outbox.
    session.add(Outbox(event_type="payment.created", aggregate_id=payment.id, payload={
        "payment_id": str(payment.id), "attempt": 0
    }))
    try:
        await session.commit()
    except IntegrityError:
        # Параллельный запрос мог уже вставить тот же idempotency key.
        await session.rollback()
        existing = await session.scalar(select(Payment).where(Payment.idempotency_key == key))
        if existing:
            if existing.request_fingerprint != request_fingerprint:
                raise HTTPException(status_code=409, detail="Idempotency-Key was already used with a different request body")
            return PaymentAccepted(payment_id=existing.id, status=existing.status, created_at=existing.created_at)
        raise
    await session.refresh(payment)
    return PaymentAccepted(payment_id=payment.id, status=payment.status, created_at=payment.created_at)

@app.get("/api/v1/payments/{payment_id}", response_model=PaymentRead,
         dependencies=[Depends(require_api_key)])
async def get_payment(payment_id: UUID, session: AsyncSession = Depends(get_session)):
    payment = await session.get(Payment, payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    return payment
