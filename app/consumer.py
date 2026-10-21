import asyncio
import random
from datetime import datetime, timezone, timedelta
from uuid import UUID

import httpx
from faststream.rabbit import RabbitMessage
from sqlalchemy import select, or_
from app.broker import broker, payments_exchange, payments_new, payments_dlq, dlx_exchange
from app.config import settings
from app.db import SessionLocal
from app.models import Payment, WebhookDelivery, DeadLetter

MAX_ATTEMPTS = 3  # всего попыток обработки, включая первую
WEBHOOK_MAX_ATTEMPTS = 3
LOCK_TTL_SECONDS = 60

async def publish_retry(payload: dict, current_attempt: int, error: Exception) -> None:
    """Текущий запуск считается попыткой; ещё два ретрая, затем запись в DLQ."""
    completed_attempts = current_attempt + 1
    if completed_attempts >= MAX_ATTEMPTS:
        dead_payload = {**payload, "attempts": completed_attempts, "failure_reason": str(error)}
        await broker.publish(dead_payload, exchange=dlx_exchange)
        print(f"Перемещено в DLQ после {completed_attempts} попыток: {payload.get('payment_id')}")
        return
    await asyncio.sleep(2 ** current_attempt)  # 1 с, затем 2 с
    await broker.publish({**payload, "attempt": current_attempt + 1},
                         exchange=payments_exchange, routing_key="payments.new")

@broker.subscriber(payments_new, exchange=payments_exchange)
async def process_payment(payload: dict, message: RabbitMessage):
    payment_id = UUID(payload["payment_id"])
    attempt = int(payload.get("attempt", 0))
    try:
        async with SessionLocal() as session:
            async with session.begin():
                payment = await session.scalar(select(Payment).where(Payment.id == payment_id).with_for_update())
                if payment is None or payment.status != "pending":
                    return
                # Эмуляция шлюза. Блокировка строки гарантирует, что финализирует только один воркер.
                await asyncio.sleep(random.uniform(2, 5))
                payment.status = "succeeded" if random.random() < 0.9 else "failed"
                payment.processed_at = datetime.now(timezone.utc)
                event = {
                    "payment_id": str(payment.id), "status": payment.status,
                    "amount": str(payment.amount), "currency": payment.currency,
                    "processed_at": payment.processed_at.isoformat(),
                }
                session.add(WebhookDelivery(payment_id=payment.id, url=payment.webhook_url, payload=event,
                                            status="pending", attempts=0,
                                            next_attempt_at=datetime.now(timezone.utc)))
    except Exception as exc:
        print(f"Ошибка обработки платежа (попытка {attempt + 1}/{MAX_ATTEMPTS}): {exc}")
        await publish_retry(payload, attempt, exc)

@broker.subscriber(payments_dlq, exchange=dlx_exchange)
async def persist_dead_letter(payload: dict):
    """Сохраняем payload DLQ до ack, чтобы сообщения оставались доступны для анализа."""
    async with SessionLocal() as session:
        session.add(DeadLetter(payload=payload, reason=str(payload.get("failure_reason", "max attempts exceeded"))))
        await session.commit()
    print(f"Сохранено dead-letter сообщение для {payload.get('payment_id')}")

async def webhook_delivery_loop():
    """Доставка webhook независимо от обработки платежа, с ограниченным числом повторов."""
    while True:
        delivery_id = None
        try:
            now = datetime.now(timezone.utc)
            async with SessionLocal() as session:
                async with session.begin():
                    delivery = await session.scalar(
                        select(WebhookDelivery)
                        .where(WebhookDelivery.status == "pending")
                        .where(WebhookDelivery.next_attempt_at <= now)
                        .where(or_(WebhookDelivery.locked_at.is_(None),
                                   WebhookDelivery.locked_at < now - timedelta(seconds=LOCK_TTL_SECONDS)))
                        .order_by(WebhookDelivery.next_attempt_at)
                        .with_for_update(skip_locked=True)
                        .limit(1)
                    )
                    if delivery:
                        delivery.locked_at = now
                        delivery.attempts += 1
                        delivery_id = delivery.id
                        url, payload, attempts = delivery.url, delivery.payload, delivery.attempts
            if delivery_id is None:
                await asyncio.sleep(1)
                continue
            error_text = None
            try:
                async with httpx.AsyncClient(timeout=settings.webhook_timeout_seconds) as client:
                    response = await client.post(url, json=payload)
                    response.raise_for_status()
            except Exception as exc:
                error_text = str(exc)
            async with SessionLocal() as session:
                delivery = await session.get(WebhookDelivery, delivery_id)
                if delivery and delivery.status == "pending":
                    delivery.locked_at = None
                    if error_text is None:
                        delivery.status = "delivered"
                        delivery.delivered_at = datetime.now(timezone.utc)
                        delivery.last_error = None
                    else:
                        delivery.last_error = error_text[:4000]
                        if attempts >= WEBHOOK_MAX_ATTEMPTS:
                            delivery.status = "failed"
                        else:
                            delivery.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=2 ** (attempts - 1))
                    await session.commit()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Ошибка webhook worker: {exc}")
            await asyncio.sleep(1)

async def main():
    await broker.connect()
    webhook_task = asyncio.create_task(webhook_delivery_loop())
    try:
        await broker.start()
        while True:
            await asyncio.sleep(3600)
    finally:
        webhook_task.cancel()
        try:
            await webhook_task
        except asyncio.CancelledError:
            pass
        await broker.close()

if __name__ == "__main__":
    asyncio.run(main())
