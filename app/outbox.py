import asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, or_
from app.config import settings
from app.db import SessionLocal
from app.models import Outbox
from app.broker import broker, payments_exchange

LOCK_TTL_SECONDS = 60
BATCH_SIZE = 100

async def outbox_loop():
    """Резервируем строки в короткой транзакции, публикуем снаружи, затем фиксируем результат.

    Просроченная блокировка снимается после падения процесса. Публикация at-least-once;
    consumer обязан быть идемпотентным.
    """
    while True:
        try:
            now = datetime.now(timezone.utc)
            async with SessionLocal() as session:
                async with session.begin():
                    rows = (await session.scalars(
                        select(Outbox)
                        .where(Outbox.published_at.is_(None))
                        .where(or_(Outbox.locked_at.is_(None), Outbox.locked_at < now - timedelta(seconds=LOCK_TTL_SECONDS)))
                        .order_by(Outbox.created_at)
                        .limit(BATCH_SIZE)
                        .with_for_update(skip_locked=True)
                    )).all()
                    claimed = [(row.id, row.payload) for row in rows]
                    for row in rows:
                        row.locked_at = now
                        row.attempts += 1
            for row_id, payload in claimed:
                try:
                    await broker.publish(payload, exchange=payments_exchange,
                                         routing_key="payments.new", message_id=str(row_id))
                except Exception as exc:
                    async with SessionLocal() as session:
                        row = await session.get(Outbox, row_id)
                        if row and row.published_at is None:
                            row.locked_at = None
                            await session.commit()
                    print(f"Ошибка публикации outbox для {row_id}: {exc}")
                    continue
                async with SessionLocal() as session:
                    row = await session.get(Outbox, row_id)
                    if row and row.published_at is None:
                        row.published_at = datetime.now(timezone.utc)
                        row.locked_at = None
                        await session.commit()
        except Exception as exc:
            print(f"Ошибка outbox relay: {exc}")
        await asyncio.sleep(settings.outbox_poll_seconds)
