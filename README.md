# Асинхронный сервис процессинга платежей

Микросервис принимает запросы на оплату, обрабатывает их асинхронно через эмуляцию платёжного шлюза и уведомляет клиента webhook'ом.

**Стек:** FastAPI + Pydantic v2 · SQLAlchemy 2 (async) · PostgreSQL · RabbitMQ (FastStream) · Alembic · Docker Compose

## Быстрый старт

```bash
cp .env.example .env
# при необходимости задайте свой API_KEY в .env
docker compose up --build -d
```

| Сервис | URL |
|--------|-----|
| API | http://localhost:8000 |
| Swagger | http://localhost:8000/docs |
| Health | http://localhost:8000/health |
| RabbitMQ UI | http://localhost:15672 (guest/guest) |

Логи:

```bash
docker compose logs -f api consumer
```

Остановка / полный сброс данных:

```bash
docker compose down        # сохранить тома
docker compose down -v     # удалить тома (БД и RabbitMQ)
```

## API

Все платёжные эндпоинты требуют заголовок `X-API-Key` (значение из `.env`, по умолчанию `dev-api-key-change-me`).

### Создать платёж

`POST /api/v1/payments` → `202 Accepted`

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-api-key-change-me' \
  -H 'Idempotency-Key: order-10001' \
  -d '{
    "amount": "1250.50",
    "currency": "RUB",
    "description": "Оплата заказа 10001",
    "metadata": {"order_id": "10001"},
    "webhook_url": "https://webhook.site/your-unique-id"
  }'
```

Ответ:

```json
{
  "payment_id": "...",
  "status": "pending",
  "created_at": "..."
}
```

Повтор с тем же `Idempotency-Key` и тем же телом — тот же платёж.  
Тот же ключ, но другое тело — `409 Conflict`.

### Получить платёж

`GET /api/v1/payments/{payment_id}`

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> \
  -H 'X-API-Key: dev-api-key-change-me'
```

Статусы: `pending` → `succeeded` | `failed` (эмуляция шлюза: ~90 % success / 10 % fail, 2–5 сек).

## Архитектура

```
Клиент
  │ POST /payments (+ Idempotency-Key, X-API-Key)
  ▼
API ──транзакция──► payments + outbox
                       │
              Outbox relay (в процессе API)
                       │ publish
                       ▼
              RabbitMQ  exchange payments
                       │ queue payments.new
                       ▼
              Consumer ──► эмуляция шлюза
                       │   обновление status
                       │   запись webhook_deliveries
                       ▼
              Webhook worker ──► HTTP POST на webhook_url
                                 (до 3 попыток, exp. backoff)

При ошибке обработки сообщения → retry (3 попытки) → DLQ (payments.dlq)
→ сохранение в dead_letters
```

### Гарантии

| Механизм | Реализация |
|----------|------------|
| **Outbox** | Платёж и событие в одной транзакции; relay публикует и ставит `published_at` |
| **Идемпотентность** | Уникальный `Idempotency-Key` + fingerprint тела запроса |
| **Retry** | 3 попытки обработки сообщения и 3 попытки webhook, экспоненциальная задержка |
| **DLQ** | Exchange `payments.dlx` → очередь `payments.dlq` → таблица `dead_letters` |
| **Auth** | Статический `X-API-Key` на всех платёжных эндпоинтах |

## Структура проекта

```
app/
  main.py            # POST/GET payments
  api_entrypoint.py  # lifespan, outbox loop
  consumer.py        # обработка платежей + webhook worker + DLQ
  outbox.py          # relay outbox → RabbitMQ
  broker.py          # exchanges, очереди, DLQ
  models.py          # payments, outbox, webhook_deliveries, dead_letters
  schemas.py         # Pydantic
  security.py        # X-API-Key
  config.py, db.py
migrations/          # Alembic (0001_initial, 0002_reliability)
docker-compose.yml   # postgres, rabbitmq, migrate, api, consumer
```

## Переменные окружения

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `DATABASE_URL` | PostgreSQL (asyncpg) | см. `.env.example` |
| `RABBITMQ_URL` | AMQP | `amqp://guest:guest@rabbitmq:5672/` |
| `API_KEY` | Ключ API | `dev-api-key-change-me` |
| `OUTBOX_POLL_SECONDS` | Интервал опроса outbox | `1.0` |
| `WEBHOOK_TIMEOUT_SECONDS` | Таймаут HTTP webhook | `5.0` |

## Проверка webhook

Удобно использовать [webhook.site](https://webhook.site/): скопируйте URL в `webhook_url` при создании платежа. После обработки (2–5 с) придёт JSON с `payment_id`, `status`, `amount`, `currency`, `processed_at`.