from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue, ExchangeType
from app.config import settings

broker = RabbitBroker(settings.rabbitmq_url)

# Топология: exchange + основная очередь с DLX + DLQ
payments_exchange = RabbitExchange(
    "payments",
    type=ExchangeType.TOPIC,
    durable=True,
)

dlx_exchange = RabbitExchange(
    "payments.dlx",
    type=ExchangeType.FANOUT,
    durable=True,
)

payments_dlq = RabbitQueue(
    "payments.dlq",
    durable=True,
)

# Основная очередь с аргументом dead-letter exchange
payments_new = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key="payments.new",
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
    },
)
