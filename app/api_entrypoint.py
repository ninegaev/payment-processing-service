import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from app.main import app as api_app
from app.broker import broker
from app.outbox import outbox_loop


@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.connect()
    task = asyncio.create_task(outbox_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await broker.close()


api_app.router.lifespan_context = lifespan
app = api_app
