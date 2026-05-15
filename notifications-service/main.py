from fastapi import FastAPI
from typing import List
from models import Notification
from aiokafka import AIOKafkaConsumer
from contextlib import asynccontextmanager
import asyncio, json
from datetime import datetime, timezone

@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer_confirmed = AIOKafkaConsumer(
        "order-confirmed",
        bootstrap_servers="kafka:9092",
        group_id="notifications-confirmed-group",
        auto_offset_reset="earliest"
    )
    consumer_not_found = AIOKafkaConsumer(
        "product_not_found_events",
        bootstrap_servers="kafka:9092",
        group_id="notifications-not-found-group",
        auto_offset_reset="earliest"
    )
    consumer_out_of_stock = AIOKafkaConsumer(
        "out_of_stock_events",
        bootstrap_servers="kafka:9092",
        group_id="notifications-out-of-stock-group",
        auto_offset_reset="earliest"
    )

    for c in (consumer_confirmed, consumer_not_found, consumer_out_of_stock):
        await c.start()

    tasks = [
        asyncio.create_task(consume_confirmed(consumer_confirmed)),
        asyncio.create_task(consume_errors(consumer_not_found)),
        asyncio.create_task(consume_errors(consumer_out_of_stock)),
    ]

    yield

    for t in tasks:
        t.cancel()
    for c in (consumer_confirmed, consumer_not_found, consumer_out_of_stock):
        await c.stop()

app = FastAPI(title="Notifications Service", lifespan=lifespan)

notifications_db: List[Notification] = []

async def consume_confirmed(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode("utf-8"))
            notifications_db.append(Notification(
                order_id=data["order_id"],
                product_id=data["product_id"],
                message=(
                    f"Narudžbina #{data['order_id']} za proizvod #{data['product_id']} "
                    f"je uspešno potvrđena."
                ),
                timestamp=datetime.now(timezone.utc).isoformat(),
                error_reason=None
            ))
    except asyncio.CancelledError:
        pass

async def consume_errors(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode("utf-8"))
            reason = data.get("error_reason", "Nepoznata greška")
            notifications_db.append(Notification(
                order_id=data["order_id"],
                product_id=data["product_id"],
                message=(
                    f"Narudžbina #{data['order_id']} je odbijena. "
                    f"Razlog: {reason}"
                ),
                timestamp=data.get("timestamp", datetime.now(timezone.utc).isoformat()),
                error_reason=reason
            ))
    except asyncio.CancelledError:
        pass

@app.get("/notifications", response_model=List[Notification])
def get_notifications():
    return notifications_db