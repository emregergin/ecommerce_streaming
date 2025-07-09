from fastapi import FastAPI
from app.models import SendEvent, PurchasedItem
from app.kafka_producer import send_to_kafka

app = FastAPI()

@app.put("/SendEvent")
def put_send_event(event: SendEvent):
    send_to_kafka("UserEvents", event.dict())
    return {"message": "SendEvent gonderildi"}

@app.post("/PurchasedItems")
def post_purchased_items(items: list[PurchasedItem]):
    for item in items:
        send_to_kafka("PurchasedItem", item.dict())
    return {"message": f"{len(items)} kayit gonderildi"}
