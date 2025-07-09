from faker import Faker
import requests
import random
import time
from datetime import datetime

fake = Faker()

API_BASE_URL = "http://localhost:8000"

event_names = ["PageVisited", "AddedBasket", "CheckedProductReviews"]
payment_types = ["CreditCard", "Cash"]

def generate_send_event():
    data = {
        "UserId": fake.uuid4(),
        "SessionId": fake.uuid4(),
        "EventName": random.choice(event_names),
        "TimeStamp": datetime.utcnow().isoformat(),
        "Attributes": {
            "ProductId": str(fake.uuid4()),
            "Price": round(random.uniform(10, 500), 2),
            "Discount": round(random.uniform(0, 50), 2)
        }
    }
    response = requests.put(f"{API_BASE_URL}/SendEvent", json=data)
    print("SendEvent:", response.status_code, response.text)

def generate_purchased_item():
    session_id = fake.uuid4()
    user_id = fake.uuid4()
    product_count = random.randint(1, 5)
    products = []
    total_price = 0.0

    for _ in range(product_count):
        price = round(random.uniform(50.0, 500.0), 2)
        discount = round(random.uniform(0, 50), 2)
        count = random.randint(1, 3)
        products.append({
            "ProductId": fake.uuid4(),
            "ItemCount": count,
            "ItemPrice": price,
            "ItemDiscount": discount
        })
        total_price = total_price + (price - discount) * count

    item = {
        "SessionId": session_id,
        "TimeStamp": datetime.utcnow().isoformat(),
        "UserId": user_id,
        "TotalPrice": round(total_price, 2),
        "OrderId": fake.uuid4(),
        "Products": products,
        "PaymentType": random.choice(payment_types)
    }

    response = requests.post(f"{API_BASE_URL}/PurchasedItems", json=[item])
    print("PurchasedItems:", response.status_code, response.text)

def run():
    while True:
        generate_send_event()
        generate_purchased_item()
        time.sleep(1)

if __name__ == "__main__":
    run()
