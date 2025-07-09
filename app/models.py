from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class SendEvent(BaseModel):
    UserId: str
    SessionId: str
    EventName: str  # (PageVisited, AddedBasket, CheckedProductReviews)
    TimeStamp: datetime
    Attributes: Optional[dict]
    ProductId: str
    Price: float
    Discount: float

class Product(BaseModel):
    ProductId: str
    ItemCount: int
    ItemPrice: float
    ItemDiscount: float

class PurchasedItem(BaseModel):
    SessionId: str
    TimeStamp: datetime
    UserId: str
    TotalPrice: float
    OrderId: str
    Products: List[Product]
    PaymentType: str
