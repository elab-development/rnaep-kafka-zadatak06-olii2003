from pydantic import BaseModel
from typing import Optional

class Notification(BaseModel):
    order_id: int
    product_id: int
    message: str
    timestamp: str
    error_reason: Optional[str] = None