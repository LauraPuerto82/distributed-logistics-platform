from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class RouteCalculatedPayload(BaseModel):
    path: list[str]
    distance_km: float = Field(ge=0)


class RouteCalculatedEvent(BaseModel):
    event_id: str
    event_type: Literal["RouteCalculated"]
    timestamp: datetime
    shipment_id: str
    payload: RouteCalculatedPayload

class ETAPredictedPayload(BaseModel):
    estimated_travel_minutes: int = Field(ge=0)


class ETAPredictedEvent(BaseModel):
    event_id: str
    event_type: Literal["ETAPredicted"]
    timestamp: datetime
    shipment_id: str
    payload: ETAPredictedPayload