from pydantic import BaseModel, Field
from datetime import datetime, timezone
from typing import Optional

class UCIPayLoad(BaseModel):

    house_id: str
    current_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    original_datetime: str
    global_active_power: Optional[float] = None
    global_reactive_power: Optional[float] = None
    voltage: Optional[float] = None
    Global_intensity: Optional[float] = None
    Sub_metering_1: Optional[float] = None
    Sub_metering_2: Optional[float] = None
    Sub_metering_3: Optional[float] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
