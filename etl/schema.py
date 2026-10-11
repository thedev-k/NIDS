import math
from typing import Optional, Any, Dict
from pydantic import BaseModel, Field, field_validator, ConfigDict


class NetworkFlow(BaseModel):
    """
    Pydantic schema for network flow records.
    Directly satisfies PRD Section 3.4.
    """
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    flow_id: Optional[str] = None
    destination_port: int = Field(0, ge=0, le=65535)
    flow_duration: int = Field(0, ge=0)
    total_fwd_packets: int = Field(0, ge=0)
    total_backward_packets: int = Field(0, ge=0)
    packet_length_mean: float = Field(0.0, ge=0)
    flow_bytes_s: float = Field(0.0)
    flow_packets_s: float = Field(0.0)
    label: str
    is_attack: int = Field(0, ge=0, le=1)

    @field_validator("flow_bytes_s", "flow_packets_s", mode="before")
    @classmethod
    def clean_inf_and_nan(cls, v: Any) -> float:
        if v is None:
            return 0.0
        try:
            val = float(v)
            if math.isnan(val) or math.isinf(val):
                return 0.0
            return val
        except (ValueError, TypeError):
            return 0.0

    @field_validator("destination_port", mode="before")
    @classmethod
    def clamp_destination_port(cls, v: Any) -> int:
        try:
            port = int(float(v))
            return max(0, min(65535, port))
        except (ValueError, TypeError):
            return 0

    @field_validator("flow_duration", "total_fwd_packets", "total_backward_packets", mode="before")
    @classmethod
    def ensure_non_negative_int(cls, v: Any) -> int:
        try:
            val = int(float(v))
            return max(0, val)
        except (ValueError, TypeError):
            return 0

    @field_validator("packet_length_mean", mode="before")
    @classmethod
    def ensure_non_negative_float(cls, v: Any) -> float:
        try:
            val = float(v)
            if math.isnan(val) or math.isinf(val):
                return 0.0
            return max(0.0, val)
        except (ValueError, TypeError):
            return 0.0


def validate_records(records: list[Dict[str, Any]]) -> tuple[list[Dict[str, Any]], int]:
    """
    Validates a list of dictionaries against NetworkFlow model.
    Returns (valid_records, invalid_count).
    """
    valid = []
    invalid_count = 0
    for record in records:
        try:
            flow = NetworkFlow(**record)
            valid.append(flow.model_dump())
        except Exception:
            invalid_count += 1
    return valid, invalid_count
