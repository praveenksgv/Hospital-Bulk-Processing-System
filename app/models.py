from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, field_validator


class HospitalInput(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(min_length=1)
    address: str = Field(min_length=1)
    phone: Optional[str] = None

    @field_validator("name", "address", "phone")
    @classmethod
    def no_nul(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and "\x00" in value:
            raise ValueError("NUL characters are not allowed in hospital fields")
        return value

    def identity(self):
        return (self.name, self.address, self.phone)


class UpstreamHospital(BaseModel):
    id: StrictInt = Field(gt=0)
    name: StrictStr = Field(min_length=1)
    address: StrictStr = Field(min_length=1)
    phone: Optional[StrictStr] = None
    creation_batch_id: UUID
    active: StrictBool

    def identity(self):
        return (self.name.strip(), self.address.strip(), (self.phone or "").strip() or None)


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: Optional[Any] = None


class ErrorResponse(BaseModel):
    error: ErrorDetail


class HospitalResult(BaseModel):
    row: int
    hospital_id: Optional[int] = None
    name: str
    status: str
    error: Optional[ErrorDetail] = None


class BatchResult(BaseModel):
    batch_id: UUID
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    processing_time_seconds: float
    batch_activated: bool
    hospitals: List[HospitalResult]
    error: Optional[ErrorDetail] = None


class ValidationResult(BaseModel):
    valid: bool = True
    input_rows: int
    unique_hospitals: int
    duplicates_removed: int
    duplicate_rows: Dict[str, List[int]]


class BatchStatus(BatchResult):
    instance_id: UUID
    status: str
    attempt_number: int
    revision: int
    input_rows: int
    duplicates_removed: int
    duplicate_rows: Dict[str, List[int]]
    successful_hospitals: int
    unknown_hospitals: int
    pending_hospitals: int
    in_progress_hospitals: int
    not_attempted_hospitals: int
    progress_percentage: float
    activation_status: str
    can_resume: bool
    created_at: str
    updated_at: str
    finished_at: Optional[str] = None
    expires_at: Optional[str] = None
    row_attempts: Dict[str, int]
