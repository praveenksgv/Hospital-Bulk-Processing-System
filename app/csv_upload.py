"""Pure CSV parsing, projection, deduplication and Pydantic validation."""

import csv
import hashlib
import io
import json
from dataclasses import dataclass
from typing import List

from pydantic import ValidationError

from app.config import Settings
from app.errors import APIError
from app.models import HospitalInput, ValidationResult

# csv's default 128 KiB field limit is below our documented upload limit.
# This is configured once, never changed concurrently by request handlers.
csv.field_size_limit(2**31 - 1)


@dataclass
class CSVRecord:
    row: int
    hospital: HospitalInput
    source_rows: List[int]


@dataclass
class ParsedCSV:
    input_rows: int
    records: List[CSVRecord]

    def summary(self) -> ValidationResult:
        return ValidationResult(
            input_rows=self.input_rows,
            unique_hospitals=len(self.records),
            duplicates_removed=self.input_rows - len(self.records),
            duplicate_rows={
                str(r.row): r.source_rows[1:] for r in self.records if len(r.source_rows) > 1
            },
        )

    def fingerprint(self) -> str:
        payload = [r.hospital.model_dump() for r in self.records]
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()


def parse_csv(contents: bytes, settings: Settings) -> ParsedCSV:
    if len(contents) > settings.max_file_bytes:
        raise APIError(413, "file_too_large", "The CSV exceeds the configured file-size limit.")
    try:
        text = contents.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise APIError(400, "invalid_encoding", "The CSV must use UTF-8 encoding.") from exc
    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    try:
        headers = next((row for row in reader if row), None)
        if headers is None:
            raise APIError(422, "empty_csv", "The CSV is empty.")
        positions = {}
        for position, header in enumerate(headers):
            name = header.strip().upper()
            if name not in {"NAME", "ADDRESS", "PHONE"}:
                continue
            if name in positions:
                raise APIError(422, "duplicate_header", "Recognized headers must be unique.", name)
            positions[name] = position
        missing = sorted({"NAME", "ADDRESS"} - positions.keys())
        if missing:
            raise APIError(422, "missing_headers", "Required CSV headers are missing.", missing)

        unique = {}
        input_rows = 0
        for row_number, cells in enumerate(reader, 1):
            if not cells:
                continue
            input_rows += 1
            values = {}
            for column in ("NAME", "ADDRESS", "PHONE"):
                position = positions.get(column)
                value = (
                    cells[position].strip()
                    if position is not None and position < len(cells)
                    else ""
                )
                values[column.lower()] = (value or None) if column == "PHONE" else value
            identity = (values["name"], values["address"], values["phone"])
            if identity in unique:
                unique[identity][2].append(row_number)
            else:
                unique[identity] = (row_number, values, [row_number])
    except csv.Error as exc:
        raise APIError(
            400, "malformed_csv", "The CSV contains malformed records or quoting."
        ) from exc

    if not unique:
        raise APIError(422, "empty_csv", "The CSV must contain at least one hospital.")
    if len(unique) > settings.max_hospitals:
        raise APIError(
            422,
            "too_many_hospitals",
            "The CSV contains {} unique hospitals. A maximum of {} hospitals can be processed per upload.".format(
                len(unique), settings.max_hospitals
            ),
            {
                "input_rows": input_rows,
                "unique_hospitals": len(unique),
                "max_hospitals": settings.max_hospitals,
            },
        )
    records, errors = [], []
    for row_number, values, source_rows in unique.values():
        try:
            records.append(CSVRecord(row_number, HospitalInput.model_validate(values), source_rows))
        except ValidationError as exc:
            for error in exc.errors(include_input=False, include_context=False, include_url=False):
                errors.append(
                    {
                        "row": row_number,
                        "source_rows": source_rows,
                        "field": error["loc"][0],
                        "message": error["msg"],
                    }
                )
    if errors:
        raise APIError(
            422, "csv_validation_failed", "The CSV contains invalid hospital records.", errors
        )
    return ParsedCSV(input_rows, records)
