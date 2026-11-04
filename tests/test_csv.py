import pytest

from app.csv_upload import parse_csv
from app.errors import APIError
from tests.conftest import csv_bytes


@pytest.mark.parametrize(
    "headers", ["name,address,phone", "NAME,ADDRESS,PHONE", " Name , aDdReSs , pHoNe "]
)
def test_headers_normalized_values_preserved(headers, settings):
    parsed = parse_csv((headers + "\nGeneral Hospital, Main Road ,00123\n").encode(), settings)
    assert parsed.records[0].hospital.model_dump() == {
        "name": "General Hospital",
        "address": "Main Road",
        "phone": "00123",
    }


def test_projection_before_deduplication(settings):
    contents = (
        b"notes,Phone,NAME,active,ADDRESS,id,notes,\nx,001,A,true,B,9,z,q\ny,001,A,false,B,2,v,k\n"
    )
    parsed = parse_csv(contents, settings)
    assert len(parsed.records) == 1
    assert parsed.summary().model_dump() == {
        "valid": True,
        "input_rows": 2,
        "unique_hospitals": 1,
        "duplicates_removed": 1,
        "duplicate_rows": {"1": [2]},
    }
    assert parsed.records[0].hospital.model_dump() == {"name": "A", "address": "B", "phone": "001"}


def test_optional_phone_surplus_and_missing_cells(settings):
    parsed = parse_csv(b"NAME,ADDRESS,PHONE,ignored\nA,B\nA,B,,x,surplus\nC,D,  ,x\n", settings)
    assert len(parsed.records) == 2
    assert parsed.records[0].source_rows == [1, 2]
    assert all(record.hospital.phone is None for record in parsed.records)


def test_duplicate_key_includes_address_phone_and_case(settings):
    parsed = parse_csv(b"name,address,phone\nA,B,001\nA,C,001\nA,B,002\na,B,001\n", settings)
    assert len(parsed.records) == 4


def test_bom_multiline_unicode_and_empty_lines(settings):
    parsed = parse_csv(
        '\ufeffname,address\r\n"Hôpital, Main","First\nSecond"\r\n\r\nCity,Road\r\n'.encode(),
        settings,
    )
    assert parsed.records[0].hospital.name == "Hôpital, Main"
    assert parsed.records[0].hospital.address == "First\nSecond"
    assert parsed.records[1].row == 3
    assert parsed.input_rows == 2


def test_twenty_unique_and_more_than_twenty_original(settings):
    parsed = parse_csv(csv_bytes(20) + b"Hospital 0,Address 0,0010\n" * 10, settings)
    assert len(parsed.records) == 20
    assert parsed.input_rows == 30


def test_twenty_one_unique_has_clear_error(settings):
    with pytest.raises(APIError) as caught:
        parse_csv(csv_bytes(21), settings)
    assert caught.value.status == 422
    assert caught.value.code == "too_many_hospitals"
    assert "21 unique hospitals" in caught.value.message
    assert caught.value.details == {"input_rows": 21, "unique_hospitals": 21, "max_hospitals": 20}


@pytest.mark.parametrize(
    "contents,status,code",
    [
        (b"", 422, "empty_csv"),
        (b"\n\n", 422, "empty_csv"),
        (b"name,address\n", 422, "empty_csv"),
        (b"name,phone\nA,1\n", 422, "missing_headers"),
        (b"name,NAME,address\nA,A,B\n", 422, "duplicate_header"),
        (b"name,address,phone,PHONE\nA,B,1,1\n", 422, "duplicate_header"),
        (b"name,address\nA,\n", 422, "csv_validation_failed"),
        (b"name,address\nA\n", 422, "csv_validation_failed"),
        (b"name,address\n , B\n", 422, "csv_validation_failed"),
        (b'name,address\n"unterminated,B', 400, "malformed_csv"),
        (b"name,address\n\xff,B", 400, "invalid_encoding"),
        (b"name,address\nA,B\x00", 400, "malformed_csv"),
    ],
)
def test_invalid_inputs(contents, status, code, settings):
    with pytest.raises(APIError) as caught:
        parse_csv(contents, settings)
    assert (caught.value.status, caught.value.code) == (status, code)


def test_invalid_duplicates_report_source_rows(settings):
    with pytest.raises(APIError) as caught:
        parse_csv(b"name,address,ignored\nA,,x\nA,,y\n", settings)
    assert caught.value.details[0]["source_rows"] == [1, 2]


def test_unparseable_ignored_column_still_requires_valid_csv(settings):
    # Python 3.8's CSV parser rejects NUL before columns can be projected.
    with pytest.raises(APIError) as caught:
        parse_csv(b"name,address,notes\nA,B,\x00\n", settings)
    assert caught.value.status == 400


def test_file_byte_limit(settings):
    config = settings.model_copy(update={"max_file_bytes": 10})
    with pytest.raises(APIError) as caught:
        parse_csv(csv_bytes(), config)
    assert caught.value.status == 413


def test_fingerprint_ignores_extras_duplicates_and_header_case(settings):
    one = parse_csv(b"Name,Address,notes\nA,B,x\nA,B,y\n", settings)
    two = parse_csv(b"ADDRESS,NAME\nB,A\n", settings)
    assert one.fingerprint() == two.fingerprint()
