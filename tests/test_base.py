from typing import ClassVar

from flexible_schema import Schema, SchemaValidationError


def test_schema_with_extra_cols():
    class Sample(Schema):
        allow_extra_columns: ClassVar[bool] = True
        subject_id: int
        foo: str | None = None

    sample = Sample(subject_id=1)
    assert sample.to_dict() == {"subject_id": 1}

    sample = Sample(subject_id=1, foo="bar")
    assert sample.to_dict() == {"subject_id": 1, "foo": "bar"}

    sample = Sample(subject_id=1, foo="bar", extra="extra")
    assert sample.to_dict() == {"subject_id": 1, "foo": "bar", "extra": "extra"}


def test_schema_no_extra_cols():
    class Sample(Schema):
        allow_extra_columns: ClassVar[bool] = False
        subject_id: int
        foo: str | None = None

    sample = Sample(subject_id=1)
    assert sample.to_dict() == {"subject_id": 1}

    sample = Sample(subject_id=1, foo="bar")
    assert sample.to_dict() == {"subject_id": 1, "foo": "bar"}

    try:
        sample = Sample(subject_id=1, foo="bar", extra="extra")
        raise AssertionError("Should have raised an exception")
    except SchemaValidationError as e:
        assert "Sample does not allow extra columns, but got: 'extra'" in str(e)
