from dataclasses import fields
from typing import Any, ClassVar

from flexible_schema import Schema, SchemaValidationError


def test_schema_with_extra_cols():
    class Sample(Schema):
        allow_extra_columns: ClassVar[bool] = True
        subject_id: int
        foo: str | None = None

        @classmethod
        def _map_type_internal(cls, field_type: Any) -> Any:
            return field_type

        @classmethod
        def schema(cls):
            return {f.name: cls.map_type(f) for f in fields(cls)}

    assert Sample.schema() == {"subject_id": int, "foo": str}

    sample = Sample(subject_id=1)
    assert sample["subject_id"] == 1
    assert sample.to_dict() == {"subject_id": 1}
    assert list(sample.keys()) == ["subject_id"]
    assert list(sample.items()) == [("subject_id", 1)]
    assert list(sample) == ["subject_id"]
    assert list(sample.values()) == [1]

    assert sample == Sample(subject_id=1)
    assert sample == Sample(1)

    sample_2 = Sample(subject_id=1, foo="bar")
    assert sample != sample_2
    assert sample_2["foo"] == "bar"
    assert sample_2.to_dict() == {"subject_id": 1, "foo": "bar"}
    assert sample_2 == Sample.from_dict({"subject_id": 1, "foo": "bar"})

    sample["foo"] = "bar"
    assert sample == sample_2

    sample_3 = Sample(subject_id=1, foo="bar", extra="extra")
    assert sample_3["extra"] == "extra"
    assert sample_3.to_dict() == {"subject_id": 1, "foo": "bar", "extra": "extra"}
    assert sample_3 == Sample.from_dict(sample_3.to_dict())

    assert sample_3 == Sample(1, "bar", extra="extra")

    assert list(sample_3.keys()) == ["subject_id", "foo", "extra"]
    assert list(sample_3.items()) == [
        ("subject_id", 1),
        ("foo", "bar"),
        ("extra", "extra"),
    ]
    assert list(sample_3) == ["subject_id", "foo", "extra"]
    assert list(sample_3.values()) == [1, "bar", "extra"]

    sample["extra"] = "extra"
    assert sample == sample_3


def test_schema_no_extra_cols():
    class Sample(Schema):
        allow_extra_columns: ClassVar[bool] = False
        subject_id: int
        foo: str | None = None

        @classmethod
        def _map_type_internal(cls, field_type: Any) -> Any:
            return field_type

        @classmethod
        def schema(cls):
            return {f.name: cls.map_type(f) for f in fields(cls)}

    sample = Sample(subject_id=1)
    assert sample.to_dict() == {"subject_id": 1}

    sample_2 = Sample(subject_id=1, foo="bar")
    assert sample_2.to_dict() == {"subject_id": 1, "foo": "bar"}

    sample["foo"] = "bar"
    assert sample == sample_2

    try:
        sample = Sample(subject_id=1, foo="bar", extra="extra")
        raise AssertionError("Should have raised an exception")
    except SchemaValidationError as e:
        assert "Sample does not allow extra columns, but got: 'extra'" in str(e)

    try:
        sample["extra"] = "extra"
        raise AssertionError("Should have raised an exception")
    except SchemaValidationError as e:
        assert "Extra field not allowed: 'extra'" in str(e)


def test_errors():
    class Sample(Schema):
        allow_extra_columns: ClassVar[bool] = False
        subject_id: int
        foo: str | None = None

        @classmethod
        def _map_type_internal(cls, field_type: Any) -> Any:
            return field_type

        @classmethod
        def schema(cls):
            return {f.name: cls.map_type(f) for f in fields(cls)}

    try:
        Sample(1, 2, 3)
        raise AssertionError("Should have raised an exception")
    except TypeError as e:
        assert "Sample expected 2 arguments, got 3" in str(e)

    try:
        Sample(1, subject_id=1, foo=2)
        raise AssertionError("Should have raised an exception")
    except TypeError as e:
        assert "Sample got multiple values for argument 'subject_id'" in str(e)
