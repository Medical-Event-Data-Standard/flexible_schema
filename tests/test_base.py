from typing import ClassVar

from flexible_schema import Schema, SchemaValidationError


def test_schema_with_extra_cols():
    class Sample(Schema):
        allow_extra_columns: ClassVar[bool] = True
        subject_id: int
        foo: str | None = None

    sample = Sample(subject_id=1)
    assert sample["subject_id"] == 1
    assert sample.to_dict() == {"subject_id": 1}
    assert list(sample.keys()) == ["subject_id"]
    assert list(sample.items()) == [("subject_id", 1)]
    assert list(sample) == ["subject_id"]
    assert list(sample.values()) == [1]

    sample_2 = Sample(subject_id=1, foo="bar")
    assert sample != sample_2
    assert sample_2["foo"] == "bar"
    assert sample_2.to_dict() == {"subject_id": 1, "foo": "bar"}

    sample["foo"] = "bar"
    assert sample == sample_2

    sample_3 = Sample(subject_id=1, foo="bar", extra="extra")
    assert sample_3["extra"] == "extra"
    assert sample_3.to_dict() == {"subject_id": 1, "foo": "bar", "extra": "extra"}

    assert list(sample_3.keys()) == ["subject_id", "foo", "extra"]
    assert list(sample_3.items()) == [("subject_id", 1), ("foo", "bar"), ("extra", "extra")]
    assert list(sample_3) == ["subject_id", "foo", "extra"]
    assert list(sample_3.values()) == [1, "bar", "extra"]

    sample["extra"] = "extra"
    assert sample == sample_3


def test_schema_no_extra_cols():
    class Sample(Schema):
        allow_extra_columns: ClassVar[bool] = False
        subject_id: int
        foo: str | None = None

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
