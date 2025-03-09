"""A simple class for flexible schema definition and usage."""

import datetime
from dataclasses import fields
from typing import Any, ClassVar, get_args

from .base import Schema


class JSONSchema(Schema):
    """A flexible mixin Schema class for easy definition of flexible, readable schemas.

    To use this class, initiate a subclass with the desired fields as dataclass fields. Fields will be
    re-mapped to PyArrow types via the `PYTHON_TO_PYARROW` dictionary. The resulting object can then be used
    to validate and reformat PyArrow tables to a validated form, or used for type-safe dictionary-like usage
    of data conforming to the schema.

    Example usage:
        >>> class Data(JSONSchema):
        ...     allow_extra_columns: ClassVar[bool] = True
        ...     subject_id: int
        ...     time: datetime.datetime
        ...     code: str
        ...     numeric_value: float | None = None
        ...     text_value: str | None = None
        >>> Data.subject_id_name
        'subject_id'
        >>> Data.subject_id_dtype
        'integer'
        >>> Data.time_name
        'time'
        >>> Data.time_dtype
        'string'
        >>> Data.to_json_schema() # doctest: +NORMALIZE_WHITESPACE
        {'type': 'object',
         'properties': {'subject_id': {'type': 'integer'},
                        'time': {'type': 'string', 'format': 'date-time'},
                        'code': {'type': 'string'},
                        'numeric_value': {'type': 'number'},
                        'text_value': {'type': 'string'}},
         'required': ['subject_id', 'time', 'code'],
         'additionalProperties': True}
    """

    PYTHON_TO_JSON: ClassVar[dict[Any, str]] = {
        int: "integer",
        float: "number",
        str: "string",
        bool: "boolean",
        datetime.datetime: "string",  # datetime as ISO8601 string
    }

    @classmethod
    def _remap_type(cls, field: Any) -> Any:
        field_type = get_args(field.type)[0] if cls._is_optional(field.type) else field.type
        return cls.PYTHON_TO_JSON.get(field_type, "string")

    @classmethod
    def to_json_schema(cls) -> dict[str, Any]:
        schema_properties = {}
        required_fields = []

        for f in fields(cls):
            json_type = cls._remap_type(f)
            base_type = get_args(f.type)[0] if cls._is_optional(f.type) else f.type

            property_schema = {"type": json_type}

            # Special handling for datetime
            if base_type is datetime.datetime:
                property_schema["format"] = "date-time"

            schema_properties[f.name] = property_schema

            if not cls._is_optional(f.type):
                required_fields.append(f.name)

        schema = {
            "type": "object",
            "properties": schema_properties,
            "required": required_fields,
            "additionalProperties": cls.allow_extra_columns,
        }

        return schema
