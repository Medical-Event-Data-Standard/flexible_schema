"""A simple class for flexible schema definition and usage."""

import datetime
from dataclasses import fields
from typing import Any, ClassVar, get_args, get_origin

from .base import Schema


class JSONSchema(Schema[Any, dict[str, Any]]):
    """A flexible mixin Schema class for easy definition of flexible, readable schemas.

    To use this class, initiate a subclass with the desired fields as dataclass fields. Fields will be
    re-mapped to PyArrow types via the `PYTHON_TO_PYARROW` dictionary. The resulting object can then be used
    to validate and reformat PyArrow tables to a validated form, or used for type-safe dictionary-like usage
    of data conforming to the schema.

    Examples:

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
        {'type': 'integer'}
        >>> Data.time_name
        'time'
        >>> Data.time_dtype
        {'type': 'string', 'format': 'date-time'}
        >>> Data.schema() # doctest: +NORMALIZE_WHITESPACE
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
    }

    @classmethod
    def _map_type_internal(cls, field_type: Any) -> str:
        """Map a Python type to a JSON schema type.

        Args:
            field_type: The Python type to map.

        Returns:
            The JSON schema type, in string form.

        Raises:
            ValueError: If the type is not supported.

        Examples:
            >>> JSONSchema._map_type_internal(int)
            {'type': 'integer'}
            >>> JSONSchema._map_type_internal(list[float])
            {'type': 'array', 'items': {'type': 'number'}}
            >>> JSONSchema._map_type_internal(str)
            {'type': 'string'}
            >>> JSONSchema._map_type_internal(list[datetime.datetime])
            {'type': 'array', 'items': {'type': 'string', 'format': 'date-time'}}
            >>> JSONSchema._map_type_internal("integer")
            {'type': 'integer'}
            >>> JSONSchema._map_type_internal((int, str))
            Traceback (most recent call last):
                ...
            ValueError: Unsupported type: (<class 'int'>, <class 'str'>)
        """

        origin = get_origin(field_type)

        if origin is list:
            args = get_args(field_type)
            return {"type": "array", "items": cls._map_type_internal(args[0])}
        elif field_type is datetime.datetime or origin is datetime.datetime:
            return {"type": "string", "format": "date-time"}
        elif field_type in cls.PYTHON_TO_JSON:
            return {"type": cls.PYTHON_TO_JSON[field_type]}
        elif isinstance(field_type, str):
            return {"type": field_type}
        else:
            raise ValueError(f"Unsupported type: {field_type}")

    @classmethod
    def schema(cls) -> dict[str, Any]:
        schema_properties = {}
        required_fields = []

        for f in fields(cls):
            schema_properties[f.name] = cls.map_type(f)

            if not cls._is_optional(f.type):
                required_fields.append(f.name)

        schema = {
            "type": "object",
            "properties": schema_properties,
            "required": required_fields,
            "additionalProperties": cls.allow_extra_columns,
        }

        return schema
