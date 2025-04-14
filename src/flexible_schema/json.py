"""A simple class for flexible schema definition and usage."""

import datetime
import logging
from dataclasses import fields
from typing import Any, ClassVar, get_args, get_origin

from jsonschema import Draft202012Validator, validate
from jsonschema.exceptions import SchemaError

from .base import Schema

logger = logging.getLogger(__name__)

JSON_Schema_T = dict[str, Any]  # Type hint for [JSON Schema](https://json-schema.org/)
JSON_blob_T = dict[str, Any]  # Type hint for JSON blob


# A Schema is a generic that takes a RawDataType_T, RawSchema_T, and a RawTable_T
# JSONSchema does not support tables
class JSONSchema(Schema[Any, JSON_Schema_T, JSON_blob_T]):
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

    Once defined, you can access the schema's columns and their types via prescribed member variables:

        >>> Data.subject_id_name
        'subject_id'
        >>> Data.subject_id_dtype
        {'type': 'integer'}
        >>> Data.time_name
        'time'
        >>> Data.time_dtype
        {'type': 'string', 'format': 'date-time'}

    You can also produce a JSON schema for the class:

        >>> Data.schema() # doctest: +NORMALIZE_WHITESPACE
        {'type': 'object',
         'properties': {'subject_id': {'type': 'integer'},
                        'time': {'type': 'string', 'format': 'date-time'},
                        'code': {'type': 'string'},
                        'numeric_value': {'type': 'number'},
                        'text_value': {'type': 'string'}},
         'required': ['subject_id', 'time', 'code'],
         'additionalProperties': True}
        >>> try:
        ...     Draft202012Validator.check_schema(Data.schema())
        ...     print("Returned schema is valid!")
        ... except Exception as e:
        ...     print(f"Returned schema is invalid")
        ...     raise e
        Returned schema is valid!

    You can also validate that a query schema is valid against this schema with the `validate` method. This
    method accounts for optional column type specification and the open-ness or closed-ness of the schema
    (e.g., does it allow extra columns):

        >>> query_schema = {
        ...     "type": "object",
        ...     "properties": {
        ...         "subject_id": {"type": "integer"},
        ...         "time": {"type": "string", "format": "date-time"},
        ...         "code": {"type": "string"},
        ...         "foobar": {"type": "string"},
        ...     },
        ...     "required": ["subject_id", "time", "code"],
        ... }
        >>> try:
        ...     Data.validate(query_schema)
        ...     print("Schema is valid")
        ... except Exception as e:
        ...     print(f"Schema is invalid")
        ...     raise e
        Schema is valid
        >>> Data.allow_extra_columns = False
        >>> Data.validate(query_schema)
        Traceback (most recent call last):
            ...
        flexible_schema.exceptions.SchemaValidationError: Disallowed extra columns: foobar
        >>> query_schema = {
        ...     "type": "object",
        ...     "properties": {
        ...         "subject_id": {"type": "integer"},
        ...         "time": {"type": "string", "format": "date-time"},
        ...         "code": {"type": "string"},
        ...         "numeric_value": {"type": "string"},
        ...     },
        ... }
        >>> Data.validate(query_schema)
        Traceback (most recent call last):
            ...
        flexible_schema.exceptions.SchemaValidationError:
            Columns with incorrect types: numeric_value (want {'type': 'number'}, got {'type': 'string'})
        >>> query_schema = {
        ...     "type": "object",
        ...     "properties": {
        ...         "subject_id": {"type": "integer"},
        ...         "time": {"type": "string", "format": "date-time"},
        ...         "numeric_value": {"type": "number"},
        ...     },
        ... }
        >>> Data.validate(query_schema)
        Traceback (most recent call last):
            ...
        flexible_schema.exceptions.SchemaValidationError: Missing required columns: code

    You can also validate against a JSON blob:

        >>> Data.validate({"subject_id": 1, "time": "2023-10-01T00:00:00Z", "code": "A"})
        >>> Data.allow_extra_columns = True
        >>> Data.validate({"subject_id": 1, "time": "2023-10-01T00:00:00Z", "code": "A", "extra": "extra"})
        >>> Data.allow_extra_columns = False
        >>> Data.validate({"subject_id": 1, "time": "2023-10-01T00:00:00Z", "code": "A", "extra": "extra"})
        Traceback (most recent call last):
            ...
        flexible_schema.exceptions.TableValidationError: Table validation failed

    You can also use this class as a dataclass for type-safe usage of data conforming to this schema:

        >>> Data(subject_id=1, time=datetime.datetime(2023, 10, 1), code="A")
        Data(subject_id=1,
             time=datetime.datetime(2023, 10, 1, 0, 0),
             code='A',
             numeric_value=None,
             text_value=None)
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

    @classmethod
    def _is_raw_schema(cls, arg: Any) -> bool:
        """Check if the argument is a schema.

        Args:
            arg: The argument to check.

        Returns:
            True if the argument is a schema, False otherwise.
        """
        try:
            Draft202012Validator.check_schema(arg)
            return (
                isinstance(arg, dict)
                and (arg.get("type", None) == "object")
                and isinstance(arg.get("properties", None), dict)
            )
        except SchemaError as e:
            logger.debug(f"JSON query schema is invalid: {e}")
            return False

    @classmethod
    def _raw_schema_cols(cls, schema: JSON_Schema_T) -> list[str]:
        """Get all columns in the schema."""
        return list(schema["properties"].keys())

    @classmethod
    def _raw_schema_col_type(cls, schema: JSON_Schema_T, col: str) -> dict[str, Any]:
        """Get the type of a column in the schema."""
        return schema["properties"][col]

    @classmethod
    def _validate_table(cls, table: JSON_blob_T):
        """Validate the table against the schema."""
        validate(instance=table, schema=cls.schema())

    @classmethod
    def _raw_table_schema(cls, table: dict) -> Any:
        return {
            "type": "object",
            "properties": {k: cls._map_type_internal(type(v)) for k, v in table.items()},
        }

    @classmethod
    def _reorder_raw_table(cls, tbl: JSON_blob_T, tbl_order: list[str]) -> dict:
        return {k: tbl[k] for k in tbl_order}

    @classmethod
    def _cast_raw_table_column(cls, tbl: JSON_blob_T, col: str, col_type: Any) -> dict:
        raise NotImplementedError("This is not supported.")
