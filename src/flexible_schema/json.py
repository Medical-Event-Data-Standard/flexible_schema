"""A simple class for flexible schema definition and usage."""

import logging
from datetime import datetime
from typing import Any, ClassVar, TypedDict, TypeVar, get_args, get_origin

from jsonschema import Draft202012Validator, validate
from jsonschema.exceptions import SchemaError

from .base import Schema

logger = logging.getLogger(__name__)

JSON_Schema_T = dict[str, Any]  # Type hint for [JSON Schema](https://json-schema.org/)
JSON_blob_T = dict[str, Any]  # Type hint for JSON blob

J = TypeVar("J", bound="JSONType")


class JSONType(TypedDict, total=False):
    """A JSON schema type definition.

    This is used to define the type of a column in the JSON schema.
    """

    type: str
    format: str | None = None
    items: J | None = None


# A Schema is a generic that takes a RawDataType_T, RawSchema_T, and a RawTable_T
# JSONSchema does not support tables
class JSONSchema(Schema[JSONType, JSON_Schema_T, JSON_blob_T]):
    """A flexible mixin Schema class for easy definition of flexible, readable schemas.

    To use this class, initiate a subclass with the desired fields as dataclass fields. Fields will be
    re-mapped to PyArrow types via the `PYTHON_TO_PYARROW` dictionary. The resulting object can then be used
    to validate and reformat PyArrow tables to a validated form, or used for type-safe dictionary-like usage
    of data conforming to the schema.

    Examples:
        >>> class Data(JSONSchema):
        ...     allow_extra_columns: ClassVar[bool] = True
        ...     subject_id: int
        ...     time: datetime
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

        >>> Data.validate({"subject_id": 1, "time": "2023-10-01T00:00:00", "code": "A"})
        >>> Data.allow_extra_columns = True
        >>> Data.validate({"subject_id": 1, "time": "2023-10-01T00:00:00", "code": "A", "extra": "extra"})
        >>> Data.allow_extra_columns = False
        >>> Data.validate({"subject_id": 1, "time": "2023-10-01T00:00:00", "code": "A", "extra": "extra"})
        Traceback (most recent call last):
            ...
        flexible_schema.exceptions.TableValidationError: Table validation failed

    Validation will fail if the passed object is neither a table or a schema:

        >>> Data.validate("foobar")
        Traceback (most recent call last):
            ...
        TypeError: Expected a schema or table, but got: str

    Alignment is not supported in JSONSchema:

        >>> Data.align({"subject_id": 1, "time": "2023-10-01T00:00:00", "code": "A"})
        Traceback (most recent call last):
            ...
        NotImplementedError: JSONSchema does not support alignment

    You can also use this class as a dataclass for type-safe usage of data conforming to this schema:

        >>> Data(subject_id=1, time=datetime(2023, 10, 1), code="A")
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
    def map_type(cls, field_type: Any) -> JSONType:
        """Map a Python type to a JSON schema type.

        Args:
            field_type: The Python type to map.

        Returns:
            The JSON schema type, in string form.

        Raises:
            ValueError: If the type is not supported.

        Examples:
            >>> JSONSchema.map_type(int)
            {'type': 'integer'}
            >>> JSONSchema.map_type(list[float])
            {'type': 'array', 'items': {'type': 'number'}}
            >>> JSONSchema.map_type(str)
            {'type': 'string'}
            >>> JSONSchema.map_type(list[datetime])
            {'type': 'array', 'items': {'type': 'string', 'format': 'date-time'}}
            >>> JSONSchema.map_type("integer")
            {'type': 'integer'}
            >>> JSONSchema.map_type((int, str))
            Traceback (most recent call last):
                ...
            ValueError: Unsupported type: (<class 'int'>, <class 'str'>)
        """

        origin = get_origin(field_type)

        if origin is list:
            args = get_args(field_type)
            return {"type": "array", "items": cls.map_type(args[0])}
        elif field_type is datetime or origin is datetime:
            return {"type": "string", "format": "date-time"}
        elif field_type in cls.PYTHON_TO_JSON:
            return {"type": cls.PYTHON_TO_JSON[field_type]}
        elif isinstance(field_type, str):
            return {"type": field_type}
        else:
            raise ValueError(f"Unsupported type: {field_type}")

    @classmethod
    def _inv_map_type(cls, json_type: JSONType) -> Any:
        """Inverse map a JSON schema type to a Python type.

        Args:
            json_type: The JSON schema type to map.

        Returns:
            The Python type.

        Raises:
            ValueError: If the type is not supported.

        Examples:
            >>> JSONSchema._inv_map_type({"type": "integer"})
            <class 'int'>
            >>> JSONSchema._inv_map_type({"type": "string"})
            <class 'str'>
            >>> JSONSchema._inv_map_type({"type": "number"})
            <class 'float'>
            >>> JSONSchema._inv_map_type({"type": "array", "items": {"type": "integer"}})
            list[int]
            >>> JSONSchema._inv_map_type({"type": "string", "format": "date-time"})
            <class 'datetime.datetime'>
            >>> JSONSchema._inv_map_type({"type": "object"})
            Traceback (most recent call last):
                ...
            ValueError: Unsupported type: {'type': 'object'}
        """

        if json_type["type"] == "array":
            return list[cls._inv_map_type(json_type["items"])]
        elif json_type["type"] == "string" and json_type.get("format") == "date-time":
            return datetime
        elif json_type["type"] in cls.PYTHON_TO_JSON.values():
            return {v: k for k, v in cls.PYTHON_TO_JSON.items()}[json_type["type"]]
        else:
            raise ValueError(f"Unsupported type: {json_type}")

    @classmethod
    def schema(cls) -> dict[str, Any]:
        schema_properties = {}
        required_fields = []

        for c in cls._columns():
            schema_properties[c.name] = c.dtype

            if c.is_required:
                required_fields.append(c.name)

        schema = {
            "type": "object",
            "properties": schema_properties,
            "required": required_fields,
            "additionalProperties": cls.allow_extra_columns,
        }

        return schema

    @classmethod
    def _is_raw_table(cls, arg: Any) -> bool:
        """Check if the argument is a raw table (e.g., of type `RawTable_T`).

        Args:
            arg: The argument to check.

        Returns:
            True if the argument is a table, False otherwise.

        Examples:
            >>> JSONSchema._is_raw_table({"subject_id": 1, "time": "2023-10-01T00:00:00Z", "code": "A"})
            True
            >>> JSONSchema._is_raw_table({"subject_id": 1, "time": datetime(2012, 12, 1), "code": 1})
            True
            >>> JSONSchema._is_raw_table("foobar")
            False
            >>> JSONSchema._is_raw_table({1: 2, 3: 4})
            False
        """

        return not (not isinstance(arg, dict) or not all(isinstance(k, str) for k in arg))

    @classmethod
    def _is_raw_schema(cls, arg: Any) -> bool:
        """Check if the argument is a schema.

        Args:
            arg: The argument to check.

        Returns:
            True if the argument is a schema, False otherwise.

        Examples:
            >>> JSONSchema._is_raw_schema(
            ...     {"type": "object", "properties": {"subject_id": {"type": "integer"}}}
            ... )
            True
            >>> JSONSchema._is_raw_schema({"subject_id": 1})
            False
            >>> JSONSchema._is_raw_schema({"type": "object"})
            False
            >>> JSONSchema._is_raw_schema({"properties": {}})
            False
            >>> JSONSchema._is_raw_schema({"type": "str", "properties": {}})
            False
            >>> JSONSchema._is_raw_schema({"type": "object", "properties": []})
            False
            >>> JSONSchema._is_raw_schema({"type": "object", "properties": {}})
            True
            >>> JSONSchema._is_raw_schema("foobar")
            False
            >>> JSONSchema._is_raw_schema({1: 2, 3: 4})
            False
            >>> JSONSchema._is_raw_schema({"type": "object", "properties": {}, "title": 33})
            False
        """

        if (
            not isinstance(arg, dict)
            or ("type" not in arg)
            or ("properties" not in arg)
            or arg["type"] != "object"
            or not isinstance(arg.get("properties", None), dict)
        ):
            return False

        try:
            Draft202012Validator.check_schema(arg)
            return True
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
    def _raw_table_schema(cls, table: dict) -> Any:  # pragma: no cover
        raise NotImplementedError("JSONSchema does not support _raw_table_schema")

    @classmethod
    def _reorder_raw_table(cls, table: JSON_blob_T, table_order: list[str]) -> JSON_blob_T:
        """Reorder the columns of a "table" (JSON blob) to a target list.

        Args:
            table: The JSON blob to reorder.
            table_order: The order to set the columns in.

        Returns:
            The reordered JSON blob.

        Examples:
            >>> JSONSchema._reorder_raw_table({"foo": 1, "bar": 2}, ["bar", "foo"])
            {'bar': 2, 'foo': 1}
        """
        return {k: table[k] for k in table_order}

    @classmethod
    def _cast_raw_table_column(cls, table: JSON_blob_T, col: str, col_type: JSONType) -> JSON_blob_T:
        """Cast a column in the "table" (JSON blob) to the specified type.

        Args:
            table: The JSON blob to cast.
            col: The column to cast.
            col_type: The type to cast the column to.

        Returns:
            The JSON blob with the casted column.

        Examples:
            >>> JSONSchema._cast_raw_table_column({"foo": 1, "bar": 2}, "foo", {"type": "string"})
            {'foo': '1', 'bar': 2}
            >>> JSONSchema._cast_raw_table_column(
            ...     {"foo": 1, "bar": "1234"}, "bar", {"type": "array", "items": {"type": "integer"}}
            ... )
            {'foo': 1, 'bar': [1, 2, 3, 4]}
            >>> JSONSchema._cast_raw_table_column(
            ...     {"foo": "2023-10-01T00:00:00"}, "foo", {"type": "string", "format": "date-time"}
            ... )
            {'foo': datetime.datetime(2023, 10, 1, 0, 0)}
            >>> JSONSchema._cast_raw_table_column(
            ...     {"foo": 1, "bar": "1234"}, "foo", {"type": "array", "items": {"type": "integer"}}
            ... )
            Traceback (most recent call last):
                ...
            ValueError: Column foo can't be casted to {'type': 'array', 'items': {'type': 'integer'}}: 1
        """
        out = {**table}
        try:
            out[col] = cls.__cast_raw_val(table[col], col_type)
        except Exception as e:
            raise ValueError(f"Column {col} can't be casted to {col_type}: {table[col]}") from e
        return out

    @classmethod
    def __cast_raw_val(cls, in_val: Any, col_type: JSONType) -> Any:
        inv_type = cls._inv_map_type(col_type)

        if inv_type is datetime:
            return datetime.fromisoformat(in_val)
        elif col_type["type"] == "array":
            return [cls.__cast_raw_val(v, col_type["items"]) for v in in_val]
        else:
            return inv_type(in_val)

    @classmethod
    def align(cls, table: JSON_blob_T) -> JSON_blob_T:
        raise NotImplementedError("JSONSchema does not support alignment")

    @classmethod
    def _any_null(cls, table: JSON_blob_T, col: str) -> bool:
        """Checks if any value in the table at the given column is None.

        This isn't used in JSON, but we keep them to match the interface.

        Examples:
            >>> class Sample(JSONSchema):
            ...     subject_id: int
            >>> Sample._any_null({"subject_id": 1}, "subject_id")
            False
            >>> Sample._any_null({"subject_id": None}, "subject_id")
            True
            >>> Sample._any_null({}, "subject_id")
            True
        """
        return table.get(col, None) is None

    _all_null = _any_null
