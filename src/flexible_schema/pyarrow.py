"""A simple class for flexible schema definition and usage."""

import datetime
from typing import Any, ClassVar, get_args, get_origin

import pyarrow as pa

from .base import Schema


# A Schema is a generic that takes a RawDataType_T, RawSchema_T, and a RawTable_T
class PyArrowSchema(Schema[pa.DataType | pa.Field, pa.Schema, pa.Table]):
    """A PyArrow-based schema class for flexible schema definition and usage.

    To use this class, initiate a subclass with the desired fields as dataclass fields. Fields will be
    re-mapped to PyArrow types via the `PYTHON_TO_PYARROW` dictionary. The resulting object can then be used
    to validate and reformat PyArrow tables to a validated form, or used for type-safe dictionary-like usage
    of data conforming to the schema.

    Examples:
        >>> class Data(PyArrowSchema):
        ...     allow_extra_columns: ClassVar[bool] = True
        ...     subject_id: int
        ...     time: datetime.datetime
        ...     code: str
        ...     numeric_value: float | None = None
        ...     text_value: str | None = None
        ...     parent_codes: list[str] | None = None
        >>> Data.subject_id_name
        'subject_id'
        >>> Data.subject_id_dtype
        DataType(int64)
        >>> Data.time_name
        'time'
        >>> Data.time_dtype
        TimestampType(timestamp[us])
        >>> Data.parent_codes_name
        'parent_codes'
        >>> Data.parent_codes_dtype
        ListType(list<item: string>)

    You can get the direct schema:

        >>> Data.schema() # doctest: +NORMALIZE_WHITESPACE
        subject_id: int64
        time: timestamp[us]
        code: string
        numeric_value: float
        text_value: string
        parent_codes: list<item: string>
          child 0, item: string

    You can also validate that a query schema is valid against this schema with the `validate` method. This
    method accounts for optional column type specification and the open-ness or closed-ness of the schema
    (e.g., does it allow extra columns):

        >>> query_schema = pa.schema([
        ...     pa.field("subject_id", pa.int64()), pa.field("time", pa.timestamp("us")),
        ...     pa.field("code", pa.string()), pa.field("numeric_value", pa.float32()),
        ...     pa.field("extra", pa.string()),
        ... ])
        >>> Data.validate(query_schema) # No issues
        >>> Data.allow_extra_columns = False
        >>> Data.validate(query_schema)
        Traceback (most recent call last):
            ...
        flexible_schema.exceptions.SchemaValidationError: Disallowed extra columns: extra

    You can also validate tables with this class

        >>> data_tbl = pa.Table.from_pydict({
        ...     "subject_id": [1, 2, 3],
        ...     "time": [
        ...         datetime.datetime(2021, 3, 1),
        ...         datetime.datetime(2021, 4, 1),
        ...         datetime.datetime(2021, 5, 1),
        ...     ],
        ...     "code": ["A", "B", "C"],
        ... })
        >>> Data.validate(data_tbl) # No issues
        >>> data_tbl = pa.Table.from_pydict({
        ...     "subject_id": ["1", "2", "3"],
        ...     "time": [
        ...         datetime.datetime(2021, 3, 1),
        ...         datetime.datetime(2021, 4, 1),
        ...         datetime.datetime(2021, 5, 1),
        ...     ],
        ...     "code": ["A", "B", "C"],
        ...     "text_value": [1, 2, 3],
        ... })
        >>> Data.validate(data_tbl)
        Traceback (most recent call last):
            ...
        flexible_schema.exceptions.SchemaValidationError:
            Columns with incorrect types: subject_id (want int64, got string),
                                          text_value (want string, got int64)

    Beyond validation of tables (which either raises an error or returns nothing), you can also _align_ tables
    with this class, which performs safe, no-data-change operations to convert an input table into a format
    that is fully compliant with the schema. These changes include re-ordering of columns and casting, when it
    can be done safely:

        >>> Data.allow_extra_columns = True
        >>> data_tbl = pa.Table.from_pydict({
        ...     "time": [
        ...         datetime.datetime(2021, 3, 1),
        ...         datetime.datetime(2021, 4, 1),
        ...         datetime.datetime(2021, 5, 1),
        ...     ],
        ...     "subject_id": [1, 2, 3],
        ...     "extra_col": ["extra1", "extra2", "extra3"],
        ...     "code": ["A", "B", "C"],
        ... }, schema=pa.schema(
        ...     [
        ...         pa.field("time", pa.timestamp("us")),
        ...         pa.field("subject_id", pa.int32()),
        ...         pa.field("extra_col", pa.string()),
        ...         pa.field("code", pa.string()),
        ...     ]
        ... ))
        >>> Data.align(data_tbl)
        pyarrow.Table
        subject_id: int64
        time: timestamp[us]
        code: string
        extra_col: string
        ----
        subject_id: [[1,2,3]]
        time: [[2021-03-01 00:00:00.000000,2021-04-01 00:00:00.000000,2021-05-01 00:00:00.000000]]
        code: [["A","B","C"]]
        extra_col: [["extra1","extra2","extra3"]]

    Alignment also raises errors when the table cannot be aligned to the target schema

        >>> Data.allow_extra_columns = False
        >>> Data.align(data_tbl)
        Traceback (most recent call last):
            ...
        flexible_schema.exceptions.SchemaValidationError:
            Disallowed extra columns: extra_col
        >>> data_tbl = pa.Table.from_pydict({
        ...     "time": [
        ...         datetime.datetime(2021, 3, 1),
        ...         datetime.datetime(2021, 4, 1),
        ...         datetime.datetime(2021, 5, 1),
        ...     ],
        ...     "subject_id": ["foo", "bar", "baz"],
        ...     "code": ["A", "B", "C"],
        ... })
        >>> Data.align(data_tbl)
        Traceback (most recent call last):
            ...
        flexible_schema.exceptions.SchemaValidationError:
            Columns with incorrect types: subject_id (want int64, got string)

    You can also specify type hints directly using PyArrow types:

        >>> from flexible_schema import Optional
        >>> class Data(PyArrowSchema):
        ...     allow_extra_columns: ClassVar[bool] = False
        ...     subject_id: pa.int64()
        ...     code: str
        ...     numeric_value: Optional(pa.float32()) = None
        >>> Data.subject_id_dtype
        DataType(int64)
        >>> Data.code_dtype
        DataType(string)
        >>> Data.numeric_value_dtype
        DataType(float)
        >>> Data.align(pa.Table.from_pydict({"subject_id": [4, 5], "code": ["D", "E"]}))
        pyarrow.Table
        subject_id: int64
        code: string
        ----
        subject_id: [[4,5]]
        code: [["D","E"]]

    Not all types are supported

        >>> class Data(PyArrowSchema):
        ...     foo: dict[str, str]
        Traceback (most recent call last):
            ...
        ValueError: Unsupported type: dict[str, str]

    Even though this is a PyArrow-based schema, you can still use it as a dataclass:

        >>> class Data(PyArrowSchema):
        ...     allow_extra_columns: ClassVar[bool] = True
        ...     subject_id: int
        ...     time: datetime.datetime
        ...     code: str
        ...     numeric_value: float | None = None
        ...     text_value: str | None = None
        ...     parent_codes: list[str] | None = None
        >>> data = Data(subject_id=1, time=datetime.datetime(2025, 3, 7, 16), code="A", numeric_value=1.0)
        >>> data
        Data(subject_id=1,
             time=datetime.datetime(2025, 3, 7, 16, 0),
             code='A',
             numeric_value=1.0,
             text_value=None,
             parent_codes=None)
    """

    PYTHON_TO_PYARROW: ClassVar[dict[Any, pa.DataType]] = {
        int: pa.int64(),
        float: pa.float32(),
        str: pa.string(),
        bool: pa.bool_(),
        datetime.datetime: pa.timestamp("us"),
    }

    @classmethod
    def map_type(cls, field_type: Any) -> pa.DataType:
        origin = get_origin(field_type)

        if origin is list:
            args = get_args(field_type)
            return pa.list_(cls.map_type(args[0]))
        elif field_type in cls.PYTHON_TO_PYARROW:
            return cls.PYTHON_TO_PYARROW[field_type]
        elif isinstance(field_type, pa.DataType):
            return field_type
        else:
            raise ValueError(f"Unsupported type: {field_type}")

    @classmethod
    def schema(cls) -> pa.Schema:
        return pa.schema([(c.name, c.dtype) for c in cls._columns()])

    @classmethod
    def _raw_schema_col_type(cls, schema: pa.Schema, col: str) -> pa.DataType:
        return schema.field(col).type

    @classmethod
    def _raw_schema_cols(cls, schema: pa.Schema) -> list[str]:
        return schema.names

    @classmethod
    def _raw_table_schema(cls, table: pa.Table) -> pa.Schema:
        return table.schema

    @classmethod
    def _reorder_raw_table(cls, tbl: pa.Table, tbl_order: list[str]) -> pa.Table:
        return tbl.select(tbl_order)

    @classmethod
    def _cast_raw_table_column(cls, tbl: pa.Table, col: str, want_type: pa.DataType) -> pa.Table:
        return tbl.set_column(tbl.schema.get_field_index(col), col, tbl.column(col).cast(want_type))
