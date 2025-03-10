"""A simple class for flexible schema definition and usage."""

import datetime
from dataclasses import fields
from typing import Any, ClassVar, get_args, get_origin

import pyarrow as pa

from .base import Schema, SchemaValidationError


class PyArrowSchema(Schema):
    """A PyArrow-based schema class for flexible schema definition and usage.

    To use this class, initiate a subclass with the desired fields as dataclass fields. Fields will be
    re-mapped to PyArrow types via the `PYTHON_TO_PYARROW` dictionary. The resulting object can then be used
    to validate and reformat PyArrow tables to a validated form, or used for type-safe dictionary-like usage
    of data conforming to the schema.

    Example usage:
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
        >>> Data.validate(data_tbl)
        pyarrow.Table
        subject_id: int64
        time: timestamp[us]
        code: string
        numeric_value: float
        text_value: string
        parent_codes: list<item: string>
          child 0, item: string
        ----
        subject_id: [[1,2,3]]
        time: [[2021-03-01 00:00:00.000000,2021-04-01 00:00:00.000000,2021-05-01 00:00:00.000000]]
        code: [["A","B","C"]]
        numeric_value: [[null,null,null]]
        text_value: [[null,null,null]]
        parent_codes: [[null,null,null]]

        Including casting and reordering columns:

        >>> data_tbl = pa.Table.from_pydict({
        ...     "time": [
        ...         datetime.datetime(2021, 3, 1),
        ...         datetime.datetime(2021, 4, 1),
        ...         datetime.datetime(2021, 5, 1),
        ...     ],
        ...     "subject_id": [1, 2, 3],
        ...     "code": ["A", "B", "C"],
        ... }, schema=pa.schema(
        ...     [
        ...         pa.field("time", pa.timestamp("us")),
        ...         pa.field("subject_id", pa.int32()),
        ...         pa.field("code", pa.string()),
        ...     ]
        ... ))
        >>> Data.validate(data_tbl)
        pyarrow.Table
        subject_id: int64
        time: timestamp[us]
        code: string
        numeric_value: float
        text_value: string
        parent_codes: list<item: string>
          child 0, item: string
        ----
        subject_id: [[1,2,3]]
        time: [[2021-03-01 00:00:00.000000,2021-04-01 00:00:00.000000,2021-05-01 00:00:00.000000]]
        code: [["A","B","C"]]
        numeric_value: [[null,null,null]]
        text_value: [[null,null,null]]
        parent_codes: [[null,null,null]]

        And handling extra columns:

        >>> data_tbl_with_extra = pa.Table.from_pydict({
        ...     "time": [
        ...         datetime.datetime(2021, 3, 1),
        ...         datetime.datetime(2021, 4, 1),
        ...     ],
        ...     "subject_id": [4, 5],
        ...     "extra_1": ["extra1", "extra2"],
        ...     "extra_2": [452, 11],
        ...     "code": ["D", "E"],
        ... })
        >>> Data.validate(data_tbl_with_extra)
        pyarrow.Table
        subject_id: int64
        time: timestamp[us]
        code: string
        numeric_value: float
        text_value: string
        parent_codes: list<item: string>
          child 0, item: string
        extra_1: string
        extra_2: int64
        ----
        subject_id: [[4,5]]
        time: [[2021-03-01 00:00:00.000000,2021-04-01 00:00:00.000000]]
        code: [["D","E"]]
        numeric_value: [[null,null]]
        text_value: [[null,null]]
        parent_codes: [[null,null]]
        extra_1: [["extra1","extra2"]]
        extra_2: [[452,11]]

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
        >>> Data.validate(pa.Table.from_pydict({"subject_id": [4, 5], "code": ["D", "E"]}))
        pyarrow.Table
        subject_id: int64
        code: string
        numeric_value: float
        ----
        subject_id: [[4,5]]
        code: [["D","E"]]
        numeric_value: [[null,null]]

        Errors will be raised when extra columns are present inapproriately or mandatory columns are missing:

        >>> data_tbl_with_extra = pa.Table.from_pydict({
        ...     "subject_id": [4, 5],
        ...     "code": ["D", "E"],
        ...     "extra_1": ["extra1", "extra2"],
        ... })
        >>> Data.validate(data_tbl_with_extra)
        Traceback (most recent call last):
            ...
        flexible_schema.base.SchemaValidationError: Unexpected extra columns: {'extra_1'}
        >>> Data.validate(pa.Table.from_pydict({ "subject_id": [4, 5], }))
        Traceback (most recent call last):
            ...
        flexible_schema.base.SchemaValidationError: Missing mandatory columns: {'code'}

        Or when columns can't be cast properly:

        >>> Data.validate(pa.Table.from_pydict({"subject_id": ["A", "B"], "code": ["D", "E"]}))
        Traceback (most recent call last):
            ...
        flexible_schema.base.SchemaValidationError: Column 'subject_id' cast failed: ...

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
        >>> data # doctest: +NORMALIZE_WHITESPACE
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
    def _map_type_internal(cls, field_type: Any) -> pa.DataType:
        origin = get_origin(field_type)

        if origin is list:
            args = get_args(field_type)
            return pa.list_(cls._map_type_internal(args[0]))
        elif field_type in cls.PYTHON_TO_PYARROW:
            return cls.PYTHON_TO_PYARROW[field_type]
        elif isinstance(field_type, pa.DataType):
            return field_type
        else:
            raise ValueError(f"Unsupported type: {field_type}")

    @classmethod
    def schema(cls) -> pa.Schema:
        return pa.schema([(f.name, cls.map_type(f)) for f in fields(cls)])

    @classmethod
    def validate(
        cls,
        table: pa.Table,
        reorder_columns: bool = True,
        cast_types: bool = True,
    ) -> pa.Table:
        table_cols = set(table.column_names)
        mandatory_cols = {f.name for f in fields(cls) if not cls._is_optional(f.type)}
        all_defined_cols = {f.name for f in fields(cls)}

        missing_cols = mandatory_cols - table_cols
        if missing_cols:
            raise SchemaValidationError(f"Missing mandatory columns: {missing_cols}")

        extra_cols = table_cols - all_defined_cols
        if extra_cols and not cls.allow_extra_columns:
            raise SchemaValidationError(f"Unexpected extra columns: {extra_cols}")

        for f in fields(cls):
            if f.name not in table_cols:
                length = table.num_rows
                arrow_type = cls.map_type(f)
                table = table.append_column(f.name, pa.array([None] * length, type=arrow_type))

        # Reorder columns
        if reorder_columns:
            ordered_cols = [f.name for f in fields(cls) if f.name in table.column_names]
            if cls.allow_extra_columns:
                ordered_cols += [c for c in table.column_names if c not in ordered_cols]
            table = table.select(ordered_cols)

        # Cast columns if needed
        if cast_types:
            for f in fields(cls):
                expected_type = cls.map_type(f)
                current_type = table.schema.field(f.name).type
                if current_type != expected_type:
                    try:
                        table = table.set_column(
                            table.schema.get_field_index(f.name),
                            f.name,
                            table.column(f.name).cast(expected_type),
                        )
                    except pa.ArrowInvalid as e:
                        raise SchemaValidationError(f"Column '{f.name}' cast failed: {e}")

        return table
