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
        >>> data = Data(subject_id=1, time=datetime.datetime(2025, 3, 7, 16), code="A", numeric_value=1.0)
        >>> data # doctest: +NORMALIZE_WHITESPACE
        Data(subject_id=1,
             time=datetime.datetime(2025, 3, 7, 16, 0),
             code='A',
             numeric_value=1.0,
             text_value=None,
             parent_codes=None)
        >>> data_tbl = pa.Table.from_pydict({
        ...     "time": [
        ...         datetime.datetime(2021, 3, 1),
        ...         datetime.datetime(2021, 4, 1),
        ...         datetime.datetime(2021, 5, 1),
        ...     ],
        ...     "subject_id": [1, 2, 3],
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
    """

    PYTHON_TO_PYARROW: ClassVar[dict[Any, pa.DataType]] = {
        int: pa.int64(),
        float: pa.float32(),
        str: pa.string(),
        bool: pa.bool_(),
        datetime.datetime: pa.timestamp("us"),
        list[str]: pa.list_(pa.string()),  # This likely won't work
    }

    @classmethod
    def _remap_type(cls, field: Any) -> pa.DataType | None:
        if field.name == "_extra_fields":
            return None

        field_type = get_args(field.type)[0] if cls._is_optional(field.type) else field.type
        return cls._remap_type_internal(field_type)

    @classmethod
    def _remap_type_internal(cls, field_type: Any) -> pa.DataType:
        origin = get_origin(field_type)

        if origin is list:
            args = get_args(field_type)
            return pa.list_(cls._remap_type_internal(args[0]))
        elif field_type in cls.PYTHON_TO_PYARROW:
            return cls.PYTHON_TO_PYARROW[field_type]
        elif isinstance(field_type, pa.DataType):
            return field_type
        else:
            raise ValueError(f"Unsupported type: {field_type}")

    @classmethod
    def validate(
        cls,
        table: pa.Table | dict[str, list[Any]],
        reorder_columns: bool = True,
        cast_types: bool = True,
    ) -> pa.Table:
        if isinstance(table, dict):
            table = pa.Table.from_pydict(table)

        table_cols = set(table.column_names)
        mandatory_cols = {f.name for f in fields(cls) if not cls._is_optional(f.type)} - {"_extra_fields"}
        all_defined_cols = {f.name for f in fields(cls)}

        missing_cols = mandatory_cols - table_cols
        if missing_cols:
            raise SchemaValidationError(f"Missing mandatory columns: {missing_cols}")

        extra_cols = table_cols - all_defined_cols
        if extra_cols and not cls.allow_extra_columns:
            raise SchemaValidationError(f"Unexpected extra columns: {extra_cols}")

        for f in fields(cls):
            if f.name == "_extra_fields":
                continue
            if f.name not in table_cols:
                length = table.num_rows
                arrow_type = cls._remap_type(f)
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
                if f.name == "_extra_fields":
                    continue
                expected_type = cls._remap_type(f)
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
