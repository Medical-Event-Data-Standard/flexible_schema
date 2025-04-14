"""A Meta-class for defining Schemas that can be created like dataclasses and used to validate tables."""

import types
from abc import ABCMeta, abstractmethod
from dataclasses import Field, dataclass, fields
from typing import Any, ClassVar, Generic, TypeVar, Union, get_args, get_origin

from .exceptions import SchemaValidationError, TableValidationError
from .fields import ColumnDType, Optional

RawDataType_T = TypeVar("RawDataType_T")
RawSchema_T = TypeVar("RawSchema_T")
RawTable_T = TypeVar("RawTable_T")


class SchemaMeta(ABCMeta):
    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        cls = dataclass(cls)  # explicitly turn cls into a dataclass here
        # Add constants after dataclass is fully initialized

        field_names = []
        for f in fields(cls):
            field_names.append(f.name)
            setattr(cls, f"{f.name}_name", f.name)
            remapped_type = cls.map_type(f)
            setattr(cls, f"{f.name}_dtype", remapped_type)

        old_init = cls.__init__

        def new_init(self, *args, **kwargs):
            if len(args) > len(field_names):
                raise TypeError(f"{cls.__name__} expected {len(field_names)} arguments, got {len(args)}")

            out_kwargs = {}
            for i, arg in enumerate(args):
                out_kwargs[field_names[i]] = arg

            for k, v in kwargs.items():
                if k in out_kwargs:
                    raise TypeError(f"{cls.__name__} got multiple values for argument '{k}'")
                out_kwargs[k] = v

            to_pass = {k: v for k, v in out_kwargs.items() if k in field_names}
            extra = {k: v for k, v in out_kwargs.items() if k not in field_names}

            if not (hasattr(cls, "allow_extra_columns") and cls.allow_extra_columns) and extra:
                err_str = ", ".join(repr(k) for k in extra)
                raise SchemaValidationError(
                    f"{cls.__name__} does not allow extra columns, but got: {err_str}"
                )

            old_init(self, **to_pass)
            for k, v in extra.items():
                self[k] = v

        cls.__init__ = new_init

        return cls


# We define this so that we can appropriately annotate the `from_dict` method in a way that will translate to
# subclasses as well.
S = TypeVar("Schema", bound="Schema")


class Schema(Generic[RawDataType_T, RawSchema_T, RawTable_T], metaclass=SchemaMeta):
    allow_extra_columns: ClassVar[bool] = True

    # The Schema class should behave like a dictionary:

    def __getitem__(self, key: str):
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any):
        if hasattr(self, key) or self.allow_extra_columns:
            setattr(self, key, value)
        else:
            raise SchemaValidationError(f"Extra field not allowed: {key!r}")

    def keys(self):
        return self.to_dict().keys()

    def values(self):
        return self.to_dict().values()

    def items(self):
        return self.to_dict().items()

    def __iter__(self):
        return iter(self.keys())

    # The Schema class should be convertible to and from a dictionary:

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_dict(cls: type[S], data: dict) -> S:
        return cls(**data)

    # The schema should support type resolution and optional vs. required type determination:

    @classmethod
    def optional_columns(cls: type[S]) -> list[str]:
        """Return a list of optional columns."""
        return [f.name for f in fields(cls) if cls._is_optional(f.type)]

    @classmethod
    def required_columns(cls: type[S]) -> list[str]:
        """Return a list of required columns."""
        return [f.name for f in fields(cls) if not cls._is_optional(f.type)]

    @classmethod
    def columns(cls: type[S]) -> list[str]:
        """Return a list of all columns, starting with required columns."""
        return cls.required_columns() + cls.optional_columns()

    @classmethod
    def column_type(cls: type[S], col: str) -> ColumnDType:
        """Return the type of a column."""
        return getattr(cls, f"{col}_dtype")

    @classmethod
    def _is_required(cls: type[S], annotation: Optional | ColumnDType) -> bool:
        """Check if the field is required."""
        return not cls._is_optional(annotation)

    @classmethod
    def _is_optional(cls: type[S], annotation: Optional | ColumnDType) -> bool:
        if isinstance(annotation, Optional):
            return True

        origin = get_origin(annotation)

        return (origin is Union or origin is types.UnionType) and type(None) in get_args(annotation)

    @classmethod
    def _base_type(cls: type[S], annotation: Optional | ColumnDType) -> ColumnDType:
        if isinstance(annotation, Optional):
            return annotation.dtype
        elif cls._is_optional(annotation):
            return next(a for a in get_args(annotation) if a is not type(None))
        else:
            return annotation

    @classmethod
    def map_type(cls: type[S], field: Field) -> RawDataType_T:
        """For the base class, we don't do any remapping."""
        return cls._map_type_internal(cls._base_type(field.type))

    @classmethod
    @abstractmethod
    def _map_type_internal(cls: type[S], field_type: ColumnDType) -> RawDataType_T:
        raise NotImplementedError(f"_map_type_internal is not supported by {cls.__name__} objects.")

    # The schema should provide a way to produce an approximate "source schema"

    @classmethod
    @abstractmethod
    def schema(cls: type[S]) -> RawSchema_T:
        raise NotImplementedError(f"schema is not supported by {cls.__name__} objects.")

    # The schema should provide a way to validate or align tables:

    @classmethod
    @abstractmethod
    def _raw_schema_col_type(cls: type[S], schema: RawSchema_T, col: str) -> ColumnDType:
        """Get the type of a column in the schema."""
        raise NotImplementedError(f"__raw_schema_col_type is not supported by {cls.__name__} objects.")

    @classmethod
    @abstractmethod
    def _raw_schema_cols(cls: type[S], schema: RawSchema_T) -> list[str]:
        """Get all columns in the schema."""
        raise NotImplementedError(f"__raw_schema_cols is not supported by {cls.__name__} objects.")

    @classmethod
    def _disallowed_extra_cols(cls: type[S], schema: RawSchema_T) -> list[str]:
        """Get a list of extra columns that are not allowed in the schema."""
        if cls.allow_extra_columns:
            return []
        return [col for col in cls._raw_schema_cols(schema) if col not in set(cls.columns())]

    @classmethod
    def _missing_req_cols(cls: type[S], schema: RawSchema_T) -> list[str]:
        """Get a list of required columns that are missing in the schema."""
        return [col for col in cls.required_columns() if col not in set(cls._raw_schema_cols(schema))]

    @classmethod
    def _mistyped_cols(cls: type[S], schema: RawSchema_T) -> list[tuple[str, ColumnDType, ColumnDType]]:
        """Get a list of columns that have incorrect types in the schema."""
        raw_cols = set(cls._raw_schema_cols(schema))
        return [
            (col, cls.column_type(col), cls._raw_schema_col_type(schema, col))
            for col in cls.columns()
            if col in raw_cols and cls.column_type(col) != cls._raw_schema_col_type(schema, col)
        ]

    @classmethod
    def _validate_schema(cls: type[S], schema: RawSchema_T):
        """Validate the schema against the class schema and raise an error if invalid.

        Args:
            schema: The schema to validate.

        Raises:
            SchemaValidationError: If the schema is invalid.
        """

        disallowed_extra_cols = cls._disallowed_extra_cols(schema)
        missing_req_cols = cls._missing_req_cols(schema)
        mistyped_cols = cls._mistyped_cols(schema)

        if disallowed_extra_cols or missing_req_cols or mistyped_cols:
            raise SchemaValidationError(
                disallowed_extra_cols=disallowed_extra_cols,
                missing_req_cols=missing_req_cols,
                mistyped_cols=mistyped_cols,
            )

    @classmethod
    @abstractmethod
    def _raw_table_schema(cls: type[S], table: RawTable_T) -> RawSchema_T:
        """Get the schema of a table."""
        raise NotImplementedError(f"__raw_table_schema is not supported by {cls.__name__} objects.")

    @classmethod
    def _validate_table(cls: type[S], table: RawTable_T):
        """Validate the table against the schema."""
        cls._validate_schema(cls._raw_table_schema(table))

    @classmethod
    def _is_raw_schema(cls, arg: Any) -> bool:
        """Check if the argument is a raw schema (e.g., of type `RawSchema_T`).

        This is a "best-guess" approach for checking in the base class.

        Args:
            arg: The argument to check.

        Returns:
            True if the argument is a schema, False otherwise.
        """

        return isinstance(arg, type(cls.schema()))

    @classmethod
    def validate(cls: type[S], arg: RawTable_T | RawSchema_T):
        """Validate the argument against the schema.

        Args:
            arg: The argument to validate. This can be a table or a schema.

        Returns:
            `True` if the argument is valid.

        Raises:
            SchemaValidationError: If the argument is invalid.
        """
        if cls._is_raw_schema(arg):
            try:
                cls._validate_schema(arg)
            except SchemaValidationError as e:
                raise e
            except Exception as e:
                raise SchemaValidationError("Schema validation failed") from e
        else:
            try:
                cls._validate_table(arg)
            except (TableValidationError, SchemaValidationError) as e:
                raise e
            except Exception as e:
                raise TableValidationError("Table validation failed") from e

    @classmethod
    def _raw_table_cols(cls: type[S], tbl: RawTable_T) -> list[str]:
        """Get all columns in the table."""
        return cls._raw_schema_cols(cls._raw_table_schema(tbl))

    @classmethod
    @abstractmethod
    def _reorder_raw_table(cls: type[S], tbl: RawTable_T, tbl_order: list[str]) -> RawTable_T:
        raise NotImplementedError(f"_reorder_raw_table is not supported by {cls.__name__} objects.")

    @classmethod
    def _align_col_order(cls: type[S], tbl: RawTable_T) -> RawTable_T:
        """Re-order the columns of the table to match the schema."""
        table_cols = cls._raw_table_cols(tbl)

        out_order = []
        for f in fields(cls):
            if cls._is_required(f.type) or f.name in table_cols:
                out_order.append(f.name)

        if cls.allow_extra_columns:
            out_order.extend([c for c in table_cols if c not in out_order])

        return cls._reorder_raw_table(tbl, out_order)

    @classmethod
    @abstractmethod
    def _cast_raw_table_column(cls: type[S], tbl: RawTable_T, col: str, want_type: ColumnDType) -> RawTable_T:
        raise NotImplementedError(f"_cast_raw_table_column is not supported by {cls.__name__} objects.")

    @classmethod
    def _cast_raw_table(
        cls: type[S], tbl: RawTable_T, mistyped_cols: list[tuple[str, ColumnDType, ColumnDType]]
    ) -> RawTable_T:
        """Cast the columns of the table to match the schema."""

        for col, want_type, _ in mistyped_cols:
            tbl = cls._cast_raw_table_column(tbl, col, want_type)

        return tbl

    @classmethod
    def align(cls: type[S], tbl: RawTable_T) -> RawTable_T:
        """Align the table to the schema.

        Args:
            tbl: The table to align.

        Returns:
            The aligned table.
        """

        mistyped_cols = []

        try:
            cls.validate(tbl)
        except SchemaValidationError as e:
            if e.missing_req_cols or e.disallowed_extra_cols:
                raise SchemaValidationError(
                    disallowed_extra_cols=e.disallowed_extra_cols,
                    missing_req_cols=e.missing_req_cols,
                ) from e
            elif e.mistyped_cols:
                mistyped_cols = e.mistyped_cols
            else:
                raise e
        except TableValidationError as e:
            raise e

        tbl = cls._align_col_order(tbl)

        if mistyped_cols:
            try:
                tbl = cls._cast_raw_table(tbl, mistyped_cols)
            except Exception as e:
                raise SchemaValidationError(mistyped_cols=mistyped_cols) from e

        return tbl
