"""A Meta-class for defining Schemas that can be created like dataclasses and used to validate tables."""

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, fields
from typing import Any, ClassVar, Generic, TypeVar

from .columns import Column, ColumnDType, Nullability, resolve_dataclass_field
from .exceptions import SchemaValidationError, TableValidationError

RawDataType_T = TypeVar("RawDataType_T")
RawSchema_T = TypeVar("RawSchema_T")
RawTable_T = TypeVar("RawTable_T")


class SchemaMeta(ABCMeta):
    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        cls = dataclass(cls)  # explicitly turn cls into a dataclass here
        # Add constants after dataclass is fully initialized

        cols = [resolve_dataclass_field(f, type_mapper=cls.map_type) for f in fields(cls)]

        for f, c in zip(fields(cls), cols, strict=False):
            f.metadata = {**f.metadata, "column": c}

        for c in cols:
            # Set attribute shortcuts
            setattr(cls, f"{c.name}_name", c.name)
            setattr(cls, f"{c.name}_dtype", c.dtype)

        field_names = [c.name for c in cols]

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
    def _columns(cls: type[S]) -> list[Column]:
        return [f.metadata["column"] for f in fields(cls)]

    @classmethod
    def _columns_map(cls: type[S]) -> dict[str, Column]:
        return {c.name: c for c in cls._columns()}

    @classmethod
    def optional_columns(cls: type[S]) -> list[str]:
        """Return a list of optional columns."""
        return [c.name for c in cls._columns() if c.is_optional]

    @classmethod
    def required_columns(cls: type[S]) -> list[str]:
        """Return a list of required columns."""
        return [c.name for c in cls._columns() if c.is_required]

    @classmethod
    def columns(cls: type[S]) -> list[str]:
        """Return a list of all columns, starting with required columns."""
        return cls.required_columns() + cls.optional_columns()

    @classmethod
    def column_type(cls: type[S], col: str) -> ColumnDType:
        """Return the type of a column."""
        return cls._columns_map()[col].dtype

    @classmethod
    @abstractmethod
    def map_type(cls: type[S], field_type: ColumnDType) -> RawDataType_T:
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
    @abstractmethod
    def _any_null(cls: type[S], table: RawTable_T, col: str) -> bool:
        """Check if any values in the column are null."""
        raise NotImplementedError(f"_any_null is not supported by {cls.__name__} objects.")

    @classmethod
    @abstractmethod
    def _all_null(cls: type[S], table: RawTable_T, col: str) -> bool:
        """Check if all values in the column are null."""
        raise NotImplementedError(f"_all_null is not supported by {cls.__name__} objects.")

    @classmethod
    def _validate_table(cls: type[S], table: RawTable_T):
        """Validate the table against the schema."""
        cls._validate_schema(cls._raw_table_schema(table))

        nullability_none_err_cols = []
        nullability_some_err_cols = []
        for col in cls.columns():
            if col not in cls._raw_table_cols(table):
                continue

            match cls._columns_map()[col].nullable:
                case Nullability.NONE if cls._any_null(table, col):
                    nullability_none_err_cols.append(col)
                case Nullability.SOME if cls._all_null(table, col):
                    nullability_some_err_cols.append(col)
                case Nullability.ALL:
                    continue

        if nullability_none_err_cols or nullability_some_err_cols:
            raise TableValidationError(
                nullability_none_err_cols=nullability_none_err_cols,
                nullability_some_err_cols=nullability_some_err_cols,
            )

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
    @abstractmethod
    def _is_raw_table(cls, arg: Any) -> bool:
        """Check if the argument is a raw table (e.g., of type `RawTable_T`).

        Args:
            arg: The argument to check.

        Returns:
            True if the argument is a table, False otherwise.
        """
        raise NotImplementedError(f"_is_raw_table is not supported by {cls.__name__} objects.")

    @classmethod
    def validate(cls: type[S], arg: RawTable_T | RawSchema_T):
        """Validate the argument against the schema.

        Args:
            arg: The argument to validate. This can be a table or a schema.

        Returns:
            `True` if the argument is valid.

        Raises:
            SchemaValidationError: If the argument is a schema and invalid.
            TableValidationError: If the argument is a table and invalid.
            TypeError: If the argument is neither a schema nor a table.
        """
        if cls._is_raw_schema(arg):
            try:
                cls._validate_schema(arg)
            except SchemaValidationError as e:
                raise e
            except Exception as e:
                raise SchemaValidationError("Schema validation failed") from e
        elif cls._is_raw_table(arg):
            try:
                cls._validate_table(arg)
            except (TableValidationError, SchemaValidationError) as e:
                raise e
            except Exception as e:
                raise TableValidationError("Table validation failed") from e
        else:
            raise TypeError(f"Expected a schema or table, but got: {type(arg).__name__}")

    @classmethod
    def _raw_table_cols(cls: type[S], table: RawTable_T) -> list[str]:
        """Get all columns in the table."""
        return cls._raw_schema_cols(cls._raw_table_schema(table))

    @classmethod
    @abstractmethod
    def _reorder_raw_table(cls: type[S], table: RawTable_T, table_order: list[str]) -> RawTable_T:
        raise NotImplementedError(f"_reorder_raw_table is not supported by {cls.__name__} objects.")

    @classmethod
    def _align_col_order(cls: type[S], table: RawTable_T) -> RawTable_T:
        """Re-order the columns of the table to match the schema."""
        table_cols = cls._raw_table_cols(table)

        out_order = []
        for c in cls._columns():
            if c.is_required or c.name in table_cols:
                out_order.append(c.name)

        if cls.allow_extra_columns:
            out_order.extend([c for c in table_cols if c not in out_order])

        return cls._reorder_raw_table(table, out_order)

    @classmethod
    @abstractmethod
    def _cast_raw_table_column(
        cls: type[S], table: RawTable_T, col: str, want_type: ColumnDType
    ) -> RawTable_T:
        raise NotImplementedError(f"_cast_raw_table_column is not supported by {cls.__name__} objects.")

    @classmethod
    def _cast_raw_table(
        cls: type[S], table: RawTable_T, mistyped_cols: list[tuple[str, ColumnDType, ColumnDType]]
    ) -> RawTable_T:
        """Cast the columns of the table to match the schema."""

        for col, want_type, _ in mistyped_cols:
            table = cls._cast_raw_table_column(table, col, want_type)

        return table

    @classmethod
    def align(cls: type[S], table: RawTable_T) -> RawTable_T:
        """Align the table to the schema.

        Args:
            table: The table to align.

        Returns:
            The aligned table.
        """

        mistyped_cols = []

        try:
            cls.validate(table)
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

        table = cls._align_col_order(table)

        if mistyped_cols:
            try:
                table = cls._cast_raw_table(table, mistyped_cols)
            except Exception as e:
                raise SchemaValidationError(mistyped_cols=mistyped_cols) from e

        return table
