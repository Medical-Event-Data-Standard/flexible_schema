import copy
import types
from collections.abc import Callable
from dataclasses import MISSING, Field
from enum import Enum
from typing import Any, Union, get_args, get_origin


class Nullability(Enum):
    """A simple str-like enum to represent the nullability of a column.

    Attributes:
        NONE: No value in the given column can be `null`/`None`.
        SOME: Some, but not all, values in the given column can be `null`/`None`.
        ALL: Any value up to and including all values in the given column can be `null`/`None`.

    Examples:
        >>> Nullability.NONE
        <Nullability.NONE: 'none'>
        >>> Nullability.SOME == "some"
        True
        >>> Nullability.ALL == "foo"
        False
        >>> Nullability.NONE == Nullability.NONE
        True
        >>> Nullability.SOME == Nullability.ALL
        False
    """

    NONE = "none"
    SOME = "some"
    ALL = "all"

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        else:
            return super().__eq__(other)


ColumnDType = type | Any


class Column:
    """A simple class to represent a column in the schema."""

    def __init__(
        self,
        dtype: ColumnDType,
        default: ColumnDType | None = None,
        nullable: bool | Nullability | None = None,
        name: str | None = None,
        is_optional: bool | None = None,
    ):
        self.dtype = dtype
        self.name = name
        self.is_optional = is_optional
        self.default = default
        self.nullable = nullable

    @property
    def default(self) -> ColumnDType | None:
        return self._default() if callable(self._default) else self._default

    @default.setter
    def default(self, value: ColumnDType | None):
        if value is None:
            self._default = None
            return

        if self.is_required:
            raise ValueError("Required columns cannot have a default value")

        self._default = copy.deepcopy(value)

    @property
    def is_required(self) -> bool:
        return not self.is_optional

    @property
    def is_optional(self) -> bool:
        return self._is_optional

    @is_optional.setter
    def is_optional(self, value: bool):
        self._is_optional = value

    @property
    def has_default(self) -> bool:
        return self.default is not None

    @property
    def nullable(self) -> Nullability:
        if self._nullable is None:
            if self.is_optional:
                return Nullability.SOME if self.has_default else Nullability.ALL
            else:
                return Nullability.SOME
        return self._nullable

    @nullable.setter
    def nullable(self, value: bool | str | Nullability | None):
        match value:
            case bool():
                if value:
                    self._nullable = Nullability.ALL
                else:
                    self._nullable = Nullability.NONE
            case str() if value in ("none", "some", "all"):
                self._nullable = Nullability(value)
            case Nullability() | None:
                self._nullable = value
            case _:
                raise TypeError(
                    f"Invalid type for nullable: {type(value)}, expected bool, str, or Nullability. "
                    f"If using a string, it must be one of 'none', 'some', or 'all'."
                )

    def __repr__(self) -> str:
        cls_str = self.__class__.__name__
        t_str = self.dtype.__name__ if hasattr(self.dtype, "__name__") else repr(self.dtype)

        if self.name:
            t_str = f"{t_str}, name={self.name}"
        if cls_str == "Column":
            t_str = f"{t_str}, is_optional={self.is_optional}"
        if self.has_default:
            t_str = f"{t_str}, default={self.default}"
        if self._nullable is not None:
            t_str = f"{t_str}, nullable={self.nullable}"

        return f"{cls_str}({t_str})"


class Optional(Column):
    """A class to represent optional types in a schema.

    Examples:
        >>> O = Optional(int)
        >>> print(O)
        Optional(int)
        >>> O.dtype
        <class 'int'>
        >>> O.has_default
        False
        >>> O.is_optional
        True
        >>> O.nullable
        <Nullability.ALL: 'all'>

    You can also define Optional columns with default values and nullability constraints:

        >>> O = Optional(int, default=42, nullable=True)
        >>> O
        Optional(int, default=42, nullable=Nullability.ALL)
        >>> O.has_default
        True
        >>> O.is_optional
        True
        >>> O.nullable
        <Nullability.ALL: 'all'>
        >>> O = Optional(list[str], default=["foo"], nullable=False)
        >>> O.nullable
        <Nullability.NONE: 'none'>
        >>> O.has_default
        True
        >>> O.default
        ['foo']

    Default values are deep-copied to avoid mutable default arguments:

        >>> default_list = ["foo"]
        >>> O = Optional(list[str], default=default_list)
        >>> O.default
        ['foo']
        >>> O.default[0] = "bar"
        >>> O.default
        ['bar']
        >>> default_list
        ['foo']

    You can't try to overwrite `is_optional` upon initialization:

        >>> O = Optional(int, is_optional=False)
        Traceback (most recent call last):
            ...
        ValueError: is_optional is not a valid argument for Optional
    """

    def __init__(self, *args, **kwargs):
        if "is_optional" in kwargs:
            raise ValueError(f"is_optional is not a valid argument for {self.__class__.__name__}")
        kwargs["is_optional"] = True
        self._is_optional = True
        super().__init__(*args, **kwargs)

    @Column.is_optional.setter
    def is_optional(self, value: bool):
        if not value:
            raise ValueError(f"is_optional cannot be set to {value} for {self.__class__.__name__} columns")


class Required(Column):
    def __init__(self, *args, **kwargs):
        if "is_optional" in kwargs:
            raise ValueError(f"is_optional is not a valid argument for {self.__class__.__name__}")
        kwargs["is_optional"] = False
        self._is_optional = False
        super().__init__(*args, **kwargs)

    @Column.is_optional.setter
    def is_optional(self, value: bool):
        if value:
            raise ValueError(f"is_optional cannot be set to {value} for {self.__class__.__name__} columns")


def _resolve_annotation(
    annotation: Any, type_mapper: Callable[[ColumnDType], ColumnDType]
) -> Column | Optional | Required:
    if isinstance(annotation, Column):
        return annotation

    origin = get_origin(annotation)
    if (origin is Union or origin is types.UnionType) and type(None) in get_args(annotation):
        base_type = next(a for a in get_args(annotation) if a is not type(None))
        col = _resolve_annotation(base_type, type_mapper)
        if col._nullable is not None:
            raise ValueError("foo")
        col.nullable = True

        return col

    return Column(type_mapper(annotation))


def resolve_dataclass_field(
    field: Field, type_mapper: Callable[[ColumnDType], ColumnDType]
) -> Column | Optional | Required:
    """Resolves a dataclass field into a column specification."""

    col = _resolve_annotation(field.type, type_mapper)
    col.name = field.name

    has_default = (field.default is not MISSING) or (field.default_factory is not MISSING)

    if has_default:
        col.is_optional = True
        col.default = field.default if field.default is not MISSING else field.default_factory

    return col
