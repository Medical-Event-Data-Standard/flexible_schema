from enum import Enum
from typing import Any


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

    dtype: ColumnDType
    default: ColumnDType | None = None
    name: str = "???"

    def __init__(
        self,
        dtype: ColumnDType,
        default: ColumnDType | None = None,
        nullable: bool | Nullability | None = None,
        name: str | None = None,
        is_optional: bool = False,
    ):
        self._frozen = False

        self.dtype = dtype
        self.default = default
        self.name = name
        self._is_optional = is_optional

        if self.is_required and self.has_default:
            raise ValueError("Required columns cannot have a default value")

        self.nullable = nullable

        self._frozen = True

    @property
    def is_required(self):
        return not self.is_optional

    @property
    def is_optional(self):
        return self._is_optional

    @property
    def has_default(self):
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
        if self._frozen:
            raise AttributeError(f"Cannot modify {self.__class__.__name__} nullability after initialization")

        match value:
            case bool():
                if value:
                    self._nullable = Nullability.SOME
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

    def __repr__(self):
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
        >>> Optional(int, default=42)
        Optional(int, default=42)
    """

    def __init__(self, *args, **kwargs):
        if "is_optional" in kwargs:
            raise ValueError("is_optional is not a valid argument for {self.__class__.__name__}")
        kwargs["is_optional"] = True
        super().__init__(*args, **kwargs)


class Required(Column):
    def __init__(self, *args, **kwargs):
        if "is_optional" in kwargs:
            raise ValueError("is_optional is not a valid argument for {self.__class__.__name__}")
        kwargs["is_optional"] = False
        super().__init__(*args, **kwargs)
