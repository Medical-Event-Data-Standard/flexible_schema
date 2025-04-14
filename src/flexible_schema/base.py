"""A simple class for flexible schema definition and usage."""

import types
from dataclasses import dataclass, fields
from typing import Any, ClassVar, Union, get_args, get_origin


class SchemaValidationError(Exception):
    pass


class Optional:
    """A class to represent optional types in a schema.

    Examples:

        >>> O = Optional(int)
        >>> print(O)
        Optional(int)
        >>> O.type
        <class 'int'>
        >>> O.default is None
        True
        >>> Optional(int, default=42)
        Optional(int, default=42)
    """

    def __init__(self, type_: Any, default: Any = None):
        self.type = type_
        self.default = default

    def __repr__(self):
        t_str = self.type.__name__ if hasattr(self.type, "__name__") else repr(self.type)
        return f"Optional({t_str})" if self.default is None else f"Optional({t_str}, default={self.default})"


class SchemaMeta(type):
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


class Schema(metaclass=SchemaMeta):
    allow_extra_columns: ClassVar[bool] = True

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

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)

    def items(self):
        return self.to_dict().items()

    def __iter__(self):
        return iter(self.keys())

    @classmethod
    def _is_optional(cls, annotation: Any) -> bool:
        if isinstance(annotation, Optional):
            return True

        origin = get_origin(annotation)

        return (origin is Union or origin is types.UnionType) and type(None) in get_args(annotation)

    @classmethod
    def _base_type(cls, annotation: Any) -> Any:
        if isinstance(annotation, Optional):
            return annotation.type
        elif cls._is_optional(annotation):
            return next(a for a in get_args(annotation) if a is not type(None))
        else:
            return annotation

    @classmethod
    def map_type(cls, field):
        """For the base class, we don't do any remapping."""
        return cls._map_type_internal(cls._base_type(field.type))

    @classmethod
    def _map_type_internal(cls, field_type: Any) -> Any:
        return field_type

    @classmethod
    def schema(cls):
        out = {}
        for f in fields(cls):
            out[f.name] = cls.map_type(f)
        return out
