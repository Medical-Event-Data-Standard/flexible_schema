"""A simple class for flexible schema definition and usage."""

import types
from dataclasses import dataclass, fields
from typing import ClassVar, Union, get_args, get_origin


class SchemaValidationError(Exception):
    pass


class SchemaMeta(type):
    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        cls = dataclass(cls)  # explicitly turn cls into a dataclass here
        # Add constants after dataclass is fully initialized

        field_names = []
        for f in fields(cls):
            field_names.append(f.name)
            setattr(cls, f"{f.name}_name", f.name)
            remapped_type = cls._remap_type(f)
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
                err_str = ", ".join(repr(k) for k in extra.keys())
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

    def __post_init__(self):
        defined_field_names = {f.name for f in fields(self)}
        provided_fields = set(self.__dict__)
        extra_fields = provided_fields - defined_field_names
        if extra_fields and not self.allow_extra_columns:
            raise SchemaValidationError(f"Unexpected extra fields: {extra_fields}")

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        if hasattr(self, key) or self.allow_extra_columns:
            setattr(self, key, value)
        else:
            raise SchemaValidationError(f"Extra field not allowed: {key}")

    def keys(self):
        return list(self.__dict__)

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def items(self):
        return [(k, self[k]) for k in self.keys()]

    def __iter__(self):
        return iter(self.keys())

    @classmethod
    def _is_optional(cls, annotation) -> bool:
        origin = get_origin(annotation)

        return (origin is Union or origin is types.UnionType) and type(None) in get_args(annotation)

    @classmethod
    def _remap_type(cls, field):
        """For the base class, we don't do any remapping."""
        return field.type
