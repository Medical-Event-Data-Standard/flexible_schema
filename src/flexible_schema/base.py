"""A simple class for flexible schema definition and usage."""

import types
from dataclasses import MISSING, asdict, dataclass, field, fields
from typing import Any, ClassVar, Union, get_args, get_origin


class SchemaValidationError(Exception):
    pass


class SchemaMeta(type):
    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        cls = dataclass(cls)  # explicitly turn cls into a dataclass here
        # Add constants after dataclass is fully initialized
        for f in fields(cls):
            setattr(cls, f"{f.name}_name", f.name)
            remapped_type = cls._remap_type(f)
            setattr(cls, f"{f.name}_dtype", remapped_type)
        return cls


class Schema(metaclass=SchemaMeta):
    """A flexible mixin Schema class for easy definition of flexible, readable schemas."""

    allow_extra_columns: ClassVar[bool] = True
    _extra_fields: dict[str, Any] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self):
        defined_field_names = {f.name for f in fields(self)}
        if self.allow_extra_columns:
            # Identify and store any extra fields provided at initialization
            provided_fields = set(self.__dict__.keys())
            extra_fields = provided_fields - defined_field_names - {"_extra_fields"}
            for field_name in extra_fields:
                self._extra_fields[field_name] = self.__dict__.pop(field_name)
        else:
            provided_fields = set(self.__dict__.keys())
            extra_fields = provided_fields - defined_field_names - {"_extra_fields"}
            if extra_fields:
                raise SchemaValidationError(f"Unexpected extra fields provided: {extra_fields}")

    def __getitem__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        elif self.allow_extra_columns and key in self._extra_fields:
            return self._extra_fields[key]
        else:
            raise KeyError(f"{key} not found in schema.")

    def __setitem__(self, key, value):
        if key in {f.name for f in fields(self)}:
            setattr(self, key, value)
        elif self.allow_extra_columns:
            self._extra_fields[key] = value
        else:
            raise SchemaValidationError(f"Extra fields not allowed, got '{key}'.")

    def keys(self):
        return list({f.name for f in fields(self)} | self._extra_fields.keys())

    def values(self):
        return [self[k] for k in self.keys()]

    def items(self):
        return [(k, self[k]) for k in self.keys()]

    def __iter__(self):
        return iter(self.keys())

    def to_dict(self) -> dict[str, Any]:
        out = {**asdict(self), **self._extra_fields}
        return {k: v for k, v in out.items() if v is not MISSING and v is not None}

    @classmethod
    def from_dict(cls, data: dict[str, Any]):
        field_names = {f.name for f in fields(cls)}
        known_fields = {k: v for k, v in data.items() if k in field_names}
        instance = cls(**known_fields)
        extra_fields = {k: v for k, v in data.items() if k not in field_names}
        if extra_fields:
            if cls.allow_extra_columns:
                instance._extra_fields = extra_fields
            else:
                raise SchemaValidationError(f"Unexpected extra fields provided: {set(extra_fields)}")
        return instance

    @classmethod
    def _is_optional(cls, annotation) -> bool:
        origin = get_origin(annotation)

        return (origin is Union or origin is types.UnionType) and type(None) in get_args(annotation)

    @classmethod
    def _remap_type(cls, field):
        """For the base class, we don't do any remapping."""
        return field.type
