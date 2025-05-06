# Flexible Schemas

[![PyPI - Version](https://img.shields.io/pypi/v/flexible_schema)](https://pypi.org/project/flexible_schema/)
[![Documentation Status](https://readthedocs.org/projects/flexible-schema/badge/?version=latest)](https://flexible-schema.readthedocs.io/en/latest/?badge=latest)
![python](https://img.shields.io/badge/-Python_3.10-blue?logo=python&logoColor=white)
[![codecov](https://codecov.io/gh/Medical-Event-Data-Standard/flexible_schema/graph/badge.svg?token=89SKXPKVRA)](https://codecov.io/gh/Medical-Event-Data-Standard/flexible_schema)
[![tests](https://github.com/Medical-Event-Data-Standard/flexible_schema/actions/workflows/tests.yaml/badge.svg)](https://github.com/Medical-Event-Data-Standard/flexible_schema/actions/workflows/tests.yml)
[![code-quality](https://github.com/Medical-Event-Data-Standard/flexible_schema/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/Medical-Event-Data-Standard/flexible_schema/actions/workflows/code-quality-main.yaml)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/Medical-Event-Data-Standard/flexible_schema#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/Medical-Event-Data-Standard/flexible_schema/pulls)
[![contributors](https://img.shields.io/github/contributors/Medical-Event-Data-Standard/flexible_schema.svg)](https://github.com/Medical-Event-Data-Standard/flexible_schema/graphs/contributors)

`flexible_schema` provides a simple vehicle to specify and validate schemas for PyArrow tables and JSON
objects that permit extension tables with additional columns, optional columns that may be missing
wholesale (but that must conform to the specified type if present), column-order agnostic validation, and
modes type-coercion where permissible.

## Installation

```bash
pip install flexible_schema
```

## Documentation and Usage

### Defining a schema

You can define a `PyArrowSchema` with a dataclass like syntax:

```python
>>> from flexible_schema import PyArrowSchema, Optional, Required
>>> import pyarrow as pa
>>> class Data(PyArrowSchema):
...     subject_id: Required(pa.int64(), nullable=False)
...     time: pa.timestamp("us")
...     code: Required(pa.string(), nullable=False)
...     numeric_value: Optional(pa.float32())
...     text_value: Optional(pa.string())

```

This schema defines a table that has the following properties:

1. It is a `PyArrow` table.
2. The order of columns in this schema _does not matter_. This is true for all schemas defined with
    `flexible_schema`.
3. It is an _open_ table -- meaning that it can have extra columns that are not defined in the schema. This is
    the default, but can be controlled by setting the `allow_extra_columns: ClassVar[bool] = False` annotation
    in the class definition.
4. It has 2 _required_ columns that do not permit any null values: `subject_id` and `code`. Each of these
    _must_ appear in any table that is valid under this schema and cannot hold `null` values.
5. It has 1 _required_ column that does permit null values: `time`. This column _must_ appear in any table
    that is valid under this schema, but it can hold some `null` values; however, it may not have all `null`
    values.
6. It has 2 _optional_ columns: `numeric_value` and `text_value`. These columns may be missing from a table
    that is valid under this schema; however, if they are present, they must conform to the type specified.
    They are permitted to have any amount of `null` values, including all `null` values.

> [!NOTE]
> Table columns can also have default values, though those should generally not be used and do not affect most
> table processing.

> [!NOTE]
> A full table of the terminology used in this library relating to column and table properties and types can
> be found [below](#terminology)

### Exported names and types

Once defined like this, the schema class can be used in a number of ways. Firstly, it can be used to
automatically get the name and data type of any column associated with the schema:

```python
>>> Data.subject_id_name
'subject_id'
>>> Data.subject_id_dtype
DataType(int64)
>>> Data.time_name
'time'
>>> Data.time_dtype
TimestampType(timestamp[us])

```

This is useful for building downstream tools that want to reliably access column names and types via
programmatic constants, rather than hard-coded literals.

> [!WARNING]
> These attributes have names that are automatically inferred from the column names. This means that if you
> change the name of a column in the schema, the associated attributes may cease to exist. This is still
> beneficial to downstream users, as their code will error out at the import / attribute level, not because a
> hard-coded string no longer matches a column name, but it is something to be aware of.

You can also directly access the "raw schema" type via the `schema` attribute:

```python
>>> Data.schema()
subject_id: int64
time: timestamp[us]
code: string
numeric_value: float
text_value: string

```

### Table and Schema Validation and Alignment

You can also use the schema to validate possible `PyArrow` schemas or tables and align possibly invalid tables
to a valid format. These two options have the following properties:

1. **Validation**: If the input to validation is a schema, it validates that the input has the appropriate
    columns in the appropriate types. If the input is a table, it validates both the schema and the nullable
    properties on the defined columns.
2. **Alignment**: This performs validation, but also performs guaranteeably safe data alterations to ensure
    the table conforms to the schema as much as possible. These alignment operations include:
    - Re-ordering columns.
    - Performing safe type coercion to the target types (e.g., `int` to `float`).

These are exposed via the `validate` and `align` functions:

```python
>>> data_tbl = pa.Table.from_pydict({
...     "subject_id": [1, 2, 3],
...     "code": ["A", "B", "C"],
... })
>>> Data.validate(data_tbl)
Traceback (most recent call last):
  ...
flexible_schema.exceptions.SchemaValidationError: Missing required columns: time
>>> from datetime import datetime
>>> data_tbl = pa.Table.from_pydict({
...     "time": [
...         datetime(2021, 3, 1),
...         datetime(2021, 4, 1),
...         datetime(2021, 5, 1),
...     ],
...     "subject_id": [1, 2, 3],
...     "code": ["A", "B", "C"],
... })
>>> Data.validate(data_tbl) # No issues
>>> aligned_tbl = Data.align(data_tbl)
>>> aligned_tbl
pyarrow.Table
subject_id: int64
time: timestamp[us]
code: string
----
subject_id: [[1,2,3]]
time: [[2021-03-01 00:00:00.000000,2021-04-01 00:00:00.000000,2021-05-01 00:00:00.000000]]
code: [["A","B","C"]]
>>> Data.validate(aligned_tbl)
>>> data_tbl_with_extra = pa.Table.from_pydict({
...     "time": [
...         datetime(2021, 3, 1),
...         datetime(2021, 4, 1),
...     ],
...     "subject_id": [4, 5],
...     "extra_1": ["extra1", "extra2"],
...     "extra_2": [452, 11],
...     "code": ["D", "E"],
... })
>>> Data.align(data_tbl_with_extra)
pyarrow.Table
subject_id: int64
time: timestamp[us]
code: string
extra_1: string
extra_2: int64
----
subject_id: [[4,5]]
time: [[2021-03-01 00:00:00.000000,2021-04-01 00:00:00.000000]]
code: [["D","E"]]
extra_1: [["extra1","extra2"]]
extra_2: [[452,11]]

```

> [!NOTE]
> Schema constraints do _not_ check nullability properties, even though PyArrow schemas permit annotation of
> this property.

### Use as a dataclass

Though rare, you can also use this as a type-hint for a row in a table matching this schema, by using the
class like a direct dataclass.

> [!NOTE]
> When used in this way, optional columns are added with a default value of `None` if no default was specified
> to the output dataclass object.

```python
>>> class Data(PyArrowSchema):
...     subject_id: Required(pa.int64(), nullable=False)
...     time: pa.timestamp("us")
...     code: Required(pa.string(), nullable=False)
...     numeric_value: Optional(pa.float32())
...     text_value: Optional(pa.string()) = "foo"
>>> Data(subject_id=42, time=datetime(2021, 3, 1), code="A")
Data(subject_id=42, time=datetime.datetime(2021, 3, 1, 0, 0), code='A', numeric_value=None, text_value='foo')

```

> [!WARNING]
> Type conversion to the schema dtypes won't happen in this usage case, nor will nullability constraints be
> validated.

```python
>>> class Data(PyArrowSchema):
...     subject_id: Required(pa.int64(), nullable=False)
...     numeric_value: Optional(pa.float32())
...     other: Optional(pa.int16(), default=3)
>>> Data(subject_id="wrong_type") # type conversion won't happen
Data(subject_id='wrong_type', numeric_value=None, other=3)
>>> Data(None, 35.0) # positional arguments and nullability violations
Data(subject_id=None, numeric_value=35.0, other=3)

```

This use case scenario makes more sense with `JSONSchema`, as a JSON "table" is just a typed dictionary. This
is useful in that the `to_dict()` method enables you to naturally use `json.dump()` or `json.dumps()` on the
dataclass object (after application of `to_dict()`).

```python
>>> from flexible_schema import JSONSchema
>>> class Measurement(JSONSchema):
...     subject_id: Required(int, nullable=False)
...     code: Optional(str)
...     numeric_value: Optional(float)
>>> measurement = Measurement(subject_id=42, code="A")
>>> measurement
Measurement(subject_id=42, code='A', numeric_value=None)
>>> measurement.to_dict()
{'subject_id': 42, 'code': 'A'}
>>> json.dumps(measurement.to_dict())
'{"subject_id": 42, "code": "A"}'
>>> Measurement(**json.loads(json.dumps(measurement.to_dict())))
Measurement(subject_id=42, code='A', numeric_value=None)

```

### Extending a schema

You can also extend a schema by subclassing it. This process allows you to add additional columns to a derived
schema without duplicating the base schema columns:

```python
>>> class DerivedData(Data):
...     extra_col: Optional(pa.int64())
>>> DerivedData.schema()
subject_id: int64
numeric_value: float
other: int16
extra_col: int64

```

Note this appends the new columns to the end of the schema, which does affect the default ordering that is
used for aligned columns, though this does not impact data table or query schema validity in any way.

### Supported Schemas

The following schemas are supported:

| Schema Type                                       | Description                                                                                 | Supported Functionalities |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------- |
| [`PyArrowSchema`](src/flexible_schema/pyarrow.py) | A schema that can be used to validate and align PyArrow tables.                             | All functionality.        |
| [`JSONSchema`](src/flexible_schema/json.py)       | A schema wrapper around [`jsonschema`](https://python-jsonschema.readthedocs.io/en/stable). | Validation only.          |

## Terminology

| Category          | Term                                     | Description                                                                                                                                         |
| ----------------- | ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Schema-Level**  | **Closed Schema**                        | Does **not** allow columns beyond explicitly defined schema columns.                                                                                |
|                   | **Open Schema**                          | Allows additional columns not defined in the schema.                                                                                                |
| **Process-Level** | **Validation**                           | Checks table conformance to schema without changing data or structure.                                                                              |
|                   | **Alignment**                            | Reorders columns, adds missing required but fully-nullable columns, performs safe type coercision.                                                  |
| **Column-Level**  | **Required Column**                      | Column must always be present, meeting specified type and nullability constraints.                                                                  |
|                   | **Optional Column**                      | Column is allowed to be absent. If present, it must satisfy specified type and nullability constraints.                                             |
| **Nullability**   | `Nullability.NONE` _or_ `nullable=False` | Column cannot contain any null values.                                                                                                              |
|                   | `Nullability.SOME`                       | Column may contain some null values, but not exclusively null values. Default for required columns.                                                 |
|                   | `Nullability.ALL` _or_ `nullable=True`   | Column may be entirely null; if missing, can be automatically created with all null values during alignment/coercion. Default for optional columns. |

### Open vs. Closed

The schema can be either open or closed. This is specified in the class definition via the
`allow_extra_columns`, which defaults to `True`

```python
>>> from typing import ClassVar
>>> class Closed(PyArrowSchema):
...     allow_extra_columns: ClassVar[bool] = False
...     subject_id: pa.int64()
...     code: pa.string()
>>> Closed.validate(pa.Table.from_pydict({"subject_id": [1, 2], "code": ["A", "B"]}))
>>> Closed.validate(pa.Table.from_pydict({"subject_id": [1, 2], "code": ["A", "B"], "foo": [1, 2]}))
Traceback (most recent call last):
  ...
flexible_schema.exceptions.SchemaValidationError: Disallowed extra columns: foo
>>> class Open(PyArrowSchema):
...     allow_extra_columns: ClassVar[bool] = True
...     subject_id: pa.int64()
...     code: pa.string()
>>> Open.validate(pa.Table.from_pydict({"subject_id": [1, 2], "code": ["A", "B"]}))
>>> Open.validate(pa.Table.from_pydict({"subject_id": [1, 2], "code": ["A", "B"], "foo": [1, 2]}))
>>> class AlsoOpen(PyArrowSchema):
...     subject_id: pa.int64()
...     code: pa.string()
>>> AlsoOpen.validate(pa.Table.from_pydict({"subject_id": [1, 2], "code": ["A", "B"]}))
>>> AlsoOpen.validate(pa.Table.from_pydict({"subject_id": [1, 2], "code": ["A", "B"], "foo": [1, 2]}))

```

### Optional vs. Required

Columns can be either required or optional. This can be specified in one of several ways:

1. By using the `Optional` or `Required` types in the schema definition (though `Required` is the default if
    no explicit `Column` annotation is used and no default value is provided, and this behavior is
    acceptable).
2. By using the `is_optional` initializer argument in the base `Column` type. This is not recommended as it
    is less readable and less explicit.

```python
>>> from flexible_schema import PyArrowSchema, Column, Optional, Required
>>> class MySchema(PyArrowSchema):
...     req_col_1: pa.int64() # Required column. Preferred.
...     req_col_2: Required(pa.int64()) # Required column. Same as above. Preferred.
...     req_col_3: Column(pa.int64(), is_optional=False) # Implicit required column. Not preferred.
...     opt_col_1: Optional(pa.int64()) # Optional column without a default. Preferred.
...     opt_col_2: Column(pa.int64(), is_optional=True) # Implicit optional column. Not preferred.
...     opt_col_3: pa.int64() = 3 # Optionality inferred due to the default value. Not preferred.
...     opt_col_4: Optional(pa.int64()) = 3 # Optional column with a default. Preferred.
>>> MySchema._columns_map()
{'req_col_1': Column(DataType(int64), name=req_col_1),
 'req_col_2': Required(DataType(int64), name=req_col_2),
 'req_col_3': Column(DataType(int64), name=req_col_3, is_optional=False),
 'opt_col_1': Optional(DataType(int64), name=opt_col_1),
 'opt_col_2': Column(DataType(int64), name=opt_col_2, is_optional=True),
 'opt_col_3': Column(DataType(int64), name=opt_col_3, is_optional=True, default=3),
 'opt_col_4': Optional(DataType(int64), name=opt_col_4, default=3)}

```

Required columns:

- Must be present in any input table or schema to validation or alignment
- Cannot have default values
- Assume that, when not specified, the column permits partial but not total nullability (i.e.,
    `nullable=Nullability.SOME`)

Optional columns:

- May be missing from any input table or schema to validation or alignment without issue, but if present,
    must conform to the specified type.
- Can have default values
- Assume that, when not specified, the column permits total nullability (i.e., `nullable=True`)

### Nullability

> [!WARNING]
> Traditional python type hint syntax treats "optional" and "nullable" as equivalent. This is _**not**_ the
> case in this package. Optionality means something may or may not appear in the syntax at all; nullability
> means if it is present, it may or may not be null.

#### Specifying Nullability

You can specify nullability either through the `nullable` initialization keyword argument or by using the
default type-hint syntax indicating a nullable type (e.g., `col: int | None`). There are three reasons to
generally avoid using the latter:

1. The type hint syntax is not as explicit as the constructor syntax, and is commonly used in normal python
    to refer to optionality, not nullability, which differs here.
2. The type hint syntax can only be used for basic python types (e.g., `int`, `str`, etc.) and not
    for the more complex types that are available in this package (e.g., `pa.int64()`, `pa.string()`, etc.).
3. The type hint syntax can only express `nullable=True` or `nullable=False`, whereas this package supports
    not only `nullable=Nullability.ALL` and `nullable=Nullability.NONE` (`True` and `False`, respectively),
    but also `nullable=Nullability.SOME`, which is the default for required columns.

```python

>>> from flexible_schema import PyArrowSchema, Column, Nullability
>>> class MySchema(PyArrowSchema):
...     nullable_col_1: int | None
...     nullable_col_2: Column(int, nullable=True) # Equivalent to int | None
...     nullable_col_3: Column(int, nullable=Nullability.ALL) # Equivalent to nullable=True
...     nullable_col_4: Column(int, nullable=Nullability.SOME) # Inexpressible with type hint syntax
>>> MySchema._columns_map()
{'nullable_col_1': Column(DataType(int64), name=nullable_col_1, nullable=Nullability.ALL),
 'nullable_col_2': Column(DataType(int64), name=nullable_col_2, nullable=Nullability.ALL),
 'nullable_col_3': Column(DataType(int64), name=nullable_col_3, nullable=Nullability.ALL),
 'nullable_col_4': Column(DataType(int64), name=nullable_col_4, nullable=Nullability.SOME)}

```

> [!WARNING]
> Do not try to mix the explicit constructor syntax and the type hint syntax, as results may not be as you
> expect.

```python
>>> class MySchema(PyArrowSchema): # This probably isn't what you want!
...     col_1: Column(int | None)
...     col_2: Column(int | None, nullable=False)
>>> MySchema._columns_map()
{'col_1': Column(int | None, name=col_1),
 'col_2': Column(int | None, name=col_2, nullable=Nullability.NONE)}

```

```python
>>> class MySchema(PyArrowSchema): # This will throw an error:
...     col_1: Column(int) | None
Traceback (most recent call last):
  ...
TypeError: unsupported operand type(s) for |: 'Column' and 'NoneType'

```

#### Meaning of the default across column types

Columns can either allow no, some, or all `null` values. This is specified in the schema via the `nullable`
parameter of the `Optional`, `Required`, or base `Column` types.

```python
>>> from flexible_schema import Optional, Required
>>> class MySchema(PyArrowSchema):
...     req_no_null_1: Required(pa.int64(), nullable=False) # `nullable=False` means no nulls allowed.
...     req_no_null_2: Required(pa.int64(), nullable=Nullability.NONE) # Equivalent to `nullable=False`
...     req_some_null_1: Required(pa.int64(), nullable=Nullability.SOME) # The default.
...     req_all_null_1: Required(pa.int64(), nullable=True) # All nulls allowed.
...     req_all_null_2: Required(pa.int64(), nullable=Nullability.ALL) # Equivalent to `nullable=True`
...     req_implicit: Required(pa.int64()) # Implicitly "some" nullable.
>>> MySchema._columns_map()
{'req_no_null_1': Required(DataType(int64), name=req_no_null_1, nullable=Nullability.NONE),
 'req_no_null_2': Required(DataType(int64), name=req_no_null_2, nullable=Nullability.NONE),
 'req_some_null_1': Required(DataType(int64), name=req_some_null_1, nullable=Nullability.SOME),
 'req_all_null_1': Required(DataType(int64), name=req_all_null_1, nullable=Nullability.ALL),
 'req_all_null_2': Required(DataType(int64), name=req_all_null_2, nullable=Nullability.ALL),
 'req_implicit': Required(DataType(int64), name=req_implicit)}
>>> MySchema._columns_map()["req_implicit"].nullable
<Nullability.SOME: 'some'>

```

The same applies to `Optional` columns, but the default is `nullable=True` (i.e., all nulls allowed).

```python
>>> class MySchema(PyArrowSchema):
...     opt_no_null_1: Optional(pa.int64(), nullable=False) # `nullable=False` means no nulls allowed.
...     opt_no_null_2: Optional(pa.int64(), nullable=Nullability.NONE) # Equivalent to `nullable=False`
...     opt_some_null_1: Optional(pa.int64(), nullable=Nullability.SOME) # No longer the default.
...     opt_all_null_1: Optional(pa.int64(), nullable=True) # All nulls allowed.
...     opt_all_null_2: Optional(pa.int64(), nullable=Nullability.ALL) # Equivalent to `nullable=True`
...     opt_implicit_default: Optional(pa.int64(), default=3) # Implicitly "some" nullable.
...     opt_implicit: Optional(pa.int64()) # Implicitly "all" nullable.
>>> MySchema._columns_map()
{'opt_no_null_1': Optional(DataType(int64), name=opt_no_null_1, nullable=Nullability.NONE),
 'opt_no_null_2': Optional(DataType(int64), name=opt_no_null_2, nullable=Nullability.NONE),
 'opt_some_null_1': Optional(DataType(int64), name=opt_some_null_1, nullable=Nullability.SOME),
 'opt_all_null_1': Optional(DataType(int64), name=opt_all_null_1, nullable=Nullability.ALL),
 'opt_all_null_2': Optional(DataType(int64), name=opt_all_null_2, nullable=Nullability.ALL),
 'opt_implicit_default': Optional(DataType(int64), name=opt_implicit_default, default=3),
 'opt_implicit': Optional(DataType(int64), name=opt_implicit)}
>>> MySchema._columns_map()["opt_implicit_default"].nullable
<Nullability.SOME: 'some'>
>>> MySchema._columns_map()["opt_implicit"].nullable
<Nullability.ALL: 'all'>

```

If you define columns manually, the behavior is the same:

```python
>>> class MySchema(PyArrowSchema):
...     no_default: pa.int64()
...     no_default_optional: Column(pa.int64(), is_optional=True)
...     default: pa.int64() = 3
>>> MySchema._columns_map()
{'no_default': Column(DataType(int64), name=no_default),
 'no_default_optional': Column(DataType(int64), name=no_default_optional, is_optional=True),
 'default': Column(DataType(int64), name=default, is_optional=True, default=3)}
>>> MySchema._columns_map()["no_default"].nullable
<Nullability.SOME: 'some'>
>>> MySchema._columns_map()["default"].nullable
<Nullability.SOME: 'some'>
>>> MySchema._columns_map()["no_default_optional"].nullable
<Nullability.ALL: 'all'>

```
