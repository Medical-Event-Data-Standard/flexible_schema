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

## Quick-start

### 1. Installation

```bash
pip install flexible_schema
```

### 2. Usage

You can define a `PyArrowSchema` with a dataclass like syntax:

```python
>>> from flexible_schema import PyArrowSchema, Optional, Required
>>> from typing import ClassVar
>>> import pyarrow as pa
>>> import datetime
>>> class Data(PyArrowSchema):
...     allow_extra_columns: ClassVar[bool] = True
...
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
3. It is an _open_ table -- meaning that it can have extra columns that are not defined in the schema.
4. It has 2 _required_ columns that do not permit any null values: `subject_id` and `code`. Each of these
    _must_ appear in any table that is valid under this schema and cannot hold `null` values.
5. It has 1 _required_ column that does permit null values: `time`. This column _must_ appear in any table
    that is valid under this schema, but it can hold `null` values.
6. It has 2 _optional_ columns: `numeric_value` and `text_value`. These columns may be missing from a table
    that is valid under this schema; however, if they are present, they must conform to the type specified.

> [!NOTE]
> A full table of the terminology used in this library relating to column and table properties and types can
> be found [below](#terminology)

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

You can also use the schema to validate, align, or coerce an input `PyArrow` table. These three options have
the following differences:

1. **Validation**: This checks that the input table has the same columns as the schema, and that the types
    of those columns are compatible with the schema. It does not modify the input table in any way. Errors
    are raised if the input table does not conform to the schema.
2. **Alignment**: This performs validation, but also performs guaranteeably safe data alterations to ensure
    the table conforms to the schema as much as possible. These alignment operations include:
    - Re-ordering columns
    - Adding required columns that permit total nullability with all null values.
    - Performing safe type coercion to the target types (e.g., `int` to `float`)

These are exposed via the `validate` and `align` functions. Either can take as input table objects or raw
schema objects -- in the latter case, some aspects of the nullability constraints are not validated, as they
can't be determined from the schema data types alone.

```python
>>> data_tbl = pa.Table.from_pydict({
...     "subject_id": [1, 2, 3],
...     "code": ["A", "B", "C"],
... })
>>> Data.validate(data_tbl)
Traceback (most recent call last):
  ...
flexible_schema.exceptions.SchemaValidationError: Missing required columns: time
>>> data_tbl = pa.Table.from_pydict({
...     "time": [
...         datetime.datetime(2021, 3, 1),
...         datetime.datetime(2021, 4, 1),
...         datetime.datetime(2021, 5, 1),
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
...         datetime.datetime(2021, 3, 1),
...         datetime.datetime(2021, 4, 1),
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

## Detailed Documentation

### Terminology

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

### Optional vs. Required

### Nullability

Columns can either allow no, some, or all `null` values. This is specified in the schema via the `nullable`
parameter of the `Optional` and `Required` types.

```python
>>> from flexible_schema import PyArrowSchema, Optional, Required, Nullability
>>> class MySchema(PyArrowSchema):
...     req_no_null_1: Required(pa.int64(), nullable=False) # `nullable=False` means no nulls allowed.
...     req_no_null_2: Required(pa.int64(), nullable=Nullability.NONE) # Equivalent to `nullable=False`
...     req_some_null_1: Required(pa.int64(), nullable=Nullability.SOME) # The default.
...     req_some_null_2: pa.int64() # Equivalent to the above.
...     req_all_null_1: Required(pa.int64(), nullable=True) # All nulls allowed.
...     req_all_null_2: Required(pa.int64(), nullable=Nullability.ALL) # Equivalent to `nullable=True`

```

The same applies to `Optional` columns, but the default is `nullable=True` (i.e., all nulls allowed).

```python
>>> class MySchema2(PyArrowSchema):
...     opt_no_null_1: Optional(pa.int64(), nullable=False) # `nullable=False` means no nulls allowed.
...     opt_no_null_2: Optional(pa.int64(), nullable=Nullability.NONE) # Equivalent to `nullable=False`
...     opt_some_null_1: Optional(pa.int64(), nullable=Nullability.SOME) # No longer the default.
...     opt_all_null_1: Optional(pa.int64(), nullable=True) # All nulls allowed.
...     opt_all_null_2: Optional(pa.int64(), nullable=Nullability.ALL) # Equivalent to `nullable=True`
...     opt_all_null_3: Optional(pa.int64()) # Equivalent to the above.

```
