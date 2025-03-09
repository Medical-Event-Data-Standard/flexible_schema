# Flexible Schemas

[![PyPI - Version](https://img.shields.io/pypi/v/flexible_schema)](https://pypi.org/project/flexible_schema/)
![python](https://img.shields.io/badge/-Python_3.10-blue?logo=python&logoColor=white)
[![codecov](https://codecov.io/gh/mmcdermott/flexible_schema/graph/badge.svg?token=89SKXPKVRA)](https://codecov.io/gh/mmcdermott/flexible_schema)
[![tests](https://github.com/mmcdermott/flexible_schema/actions/workflows/tests.yaml/badge.svg)](https://github.com/mmcdermott/flexible_schema/actions/workflows/tests.yml)
[![code-quality](https://github.com/mmcdermott/flexible_schema/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/mmcdermott/flexible_schema/actions/workflows/code-quality-main.yaml)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/mmcdermott/flexible_schema#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/mmcdermott/flexible_schema/pulls)
[![contributors](https://img.shields.io/github/contributors/mmcdermott/flexible_schema.svg)](https://github.com/mmcdermott/flexible_schema/graphs/contributors)

This package provides a simple metaclass mixin to enable specifying validatable PyArrow schemas with optional
columns or while allowing column re-ordering or extra columns.

## Installation

```bash
pip install flexible_schema
```

## Usage

```python
>>> from flexible_schema import Schema, with_field_names_and_types

>>> from typing import Optional
>>> @with_field_names_and_types
... @dataclass
... class Data(Schema):
...     allow_extra_columns: ClassVar[bool] = True
...     subject_id: int
...     time: datetime.datetime
...     code: str
...     numeric_value: Optional[float] = None
...     text_value: Optional[str] = None
>>> Data.subject_id_name
'subject_id'
>>> Data.subject_id_dtype
DataType(int64)
>>> Data.time_name
'time'
>>> Data.time_dtype
TimestampType(timestamp[us])
>>> data = Data(subject_id=1, time=datetime.datetime(2025, 3, 7, 16), code="A", numeric_value=1.0)
>>> data # doctest: +NORMALIZE_WHITESPACE
Data(subject_id=1,
     time=datetime.datetime(2025, 3, 7, 16, 0),
     code='A',
     numeric_value=1.0,
     text_value=None)
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
----
subject_id: [[1,2,3]]
time: [[2021-03-01 00:00:00.000000,2021-04-01 00:00:00.000000,2021-05-01 00:00:00.000000]]
code: [["A","B","C"]]
numeric_value: [[null,null,null]]
text_value: [[null,null,null]]
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
extra_1: string
extra_2: int64
----
subject_id: [[4,5]]
time: [[2021-03-01 00:00:00.000000,2021-04-01 00:00:00.000000]]
code: [["D","E"]]
numeric_value: [[null,null]]
text_value: [[null,null]]
extra_1: [["extra1","extra2"]]
extra_2: [[452,11]]
>>> Data.to_json_schema() # doctest: +NORMALIZE_WHITESPACE
{'type': 'object',
 'properties': {'subject_id': {'type': 'integer'},
                'time': {'type': 'string', 'format': 'date-time'},
                'code': {'type': 'string'},
                'numeric_value': {'type': 'number'},
                'text_value': {'type': 'string'}},
 'required': ['subject_id', 'time', 'code'],
 'additionalProperties': True}
```
