from typing import Any


class SchemaValidationError(Exception):
    """Exception raised for schema validation errors."""

    def __init__(
        self,
        msg: str | None = None,
        *,
        disallowed_extra_cols: list[str] | None = None,
        missing_req_cols: list[str] | None = None,
        mistyped_cols: list[tuple[str, Any, Any]] | None = None,
    ):
        self.disallowed_extra_cols = disallowed_extra_cols
        self.missing_req_cols = missing_req_cols
        self.mistyped_cols = mistyped_cols
        if msg is not None:
            super().__init__(msg)
        else:
            super().__init__(self.msg)

    @property
    def msg(self) -> str:
        msg_parts = []
        if self.disallowed_extra_cols:
            msg_parts.append(f"Disallowed extra columns: {', '.join(self.disallowed_extra_cols)}")
        if self.missing_req_cols:
            msg_parts.append(f"Missing required columns: {', '.join(self.missing_req_cols)}")
        if self.mistyped_cols:
            mistyped_strs = [f"{col} (want {want}, got {got})" for col, want, got in self.mistyped_cols]
            msg_parts.append(f"Columns with incorrect types: {', '.join(mistyped_strs)}")

        return ". ".join(msg_parts)


class TableValidationError(Exception):
    pass
