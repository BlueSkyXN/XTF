"""Target-specific compilers from reconciled values to typed execution actions."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

import pandas as pd

from api.bitable_backend import CanonicalRecord

from .plan import (
    AppendRowsAction,
    ApplySheetConfigAction,
    ClearRangeAction,
    CreateFieldAction,
    CreateRecordsAction,
    DeleteRecordsAction,
    UpdateRecordsAction,
    WriteColumnsAction,
    WriteRangeAction,
)


class BitablePlanCompiler:
    @staticmethod
    def create_field(
        name: str, field_type: int, *, scope: Mapping[str, Any]
    ) -> CreateFieldAction:
        return CreateFieldAction(
            field_name=name, suggested_type=field_type, scope=dict(scope)
        )

    @staticmethod
    def create_records(
        records: Sequence[CanonicalRecord], *, scope: Mapping[str, Any]
    ) -> CreateRecordsAction:
        return CreateRecordsAction(records=tuple(records), scope=dict(scope))

    @staticmethod
    def update_records(
        records: Sequence[CanonicalRecord],
        *,
        scope: Mapping[str, Any],
        clears_values: bool = False,
    ) -> UpdateRecordsAction:
        return UpdateRecordsAction(
            records=tuple(records),
            scope=dict(scope),
            clears_values=clears_values,
        )

    @staticmethod
    def delete_records(
        record_ids: Sequence[str], *, scope: Mapping[str, Any]
    ) -> DeleteRecordsAction:
        return DeleteRecordsAction(
            record_ids=tuple(record_ids), scope=dict(scope), destructive=True
        )


class SheetPlanCompiler:
    @staticmethod
    def clear(a1_range: str, *, scope: Mapping[str, Any]) -> ClearRangeAction:
        return ClearRangeAction(
            a1_range=a1_range,
            scope=dict(scope),
            destructive=True,
            clears_values=True,
        )

    @staticmethod
    def write(
        values: Sequence[Sequence[Any]],
        *,
        scope: Mapping[str, Any],
        destructive: bool = False,
        clears_values: bool = True,
    ) -> WriteRangeAction:
        return WriteRangeAction(
            values=tuple(tuple(row) for row in values),
            scope=dict(scope),
            destructive=destructive,
            clears_values=clears_values,
        )

    @staticmethod
    def append(
        values: Sequence[Sequence[Any]],
        *,
        header_width: int,
        scope: Mapping[str, Any],
    ) -> AppendRowsAction:
        return AppendRowsAction(
            values=tuple(tuple(row) for row in values),
            header_width=header_width,
            scope=dict(scope),
        )

    @staticmethod
    def columns(
        *,
        column_data: Mapping[str, Sequence[Any]],
        column_positions: Mapping[str, int],
        start_row: int,
        max_gap: int,
        header_width: int,
        scope: Mapping[str, Any],
        clears_values: bool,
    ) -> WriteColumnsAction:
        return WriteColumnsAction(
            column_data={name: tuple(values) for name, values in column_data.items()},
            column_positions=dict(column_positions),
            start_row=start_row,
            max_gap=max_gap,
            header_width=header_width,
            scope=dict(scope),
            clears_values=clears_values,
        )

    @staticmethod
    def enrichment(frame: pd.DataFrame) -> ApplySheetConfigAction:
        return ApplySheetConfigAction(frame=frame.copy(), scope={"target": "sheet"})


__all__ = ["BitablePlanCompiler", "SheetPlanCompiler"]
