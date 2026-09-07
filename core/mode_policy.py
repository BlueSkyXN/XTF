"""Single XTF 2.0 mode/strategy policy used before inspection or compilation."""

from __future__ import annotations

from dataclasses import dataclass

from .config import MatchStrategy, SourceType, SyncMode


@dataclass(frozen=True)
class ModeDecision:
    requested_mode: SyncMode
    effective_mode: SyncMode
    effective_strategy: str


class ModePolicy:
    @staticmethod
    def decide(
        *,
        mode: SyncMode,
        strategy: MatchStrategy | None,
        index_column: str | None,
        source_type: SourceType,
        selective_enabled: bool,
    ) -> ModeDecision:
        if mode is SyncMode.CLONE:
            if strategy is not None:
                raise ValueError("clone 模式必须省略 match_strategy")
            return ModeDecision(mode, mode, "replace_all")
        if strategy is MatchStrategy.BY_KEY:
            if not index_column:
                raise ValueError("by_key 模式必须配置 index_column")
        elif strategy is MatchStrategy.APPEND_ONLY:
            if mode is not SyncMode.INCREMENTAL:
                raise ValueError("append_only 仅支持 incremental")
            if index_column:
                raise ValueError("append_only 禁止配置 index")
            if selective_enabled:
                raise ValueError("append_only 禁止与 selective 同时使用")
            if source_type is SourceType.BITABLE:
                raise ValueError("Bitable source 只支持 by_key")
        else:
            raise ValueError("非 clone 模式必须显式配置 match_strategy")
        if (
            mode in {SyncMode.FULL, SyncMode.OVERWRITE}
            and strategy is not MatchStrategy.BY_KEY
        ):
            raise ValueError(f"{mode.value} 只支持 by_key")
        return ModeDecision(mode, mode, strategy.value)


__all__ = ["ModeDecision", "ModePolicy"]
