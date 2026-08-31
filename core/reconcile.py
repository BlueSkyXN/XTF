"""Target-neutral key reconciliation used before target-specific compilation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Generic, Iterable, Mapping, TypeVar

S = TypeVar("S")
T = TypeVar("T")


@dataclass(frozen=True)
class Reconciliation(Generic[S, T]):
    matched: tuple[tuple[str, S, T], ...]
    missing: tuple[S, ...]
    target_only: tuple[tuple[str, T], ...]


class Reconciler:
    @staticmethod
    def by_key(
        source_items: Iterable[S],
        target_index: Mapping[str, T],
        *,
        source_key: Callable[[S], str | None],
    ) -> Reconciliation[S, T]:
        matched: list[tuple[str, S, T]] = []
        missing: list[S] = []
        seen: set[str] = set()
        for item in source_items:
            key = source_key(item)
            if key is not None and key in target_index:
                matched.append((key, item, target_index[key]))
                seen.add(key)
            else:
                missing.append(item)
        target_only = tuple(
            (key, item) for key, item in target_index.items() if key not in seen
        )
        return Reconciliation(tuple(matched), tuple(missing), target_only)


__all__ = ["Reconciliation", "Reconciler"]
