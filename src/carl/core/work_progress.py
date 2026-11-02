"""Exact continuation subscriptions, separate from all-terminal dependencies."""

import hashlib
from collections.abc import Mapping
from functools import cached_property
from typing import Self

from pydantic import Field, field_validator, model_validator

from carl.core.json import encode_json
from carl.core.models import StrictModel


class WorkProgressWatch(StrictModel):
    consumer_requester_kinds: tuple[tuple[str, ...], ...] = Field(default=(), max_length=16)
    # Large retained fan-ins are normalized in bounded storage batches, never
    # silently truncated or serialized into a stage's small payload.
    work_identifiers: tuple[str, ...] = ()

    @field_validator("consumer_requester_kinds")
    @classmethod
    def validate_kinds(cls, value: tuple[tuple[str, ...], ...]) -> tuple[tuple[str, ...], ...]:
        if any(not kind or any(not part for part in kind) for kind in value):
            raise ValueError("Progress consumer kinds require nonempty exact parts")
        return value

    @field_validator("work_identifiers")
    @classmethod
    def validate_identifiers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not identifier for identifier in value):
            raise ValueError("Progress work identifiers must be nonempty")
        return value

    @property
    def empty(self) -> bool:
        return not self.consumer_requester_kinds and not self.work_identifiers


class ProgressWait(StrictModel):
    observed_through_revision: int = Field(ge=0)
    watch: WorkProgressWatch

    def model_copy(self, *, update: Mapping[str, object] | None = None, deep: bool = False) -> Self:
        copied = super().model_copy(update=update, deep=deep)
        if "fingerprint" in copied.__dict__:
            object.__delattr__(copied, "fingerprint")
        return copied

    @cached_property
    def fingerprint(self) -> str:
        digest = hashlib.sha256(
            encode_json(
                [
                    self.observed_through_revision,
                    [list(kind) for kind in self.watch.consumer_requester_kinds],
                ]
            ).encode()
        )
        for identifier in self.watch.work_identifiers:
            value = identifier.encode()
            digest.update(len(value).to_bytes(8, "big"))
            digest.update(value)
        return digest.hexdigest()

    @model_validator(mode="after")
    def validate_watch(self) -> "ProgressWait":
        if self.watch.empty:
            raise ValueError("A progress wait requires at least one exact input")
        return self


class ProgressDrainResult(StrictModel):
    more: bool
    woke: int = Field(ge=0)


class ProgressReconcilePage(StrictModel):
    next_cursor: str | None
    more: bool
    woke: int = Field(ge=0)
