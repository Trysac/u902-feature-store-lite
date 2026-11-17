"""Behavior tests for the public API of feature_store_lite."""

from __future__ import annotations

import pytest

import feature_store_lite


def test_normalize_trims_and_lowercases() -> None:
    assert feature_store_lite.normalize("  Hello World ") == "hello world"


@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_normalize_rejects_blank_input(value: str) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        feature_store_lite.normalize(value)


def test_version_is_semver() -> None:
    parts = feature_store_lite.__version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)
