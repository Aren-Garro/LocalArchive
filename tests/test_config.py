"""Tests for configuration validation and defaults."""

import pytest

from localarchive.config import Config, ExtractionConfig


def test_config_validation_rejects_invalid_strategy():
    cfg = Config(extraction=ExtractionConfig(strategy="invalid"))
    with pytest.raises(ValueError):
        cfg.validate()


def test_config_validation_accepts_defaults():
    cfg = Config()
    cfg.validate()
