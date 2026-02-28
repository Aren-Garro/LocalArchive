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


def test_config_validation_rejects_invalid_fuzzy_threshold():
    cfg = Config()
    cfg.search.fuzzy_threshold = 1.5
    with pytest.raises(ValueError):
        cfg.validate()


def test_config_validation_rejects_invalid_processing_limits():
    cfg = Config()
    cfg.processing.max_errors_per_run = 0
    with pytest.raises(ValueError):
        cfg.validate()


def test_config_validation_rejects_invalid_ui_language():
    cfg = Config()
    cfg.ui.language = "english"
    with pytest.raises(ValueError):
        cfg.validate()
