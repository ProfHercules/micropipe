import pytest

from micropipe import Pipeline


def test_pipeline_no_stages():
    with pytest.raises(ValueError):
        _ = Pipeline()
