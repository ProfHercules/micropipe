import pytest

from micropipe import RateLimit


def test_rate_limit():
    _ = RateLimit(max_per_sec=1)
    # how do we actually test that this is working?


def test_rate_limit_not_zero():
    with pytest.raises(AssertionError):
        _ = RateLimit(max_per_sec=0)
