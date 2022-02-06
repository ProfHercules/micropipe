import pytest

from micropipe.stages import RateLimit


def test_rate_limit():
    _ = RateLimit(max_per_sec=1)
    # TODO: how do we actually test that this is working?
    # seems like it'd be hard to have a deterministic test in this case.


def test_rate_limit_not_zero():
    with pytest.raises(ValueError):
        _ = RateLimit(max_per_sec=0)
