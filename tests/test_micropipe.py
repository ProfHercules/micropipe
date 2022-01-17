import pytest
from micropipe import Pipeline
from micropipe.stages import *


def test_micropipe_no_stages():
    with pytest.raises(AssertionError):
        stages = []
        pipeline = Pipeline(stages=stages, logger=logging.getLogger())
        pipeline.flow_sync()
