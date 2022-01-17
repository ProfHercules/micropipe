from micropipe import Pipeline
from micropipe.stages import *


def test_uri_generator():
    ids = list(range(1, 11))

    stages = [
        FlowGenerator(iterator=ids),
        UrlGenerator(
            template_url="https://jsonplaceholder.typicode.com/users/{id}",
            params=lambda id: {"id": str(id.value)},
        ),
    ]
    pipeline = Pipeline(stages=stages, logger=logging.getLogger())
    result = pipeline.flow_sync()
    urls = list(map(lambda r: r.value, result))

    assert len(urls) == 10

    for id in ids:
        idx = urls.index(f"https://jsonplaceholder.typicode.com/users/{id}")
