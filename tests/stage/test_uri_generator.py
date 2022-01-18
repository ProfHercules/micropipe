import logging

from micropipe import FlowGeneratorStage, Pipeline, UrlGeneratorStage


def test_uri_generator():
    ids = list(range(1, 11))

    stages = [
        FlowGeneratorStage(iterator=ids),
        UrlGeneratorStage(
            template_url="https://jsonplaceholder.typicode.com/users/{id}",
            params=lambda id: {"id": str(id.value)},
        ),
    ]
    pipeline = Pipeline(stages=stages, logger=logging.getLogger())
    result = pipeline.flow_sync()
    urls = list(map(lambda r: r.value, result))

    assert len(urls) == 10

    for id in ids:
        _ = urls.index(f"https://jsonplaceholder.typicode.com/users/{id}")
