import dataclasses
import functools
import logging
import os
import time
from typing import Any

import elasticsearch


LOG = logging.getLogger(__name__)

API_KEY: str | None = os.getenv("ESPROBER_API_KEY", "").strip() or None
API_URL: str = (os.getenv("ESPROBER_API_URL", "").strip() or "https://overview-elastic-cloud-com.es.us-east-1.aws.found.io:443").rstrip("/")


@dataclasses.dataclass
class Query:
    path: str
    body: dict[str, Any]


QUERIES: dict[str, Query] = {
    "service.node.name-wildcard": Query(
        path="serverless-metrics-*:apm-*,serverless-metrics-*:metrics-apm*",
        body={
            "query": {
                "wildcard": {
                    "service.node.name": {
                        "value": "es-es-search*"
                    }
                }
            }
        },
    ),
    "service.node.name-term": Query(
        path="serverless-metrics-*:apm-*,serverless-metrics-*:metrics-apm*",
        body={
            "query": {
                "term": {
                    "service.node.name": {
                        "value": "es-es-search-7c46b56686-sdtrl"
                    }
                }
            }
        }
    ),
    "kubernetes.pod.name-wildcard": Query(
        path="metrics-*,serverless-metrics-*:metrics-*",
        body={
            "query": {
                "wildcard": {
                    "kubernetes.pod.name": {
                        "value": "es-*"
                    }
                }
            }
        },
    ),
    "kubernetes.pod.name-term": Query(
        path="metrics-*,serverless-metrics-*:metrics-*",
        body={
            "query": {
                "term": {
                    "kubernetes.pod.name": {
                        "value": "es-es-index-564b5c6d45-7hldp"
                    }
                }
            }
        },
    ),
}


QUERY_INTERVAL: float = max(1., float(os.getenv("ESPROBER_QUERY_INTERVAL", "").strip() or 60.))
TEST_DURATION: float | None = max(0., float(os.getenv("ESPROBER_TEST_DURATION", "").strip() or 0.)) or None
REQUEST_TIMEOUT: float = max(1., float(os.getenv("ESPROBER_REQUEST_TIMEOUT", "").strip() or 120.))

CLIENTS: dict[str, elasticsearch.Elasticsearch] = {}

def main():
    logging.basicConfig(level=logging.INFO)

    test_deadline: float | None = None
    if TEST_DURATION:
        test_deadline = time.monotonic() + TEST_DURATION

    query_names = list(QUERIES)
    durations: dict[str, list[float]] = {}
    for n in query_names:
        durations[n] = []

    # body = None
    while not test_deadline or time.monotonic() < test_deadline:
        for i in range(len(query_names)):
            name = query_names[i % len(query_names)]
            query = QUERIES[name]

            start_time = time.monotonic()
            search(query)
            durations[name].append(time.monotonic() - start_time)
            LOG.info("Query '%s' average time: %f seconds", name, average(durations[name]))
            time.sleep(QUERY_INTERVAL)


def average(durations: list[float]) -> float:
    if not durations:
        return 0.
    return sum(durations) / len(durations)


def search(query: Query) -> Any:
    url = f"{API_URL}/{query.path}".rstrip("/")
    try:
        return client(url).search(**query.body)
    except elasticsearch.exceptions.ConnectionTimeout:
        LOG.error("Connection to %s timed out", API_URL)
        exit(1)


@functools.cache
def client(url: str) -> elasticsearch.Elasticsearch:
    c = elasticsearch.Elasticsearch(url).options(api_key=API_KEY, request_timeout=REQUEST_TIMEOUT)
    if API_KEY:
        c = c.options(api_key=API_KEY)
    return c


if __name__ == "__main__":
    main()
