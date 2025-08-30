import contextlib
import csv
import dataclasses
import datetime
import functools
import logging
import os
import time
from collections.abc import Iterator
from typing import Any

import elasticsearch


LOG = logging.getLogger(__name__)

API_KEY: str | None = os.getenv("ESPROBER_API_KEY", "").strip() or None
API_URL: str = (os.getenv("ESPROBER_API_URL", "").strip() or "https://overview-elastic-cloud-com.es.us-east-1.aws.found.io:443").rstrip("/")

CSV_FILENAME = os.path.expanduser(os.getenv("ESPROBER_CSV_FILENAME", "esprober.log"))
LOG_FILENAME = os.path.expanduser(os.getenv("ESPROBER_LOG_FILENAME", "esprober.log"))


@dataclasses.dataclass
class Query:
    name: str
    path: str
    body: dict[str, Any]


QUERIES: list[Query] = [
    Query(
        name="service.node.name-wildcard",
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
   Query(
        name= "service.node.name-term",
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
    Query(
        name="kubernetes.pod.name-wildcard",
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
    Query(
        name="kubernetes.pod.name-term",
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
]


QUERY_INTERVAL: float = max(1., float(os.getenv("ESPROBER_QUERY_INTERVAL", "").strip() or 60.))
TEST_DURATION: float | None = max(0., float(os.getenv("ESPROBER_TEST_DURATION", "").strip() or 0.)) or None
REQUEST_TIMEOUT: float = max(1., float(os.getenv("ESPROBER_REQUEST_TIMEOUT", "").strip() or 120.))

CLIENTS: dict[str, elasticsearch.Elasticsearch] = {}


def main():
    logging.basicConfig(filename=LOG_FILENAME, encoding='utf-8', level=logging.DEBUG)

    test_deadline: float | None = None
    if TEST_DURATION:
        test_deadline = time.monotonic() + TEST_DURATION

    durations: dict[str, list[float]] = {}
    for q in QUERIES:
        durations[q.name] = []

    with QueryResult.csv_writer() as writer:
        while not test_deadline or time.monotonic() < test_deadline:
            for query in QUERIES:
                # Send a query to elastic search service
                try:
                    LOG.info("Executing query '%s'...", query.name)
                    result = send_query(query)
                    # Write query results to CSV file
                    result.write_to(writer)

                    # Aggregate print query stats.
                    durations[query.name].append(result.duration)
                    LOG.info("Query '%s' average time: %f seconds", query.name, average(durations[query.name]))
                except Exception as ex:
                    LOG.exception("Query '%s' failed: %s", query.name, ex)
                # Give the service a fair break to reduce its charge
                LOG.debug("Sleeping %d seconds...", int(QUERY_INTERVAL))
                time.sleep(QUERY_INTERVAL)


class DictWriter(csv.DictWriter):

    def __init__(self, f, fieldnames, *args, **kwargs):
        super().__init__(f, fieldnames, *args, **kwargs)
        self.flush = f.flush

    def writerow(self, row):
        super().writerow(row)
        self.flush()


@dataclasses.dataclass
class QueryResult:
    timestamp: str
    name: str
    duration: float

    @classmethod
    @contextlib.contextmanager
    def csv_writer(cls) -> Iterator[csv.writer]:
        out_dir = os.path.dirname(CSV_FILENAME)
        if out_dir and not os.path.isdir(out_dir):
            os.makedirs(out_dir)
        write_header = not os.path.isfile(CSV_FILENAME)
        with open(CSV_FILENAME, "a", newline="") as csv_file:
            csv_writer = DictWriter(csv_file, fieldnames=["timestamp", "name", "duration"])
            if write_header:
                csv_writer.writeheader()

            yield csv_writer
            csv_file.flush()

    def write_to(self, writer: csv.DictWriter) -> None:
        writer.writerow(dataclasses.asdict(self))


def send_query(query) -> QueryResult:
    timestamp = datetime.datetime.strftime(
        datetime.datetime.now(datetime.timezone.utc),
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3]
    start_time = time.monotonic()
    search(query)
    duration = time.monotonic() - start_time
    return QueryResult(timestamp=timestamp, name=query.name, duration=duration)


def average(durations: list[float]) -> float:
    if not durations:
        return 0.
    return sum(durations) / len(durations)


def search(query: Query) -> Any:
    url = f"{API_URL}/{query.path}".rstrip("/")
    return client(url).search(**query.body)


@functools.cache
def client(url: str) -> elasticsearch.Elasticsearch:
    c = elasticsearch.Elasticsearch(url).options(api_key=API_KEY, request_timeout=REQUEST_TIMEOUT)
    if API_KEY:
        c = c.options(api_key=API_KEY)
    return c


if __name__ == "__main__":
    main()
