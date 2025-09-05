import csv
import dataclasses
import datetime
import functools
import json
import logging
import os
import time
from collections.abc import Iterator, Iterable
from typing import Any

import elasticsearch


LOG = logging.getLogger(__name__)

API_KEY: str | None = os.getenv("ESPROBER_API_KEY", "").strip() or None
API_URL: str = (os.getenv("ESPROBER_API_URL", "").strip() or "https://overview-elastic-cloud-com.es.us-east-1.aws.found.io:443").rstrip("/")

LOG_FILENAME = os.path.expanduser(os.getenv("ESPROBER_LOG_FILENAME", "esprober.log"))
QUERIES_FILENAME = os.path.expanduser(os.getenv("ESPROBER_QUERIES_FILENAME", "queries.json"))
RESULTS_FILENAME = os.path.expanduser(os.getenv("ESPROBER_CSV_FILENAME", "results.csv"))

QUERY_INTERVAL: float = max(1., float(os.getenv("ESPROBER_QUERY_INTERVAL", "").strip() or 60.))
TEST_DURATION: float | None = max(0., float(os.getenv("ESPROBER_TEST_DURATION", "").strip() or 0.)) or None
REQUEST_TIMEOUT: float = max(1., float(os.getenv("ESPROBER_REQUEST_TIMEOUT", "").strip() or 120.))


@dataclasses.dataclass
class Query:
    name: str
    path: str
    body: dict[str, Any]

    def send(self) -> 'QueryResult':
        url = f"{API_URL}/{self.path}".rstrip("/")
        timestamp = datetime.datetime.strftime(
            datetime.datetime.now(datetime.timezone.utc),
            "%Y-%m-%dT%H:%M:%S.%f"
        )[:-3]
        start_time = time.monotonic()
        client(url).search(**self.body)
        duration = time.monotonic() - start_time
        return QueryResult(timestamp=timestamp, duration=duration, name=self.name)


@dataclasses.dataclass
class QueryResult:
    timestamp: str
    name: str
    duration: float


def main(
    log_filename: str = LOG_FILENAME,
    queries_filename: str = QUERIES_FILENAME,
    results_filename: str = RESULTS_FILENAME
) -> None:
    init_logging(log_filename)

    queries = load_queries(queries_filename)

    durations: dict[str, list[float]] = {q.name: [] for q in queries}
    for result in read_results(results_filename):
        durations[result.name].append(result.duration)
    for q in queries:
        LOG.info("Query '%s' average time: %f seconds", q.name, average(durations[q.name]))

    LOG.debug(f"Start sending queries...")
    try:
        results = send_queries(queries=queries, durations=durations)
        write_results(results_filename, results)
    finally:
        LOG.debug(f"Terminated sending queries.")
        for q in queries:
            LOG.info("Query '%s' average time: %f seconds", q.name, average(durations[q.name]))


def init_logging(filename: str = LOG_FILENAME) -> None:
    logging.basicConfig(
        filename=filename,
        encoding='utf-8',
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_queries(filename: str) -> list[Query]:
    LOG.debug("Loading queries from '%s'.", filename)
    try:
        with open(filename) as f:
            return [Query(**d) for d in json.load(f)]
    finally:
        LOG.debug("Terminated loading queries from '%s'.", filename)


def send_queries(
    queries: Iterable[Query],
    durations: dict[str, list[float]],
    interval: float=QUERY_INTERVAL
) -> Iterator[QueryResult]:
    test_deadline: float | None = None
    if TEST_DURATION:
        test_deadline = time.monotonic() + TEST_DURATION

    for query in queries:
        if test_deadline and time.monotonic() < test_deadline:
            LOG.warning("Test duration expired.", query.name)
            break

        LOG.info("Sending query '%s'...", query.name)
        try:
            result = query.send()
        except Exception as ex:
            LOG.exception("Query '%s' failed: %s", query.name, ex)
        else:
            durations[query.name].append(result.duration)
            LOG.info("Query '%s' average time: %f seconds", query.name, average(durations[query.name]))
            yield result
        finally:
            if interval > 0:
                # Give the service a fair break to reduce its charge
                LOG.debug("Sleeping %d seconds...", int(QUERY_INTERVAL))
                time.sleep(QUERY_INTERVAL)


def read_results(filename) -> Iterator[QueryResult]:
    if not os.path.isfile(filename):
        return
    LOG.debug("Reading results from '%s'.", filename)
    try:
        with open(filename, "r", newline="") as csv_file:
            for row in csv.DictReader(csv_file):
                yield QueryResult(
                    timestamp=row["timestamp"],
                    name=row["name"],
                    duration=float(row["duration"]),
                )
    finally:
        LOG.debug("Terminated reading results from '%s'.", filename)


def write_results(filename: str, results: Iterable['QueryResult']) -> None:
    out_dir = os.path.dirname(filename)
    if out_dir and not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    write_header = not os.path.isfile(filename)
    LOG.debug("Start writing results to '%s'...", filename)
    try:
        with open(filename, "a", newline="") as fd:
            writer = csv.DictWriter(fd, fieldnames=["timestamp", "name", "duration"])
            if write_header:
                LOG.debug("Writing header to '%s'...", filename)
                writer.writeheader()
            for r in results:
                LOG.debug("Writing result to '%s' (result: %r)...", filename, r)
                writer.writerow(dataclasses.asdict(r))
                fd.flush()
    finally:
        LOG.debug("Terminated writing results to '%s'.", filename)


def average(durations: list[float]) -> float:
    if not durations:
        return 0.
    return sum(durations) / len(durations)


@functools.cache
def client(url: str) -> elasticsearch.Elasticsearch:
    c = elasticsearch.Elasticsearch(url).options(api_key=API_KEY, request_timeout=REQUEST_TIMEOUT)
    if API_KEY:
        c = c.options(api_key=API_KEY)
    return c


if __name__ == "__main__":
    main()
