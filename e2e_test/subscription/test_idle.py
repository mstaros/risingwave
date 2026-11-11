"""Idle subscription cursor regressions against a private RisingWave process.

Run: python e2e_test/subscription/test_idle.py --binary <risingwave executable>
Only Python's standard library is required. No existing server or data is used.
"""
import argparse
import pathlib
import re
import time
import unittest
import urllib.request

from test_fetch import server

RETENTION_SECONDS = 6


def change_log_lookups(client):
    """Number of change-log lookups the meta node has served so far."""
    url = "http://127.0.0.1:%d/metrics" % client.metrics_port
    with urllib.request.urlopen(url, timeout=10) as response:
        text = response.read().decode(errors="replace")
    match = re.search(
        r"^storage_table_change_log_get_latency_count(?:\{[^}]*\})? (\S+)",
        text, re.MULTILINE)
    if match is None:
        raise AssertionError("change-log lookup metric is not exported")
    return int(float(match.group(1)))


class IdleCursorTests(unittest.TestCase):
    binary = None

    def declare_caught_up(self, client):
        client.query("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        client.query("CREATE SUBSCRIPTION sub FROM t WITH (retention = '%ds')"
                     % RETENTION_SECONDS)
        client.query("DECLARE cur SUBSCRIPTION CURSOR FOR sub FULL")
        _, rows = client.query("FETCH 100 FROM cur")
        self.assertEqual(rows, [])

    def test_quiet_cursor_outlives_retention(self):
        with server(self.binary) as client:
            self.declare_caught_up(client)
            deadline = time.monotonic() + RETENTION_SECONDS + 4
            while time.monotonic() < deadline:
                started = time.monotonic()
                _, rows = client.query("FETCH 100 FROM cur WITH (timeout = '2s')")
                elapsed = time.monotonic() - started
                self.assertEqual(rows, [])
                self.assertGreater(elapsed, 1.5)
                self.assertLess(elapsed, 4.5)
            client.query("INSERT INTO t VALUES (1, 10); FLUSH")
            _, rows = client.query("FETCH 100 FROM cur WITH (timeout = '5s')")
            self.assertEqual([row[:3] for row in rows], [[b"1", b"10", b"Insert"]])
            self.assertIsNotNone(rows[0][3])

    def test_unfetched_cursor_still_expires(self):
        with server(self.binary) as client:
            self.declare_caught_up(client)
            time.sleep(RETENTION_SECONDS + 2)
            with self.assertRaisesRegex(RuntimeError, "maximum lifetime"):
                client.query("FETCH 100 FROM cur")

    def test_idle_fetch_does_not_repeat_lookups(self):
        with server(self.binary) as client:
            self.declare_caught_up(client)
            before = change_log_lookups(client)
            _, rows = client.query("FETCH 100 FROM cur WITH (timeout = '4s')")
            self.assertEqual(rows, [])
            lookups = change_log_lookups(client) - before
            # One lookup per committed checkpoint is expected. A cursor that repeats the
            # same lookup makes hundreds in this window.
            self.assertLess(lookups, 40)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    args, remaining = parser.parse_known_args()
    IdleCursorTests.binary = str(pathlib.Path(args.binary).resolve(strict=True))
    unittest.main(argv=[__file__] + remaining)