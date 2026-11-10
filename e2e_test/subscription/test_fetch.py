"""Subscription regressions against a private RisingWave process.

Run: python e2e_test/subscription/test_fetch.py --binary <risingwave executable>
Only Python's standard library is required. No existing server or data is used.
"""
import argparse
import contextlib
import os
import pathlib
import socket
import struct
import subprocess
import tempfile
import time
import unittest


def i16(n):
    return struct.pack("!h", n)


def i32(n):
    return struct.pack("!i", n)


class Pg:
    def __init__(self, port):
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=20)
        body = i32(196608) + b"user\0root\0database\0dev\0\0"
        self.socket.sendall(i32(len(body) + 4) + body)
        self.read_result()

    def close(self):
        self.socket.close()

    def read_exact(self, n):
        data = b""
        while len(data) < n:
            part = self.socket.recv(n - len(data))
            if not part:
                raise ConnectionError("RisingWave closed the connection")
            data += part
        return data

    def send(self, kind, body):
        self.socket.sendall(kind + i32(len(body) + 4) + body)

    def read_result(self):
        fields, rows, error = [], [], None
        while True:
            kind = self.read_exact(1)
            body = self.read_exact(struct.unpack("!i", self.read_exact(4))[0] - 4)
            if kind == b"E":
                error = body.replace(b"\0", b" ").decode(errors="replace")
            elif kind == b"T":
                count = struct.unpack_from("!h", body)[0]
                offset = 2
                fields = []
                for _ in range(count):
                    end = body.index(0, offset)
                    name = body[offset:end].decode()
                    table, attr, oid, size, modifier, fmt = struct.unpack_from(
                        "!ihihih", body, end + 1
                    )
                    fields.append((name, oid, fmt))
                    offset = end + 19
            elif kind == b"D":
                count = struct.unpack_from("!h", body)[0]
                offset, row = 2, []
                for _ in range(count):
                    size = struct.unpack_from("!i", body, offset)[0]
                    offset += 4
                    row.append(None if size == -1 else body[offset:offset + size])
                    if size != -1:
                        offset += size
                rows.append(row)
            elif kind == b"Z":
                if error:
                    raise RuntimeError(error)
                return fields, rows

    def query(self, sql):
        self.send(b"Q", sql.encode() + b"\0")
        return self.read_result()

    def fetch(self, formats):
        self.send(b"P", b"\0FETCH 100 FROM cur\0" + i16(0))
        self.send(b"B", b"\0\0" + i16(0) + i16(0) +
                  i16(len(formats)) + b"".join(i16(f) for f in formats))
        self.send(b"D", b"P\0")
        self.send(b"E", b"\0" + i32(0))
        self.send(b"S", b"")
        return self.read_result()


@contextlib.contextmanager
def server(binary):
    with tempfile.TemporaryDirectory(prefix="rw-fetch-") as directory:
        sockets = [socket.socket() for _ in range(5)]
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        ports = [listener.getsockname()[1] for listener in sockets]
        meta, compute, sql, rpc, webhook = ["127.0.0.1:%d" % p for p in ports]
        config = pathlib.Path(directory) / "config.toml"
        config.write_text("[server]\ntelemetry_enabled = false\n")
        log_path = pathlib.Path(directory) / "server.log"
        env = {k: v for k, v in os.environ.items()
               if not k.startswith("RW_") and k != "RISEDEV"}
        env["RUST_LOG"] = "warn"
        args = [
            binary, "standalone", "--config-path", str(config),
            "--prometheus-listener-addr", "127.0.0.1:0",
            f"--meta-opts=--listen-addr {meta} --advertise-addr {meta} "
            "--backend mem --state-store hummock+memory --data-directory test "
            "--dangerous-max-idle-secs 120",
            f"--compute-opts=--listen-addr {compute} --meta-address http://{meta} "
            "--total-memory-bytes 2147483648 --memory-manager-target-bytes 2147483648 "
            "--parallelism 1",
            f"--frontend-opts=--listen-addr {sql} --meta-addr http://{meta} "
            f"--frontend-rpc-listener-addr {rpc} --webhook-listen-addr {webhook} "
            "--frontend-total-memory-bytes 268435456",
        ]
        for listener in sockets:
            listener.close()
        with log_path.open("w") as log:
            process = subprocess.Popen(args, cwd=directory, env=env,
                                       stdin=subprocess.DEVNULL, stdout=log, stderr=log)
            client = None
            try:
                deadline = time.monotonic() + 60
                while client is None:
                    if process.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError("Private server failed to start")
                    try:
                        client = Pg(ports[2])
                    except (OSError, ConnectionError):
                        time.sleep(0.1)
                client.query("CREATE TABLE ready (id INT)")
                while True:
                    try:
                        client.query("SELECT * FROM ready")
                        break
                    except RuntimeError as error:
                        if ("No worker node" not in str(error) and
                                "Empty workers found" not in str(error)):
                            raise
                        if time.monotonic() > deadline:
                            raise
                        time.sleep(0.1)
                yield client
            except Exception:
                log.flush()
                print(log_path.read_text(errors="replace")[-12000:])
                raise
            finally:
                if client is not None:
                    client.close()
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=15)


class FetchTests(unittest.TestCase):
    binary = None

    def check_formats(self, hidden_pk, formats):
        with server(self.binary) as client:
            pk = "" if hidden_pk else " PRIMARY KEY"
            client.query(f"CREATE TABLE t (id INT{pk}, v INT)")
            client.query("INSERT INTO t VALUES (1, 10); FLUSH")
            client.query("CREATE SUBSCRIPTION sub FROM t WITH (retention = '1h')")
            client.query("DECLARE cur SUBSCRIPTION CURSOR FOR sub FULL")
            fields, rows = client.fetch(formats)
            self.assertEqual([f[0] for f in fields], ["id", "v", "op", "rw_timestamp"])
            self.assertEqual(len(rows), 1)
            expanded = formats if len(formats) > 1 else (formats or [0]) * 4
            self.assertEqual([f[2] for f in fields], expanded)
            self.assertEqual(rows[0][0], i32(1) if expanded[0] else b"1")
            self.assertEqual(rows[0][1], i32(10) if expanded[1] else b"10")
            self.assertEqual(rows[0][2:], [b"Insert", None])
            client.query("INSERT INTO t VALUES (2, 20); FLUSH")
            fields, rows = client.fetch(formats)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], i32(2) if expanded[0] else b"2")
            self.assertEqual(rows[0][1], i32(20) if expanded[1] else b"20")
            self.assertEqual(rows[0][2], b"Insert")
            timestamp = (struct.unpack("!q", rows[0][3])[0] if expanded[3]
                         else int(rows[0][3]))
            self.assertGreater(timestamp, 0)

    def test_all_binary(self):
        self.check_formats(False, [1])

    def test_per_column(self):
        self.check_formats(False, [1, 0, 0, 1])

    def test_hidden_pk_per_column(self):
        self.check_formats(True, [0, 1, 0, 1])

    def test_hidden_pk_all_binary(self):
        self.check_formats(True, [1])

    def test_default_text(self):
        self.check_formats(True, [])

    def test_schema_boundary(self):
        with server(self.binary) as client:
            client.query("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            client.query("INSERT INTO t VALUES (1, 10), (2, 20); FLUSH")
            client.query("CREATE SUBSCRIPTION sub FROM t WITH (retention = '1h')")
            client.query("DECLARE cur SUBSCRIPTION CURSOR FOR sub FULL")
            client.query("ALTER TABLE t ADD COLUMN extra INT")
            client.query("INSERT INTO t VALUES (3, 30, 300); FLUSH")
            fields, rows = client.query("FETCH 100 FROM cur")
            self.assertEqual([f[0] for f in fields], ["id", "v", "op", "rw_timestamp"])
            self.assertEqual(rows, [[b"1", b"10", b"Insert", None],
                                    [b"2", b"20", b"Insert", None]])
            fields, rows = client.query("FETCH 100 FROM cur")
            self.assertEqual([f[0] for f in fields],
                             ["id", "v", "extra", "op", "rw_timestamp"])
            self.assertEqual([row[:4] for row in rows],
                             [[b"3", b"30", b"300", b"Insert"]])
            self.assertIsNotNone(rows[0][4])


    def test_explicit_all_text(self):
        self.check_formats(False, [0])

    def test_per_column_all_binary(self):
        self.check_formats(True, [1, 1, 1, 1])

    def test_per_column_all_text(self):
        self.check_formats(True, [0, 0, 0, 0])

    def test_schema_boundary_per_column(self):
        with server(self.binary) as client:
            client.query("CREATE TABLE t (id INT, v INT)")
            client.query("INSERT INTO t VALUES (1, 10), (2, 20); FLUSH")
            client.query("CREATE SUBSCRIPTION sub FROM t WITH (retention = '1h')")
            client.query("DECLARE cur SUBSCRIPTION CURSOR FOR sub FULL")
            client.query("ALTER TABLE t ADD COLUMN extra INT")
            client.query("INSERT INTO t VALUES (3, 30, 300); FLUSH")
            fields, rows = client.fetch([1, 0, 0, 1])
            self.assertEqual([f[0] for f in fields], ["id", "v", "op", "rw_timestamp"])
            self.assertEqual(rows, [[i32(1), b"10", b"Insert", None],
                                    [i32(2), b"20", b"Insert", None]])
            fields, rows = client.fetch([1, 0, 1, 0, 1])
            self.assertEqual([f[0] for f in fields],
                             ["id", "v", "extra", "op", "rw_timestamp"])
            self.assertEqual([row[:4] for row in rows],
                             [[i32(3), b"30", i32(300), b"Insert"]])
            self.assertGreater(struct.unpack("!q", rows[0][4])[0], 0)

    def test_drop_column_with_buffered_log_rows(self):
        with server(self.binary) as client:
            client.query("CREATE TABLE t (id INT PRIMARY KEY, v INT, extra INT)")
            client.query("CREATE SUBSCRIPTION sub FROM t WITH (retention = '1h')")
            client.query("DECLARE cur SUBSCRIPTION CURSOR FOR sub FULL")
            client.query("FETCH 100 FROM cur")
            client.query("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200); FLUSH")
            _, rows = client.query("FETCH 1 FROM cur")
            self.assertEqual(rows[0][:4], [b"1", b"10", b"100", b"Insert"])
            client.query("ALTER TABLE t DROP COLUMN v")
            client.query("INSERT INTO t VALUES (3, 300); FLUSH")
            fields, rows = client.query("FETCH 100 FROM cur WITH (timeout = '5s')")
            self.assertEqual([f[0] for f in fields],
                             ["id", "v", "extra", "op", "rw_timestamp"])
            self.assertEqual([row[:4] for row in rows],
                             [[b"2", b"20", b"200", b"Insert"]])
            fields, rows = client.query("FETCH 100 FROM cur")
            self.assertEqual([f[0] for f in fields],
                             ["id", "extra", "op", "rw_timestamp"])
            self.assertEqual([row[:3] for row in rows], [[b"3", b"300", b"Insert"]])

    def test_empty_schema_boundary_with_timeout(self):
        with server(self.binary) as client:
            client.query("CREATE TABLE t (id INT)")
            client.query("CREATE SUBSCRIPTION sub FROM t WITH (retention = '1h')")
            client.query("DECLARE cur SUBSCRIPTION CURSOR FOR sub FULL")
            client.query("ALTER TABLE t ADD COLUMN v INT")
            client.query("INSERT INTO t VALUES (1, 10); FLUSH")
            fields, rows = client.query("FETCH 100 FROM cur WITH (timeout = '5s')")
            self.assertEqual(rows, [])
            self.assertEqual([f[0] for f in fields], ["id", "v", "op", "rw_timestamp"])
            _, rows = client.query("FETCH 100 FROM cur")
            self.assertEqual([row[:3] for row in rows], [[b"1", b"10", b"Insert"]])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    args, remaining = parser.parse_known_args()
    FetchTests.binary = str(pathlib.Path(args.binary).resolve(strict=True))
    unittest.main(argv=[__file__] + remaining)