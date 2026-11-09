// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg(windows)]

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

#[test]
fn binary_starts_and_displays_help() {
    let cases: &[(&[&str], &str)] = &[
        (&["--version"], "risingwave"),
        (&["single-node", "--help"], "--store-directory"),
    ];
    for (args, expected) in cases {
        let output = Command::new(env!("CARGO_BIN_EXE_risingwave"))
            .args(*args)
            .output()
            .expect("failed to start the native RisingWave executable");
        assert!(
            output.status.success(),
            "risingwave {args:?} failed: {:?}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains(expected), "unexpected output: {stdout}");
    }
}

struct TestServer(Child);

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn standalone_executes_streaming_sql() {
    // Reserve distinct loopback ports so this test can coexist with local installations.
    let listeners: Vec<_> = (0..5)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect();
    let (meta, compute, sql, frontend_rpc, webhook) = (
        addresses[0], addresses[1], addresses[2], addresses[3], addresses[4],
    );
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("config.toml"),
        "[server]\ntelemetry_enabled = false\n",
    )
    .unwrap();
    let log_path = directory.path().join("risingwave.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_risingwave"));
    // Use only this test's settings, including its own storage and secret directories.
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("RW_") || name == "RISEDEV" {
            command.env_remove(name);
        }
    }
    command
        .current_dir(directory.path())
        .env("RUST_LOG", "warn")
        .args(["standalone", "--config-path", "config.toml"])
        .args(["--prometheus-listener-addr", "127.0.0.1:0"])
        .arg(format!(
            "--meta-opts=--listen-addr {meta} --advertise-addr {meta} --backend mem \
             --state-store hummock+memory --data-directory smoke \
             --dangerous-max-idle-secs 120"
        ))
        .arg(format!(
            "--compute-opts=--listen-addr {compute} --meta-address http://{meta} \
             --total-memory-bytes 2147483648 --memory-manager-target-bytes 2147483648 \
             --parallelism 1"
        ))
        .arg(format!(
            "--frontend-opts=--listen-addr {sql} --meta-addr http://{meta} \
             --frontend-rpc-listener-addr {frontend_rpc} \
             --webhook-listen-addr {webhook} --frontend-total-memory-bytes 268435456"
        ))
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    drop(listeners);
    let mut server = TestServer(command.spawn().expect("failed to start RisingWave"));

    let result = tokio::time::timeout(Duration::from_secs(120), async {
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(sql.port())
            .user("root")
            .dbname("dev")
            .connect_timeout(Duration::from_secs(2));
        let (client, connection) = loop {
            if let Some(status) = server.0.try_wait()? {
                return Err(format!("RisingWave exited during startup: {status}").into());
            }
            if let Ok(connection) = config.connect(tokio_postgres::NoTls).await {
                break connection;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        let connection = tokio::spawn(connection);
        client
            .batch_execute(
                "CREATE TABLE windows_smoke (id INT PRIMARY KEY, v INT);
                 CREATE MATERIALIZED VIEW windows_total AS
                     SELECT SUM(v) AS total FROM windows_smoke;
                 INSERT INTO windows_smoke VALUES (1, 10), (2, 20);
                 FLUSH;",
            )
            .await?;
        let row = client.query_one("SELECT total FROM windows_total", &[]).await?;
        assert_eq!(row.get::<_, i64>(0), 30);

        client
            .batch_execute(
                "UPDATE windows_smoke SET v = 15 WHERE id = 1;
                 DELETE FROM windows_smoke WHERE id = 2;
                 FLUSH;",
            )
            .await?;
        let row = client.query_one("SELECT total FROM windows_total", &[]).await?;
        assert_eq!(row.get::<_, i64>(0), 15);
        drop(client);
        connection.await??;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await;
    drop(server);
    assert!(
        matches!(result, Ok(Ok(()))),
        "native SQL smoke test failed: {result:?}\n{}",
        std::fs::read_to_string(log_path).unwrap_or_default()
    );
}
