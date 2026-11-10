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

#![cfg(all(windows, not(feature = "java")))]

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio_postgres::{Client, NoTls};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct PostgresServer {
    server: Server,
    bin: PathBuf,
    data: PathBuf,
}

impl Drop for PostgresServer {
    fn drop(&mut self) {
        // This is exclusively the temporary cluster created by this test.
        let _ = Command::new(self.bin.join("pg_ctl.exe"))
            .args(["stop", "-m", "immediate", "-w", "-D"])
            .arg(&self.data)
            .output();
    }
}

fn start_postgres(bin: PathBuf, root: &Path, address: SocketAddr) -> Result<PostgresServer> {
    let data = root.join("pgdata");
    let output = Command::new(bin.join("initdb.exe"))
        .args([
            "-A",
            "trust",
            "-U",
            "postgres",
            "--no-locale",
            "--encoding=UTF8",
            "-D",
        ])
        .arg(&data)
        .output()?;
    if !output.status.success() {
        return Err(format!("initdb failed: {}", String::from_utf8_lossy(&output.stderr)).into());
    }
    let log = std::fs::File::create(root.join("postgres.log"))?;
    let server = Server(
        Command::new(bin.join("postgres.exe"))
            .arg("-D")
            .arg(&data)
            .args(["-h", "127.0.0.1", "-p", &address.port().to_string()])
            .args([
                "-c",
                "wal_level=replica",
                "-c",
                "max_wal_senders=4",
                "-c",
                "max_replication_slots=4",
            ])
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?,
    );
    Ok(PostgresServer { server, bin, data })
}

fn start_rw(root: &Path, addresses: &[SocketAddr]) -> Result<Server> {
    let [meta, compute, sql, frontend_rpc, webhook, compactor] = addresses else {
        return Err("expected six RisingWave addresses".into());
    };
    let meta_dir = root.join("meta_store");
    let state_dir = root.join("state_store");
    std::fs::create_dir_all(&meta_dir)?;
    std::fs::create_dir_all(&state_dir)?;
    let endpoint = meta_dir
        .join("native_cdc.db")
        .to_string_lossy()
        .replace('\\', "/");
    let state = state_dir.to_string_lossy().replace('\\', "/");
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("risingwave.log"))?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_risingwave"));
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("RW_") || key == "RISEDEV" {
            command.env_remove(key);
        }
    }
    command
        .current_dir(root)
        .env("RUST_LOG", "warn")
        .args(["standalone", "--config-path", "config.toml"])
        .args(["--prometheus-listener-addr", "127.0.0.1:0"])
        .arg(format!(
            "--meta-opts=--listen-addr {meta} --advertise-addr {meta} \
             --backend sqlite --sql-endpoint {endpoint:?} \
             --state-store hummock+fs://{state} --data-directory native_cdc \
             --dashboard-host 127.0.0.1:0 --dangerous-max-idle-secs 240"
        ))
        .arg(format!(
            "--compute-opts=--listen-addr {compute} --meta-address http://{meta} \
             --total-memory-bytes 2147483648 --memory-manager-target-bytes 2147483648 \
             --parallelism 1"
        ))
        .arg(format!(
            "--frontend-opts=--listen-addr {sql} --meta-addr http://{meta} \
             --frontend-rpc-listener-addr {frontend_rpc} --webhook-listen-addr {webhook} \
             --frontend-total-memory-bytes 268435456"
        ))
        .arg(format!(
            "--compactor-opts=--listen-addr {compactor} --meta-address http://{meta} \
             --compactor-total-memory-bytes 1073741824"
        ))
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    Ok(Server(command.spawn()?))
}

async fn connect(
    server: &mut Server,
    address: SocketAddr,
    user: &str,
    database: &str,
) -> Result<Client> {
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(address.port())
        .user(user)
        .dbname(database)
        .connect_timeout(Duration::from_secs(2));
    loop {
        if let Some(status) = server.0.try_wait()? {
            return Err(format!("server exited during startup: {status}").into());
        }
        if let Ok((client, connection)) = config.connect(NoTls).await {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            return Ok(client);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn totals(client: &Client) -> Result<(i64, i64)> {
    let row = client
        .query_one(
            "SELECT COUNT(*), COALESCE(SUM(v), 0) FROM native_orders",
            &[],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
}

async fn await_totals(
    client: &Client,
    expected: (i64, i64),
    previous: Option<(i64, i64)>,
) -> Result<()> {
    loop {
        let actual = totals(client).await?;
        if actual == expected {
            return Ok(());
        }
        if let Some(previous) = previous {
            if actual != previous {
                return Err(format!("part of a PostgreSQL transaction became visible: {actual:?}, expected {previous:?} or {expected:?}").into());
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Runs real PostgreSQL 19 and the packaged native RW engine on private ports.
/// No existing database, publication, replication slot or service is used.
#[tokio::test]
#[ignore = "requires PostgreSQL 19 binaries in RW_CDC_TEST_PG_BIN"]
async fn native_postgres_snapshot_transactions_toast_and_crash_recovery() {
    let bin =
        PathBuf::from(std::env::var_os("RW_CDC_TEST_PG_BIN").expect("set RW_CDC_TEST_PG_BIN"));
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("config.toml"),
        "[server]\ntelemetry_enabled = false\n",
    )
    .unwrap();
    let listeners: Vec<_> = (0..7)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: Vec<_> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
    drop(listeners);

    let result = tokio::time::timeout(Duration::from_secs(240), async {
        let mut postgres = start_postgres(bin, directory.path(), addresses[0])?;
        let mut pg = connect(&mut postgres.server, addresses[0], "postgres", "postgres").await?;
        let version: String = pg.query_one("SHOW server_version_num", &[]).await?.get(0);
        assert!(version.parse::<u32>()? >= 190000, "test requires PostgreSQL 19");
        let wal: String = pg.query_one("SHOW wal_level", &[]).await?.get(0);
        assert_eq!(wal, "replica");
        pg.batch_execute(
            "CREATE TABLE public.native_orders (id INT PRIMARY KEY, v INT, body TEXT, n NUMERIC(28,8));
             ALTER TABLE public.native_orders REPLICA IDENTITY FULL;
             ALTER TABLE public.native_orders ALTER COLUMN body SET STORAGE EXTERNAL;"
        ).await?;
        let body = "native CDC keeps unchanged TOAST values; ".repeat(1000);
        pg.execute(
            "INSERT INTO public.native_orders VALUES (1,10,$1,12345678901234567890.12345678),(2,20,'remove',2)",
            &[&body],
        ).await?;

        let mut rw = start_rw(directory.path(), &addresses[1..])?;
        let client = connect(&mut rw, addresses[3], "root", "dev").await?;
        client.batch_execute(&format!(
            "CREATE SOURCE native_pg WITH (
                connector='postgres-cdc', hostname='127.0.0.1', port='{}',
                username='postgres', password='', database.name='postgres',
                schema.name='public', slot.name='rw_native_test',
                publication.name='rw_native_test', ssl.mode='disabled',
                transactional='true'
             );",
            addresses[0].port()
        )).await?;
        loop {
            if pg.query_opt(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name='rw_native_test' AND active", &[]
            ).await?.is_some() { break; }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // This row's WAL record predates the snapshot watermark, but it becomes
        // visible only after the snapshot. CDC must compare its commit position.
        let overlap = pg.transaction().await?;
        overlap.batch_execute("UPDATE public.native_orders SET v=11 WHERE id=1;").await?;
        client.batch_execute(
            "CREATE TABLE native_orders (
                id INT PRIMARY KEY, v INT, body VARCHAR, n DECIMAL
             ) FROM native_pg TABLE 'public.native_orders';"
        ).await?;
        await_totals(&client, (2,30), None).await?;
        overlap.commit().await?;
        await_totals(&client, (2,31), Some((2,30))).await?;
        pg.batch_execute("UPDATE public.native_orders SET v=10 WHERE id=1;").await?;
        await_totals(&client, (2,30), Some((2,31))).await?;
        let effective: String = pg.query_one("SHOW effective_wal_level", &[]).await?.get(0);
        assert_eq!(effective, "logical");

        let tx = pg.transaction().await?;
        tx.batch_execute(
            "INSERT INTO public.native_orders VALUES (3,30,'new',3);
             UPDATE public.native_orders SET id=10,v=15 WHERE id=1;
             DELETE FROM public.native_orders WHERE id=2;"
        ).await?;
        assert_eq!(totals(&client).await?, (2,30), "uncommitted changes became visible");
        tx.commit().await?;
        await_totals(&client, (2,45), Some((2,30))).await?;
        let row = client.query_one("SELECT body, n::VARCHAR FROM native_orders WHERE id=10", &[]).await?;
        assert_eq!(row.get::<_, String>(0), body);
        assert_eq!(row.get::<_, String>(1), "12345678901234567890.12345678");

        let before: tokio_postgres::types::PgLsn = pg.query_one(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='rw_native_test'", &[]
        ).await?.get(0);
        // Make another transaction so the post-FLUSH acknowledgement must advance.
        pg.batch_execute("UPDATE public.native_orders SET v=16 WHERE id=10;").await?;
        await_totals(&client, (2,46), Some((2,45))).await?;
        client.batch_execute("FLUSH;").await?;
        loop {
            let after: tokio_postgres::types::PgLsn = pg.query_one(
                "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='rw_native_test'", &[]
            ).await?.get(0);
            if u64::from(after) > u64::from(before) { break; }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Kill the process, preserving only the durable SQLite/Hummock files.
        drop(client);
        drop(rw);
        loop {
            let active: bool = pg.query_one(
                "SELECT active FROM pg_replication_slots WHERE slot_name='rw_native_test'", &[]
            ).await?.get(0);
            if !active { break; }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        pg.batch_execute("INSERT INTO public.native_orders VALUES (4,40,'while offline',4);").await?;
        rw = start_rw(directory.path(), &addresses[1..])?;
        let client = connect(&mut rw, addresses[3], "root", "dev").await?;
        await_totals(&client, (3,86), None).await?;
        let ids: Vec<i32> = client.query("SELECT id FROM native_orders ORDER BY id", &[])
            .await?.iter().map(|r| r.get(0)).collect();
        assert_eq!(ids, vec![3,4,10]);
        pg.batch_execute("UPDATE public.native_orders SET v=41 WHERE id=4;").await?;
        await_totals(&client, (3,87), Some((3,86))).await?;
        drop(client);
        drop(rw);
        drop(pg);
        drop(postgres);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }).await;

    assert!(
        matches!(result, Ok(Ok(()))),
        "native PostgreSQL CDC test failed: {result:?}\nRW:\n{}\nPG:\n{}",
        std::fs::read_to_string(directory.path().join("risingwave.log")).unwrap_or_default(),
        std::fs::read_to_string(directory.path().join("postgres.log")).unwrap_or_default()
    );
}
