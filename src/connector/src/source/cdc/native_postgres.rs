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

//! PostgreSQL logical replication for builds without the JVM.
//!
//! Snapshot/backfill stays in RisingWave's existing CDC executors. This reader
//! emits the same event envelope they already consume. Only a durable RW
//! checkpoint may advance PostgreSQL's confirmed flush position.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail, ensure};
use base64::Engine;
use bytes::Bytes;
use futures::StreamExt;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl_replication::MakeTlsConnector;
use postgres_replication::LogicalReplicationStream;
use postgres_replication::protocol::{
    LogicalReplicationMessage, RelationBody, ReplicaIdentity, ReplicationMessage, Tuple, TupleData,
};
use risingwave_common::types::{DataType, ScalarImpl, ScalarRefImpl};
use risingwave_pb::connector_service::cdc_message::CdcMessageType;
use risingwave_pb::connector_service::{CdcMessage, GetEventStreamResponse, SourceType};
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tokio_postgres_replication::config::{ReplicationMode, SslMode as DriverSslMode};
use tokio_postgres_replication::types::{Kind, Type};

use crate::connector_common::{
    PgConnectionConfig, SslMode, create_pg_client, pg_connection_config_from_properties,
};

const OFFSET_VERSION: u64 = 1;
const POSTGRES_EPOCH_SECS: u64 = 946_684_800;

struct Checkpoint {
    source_id: u64,
    slot: String,
    offered: AtomicU64,
    acknowledged: AtomicU64,
}

impl Drop for Checkpoint {
    fn drop(&mut self) {
        let mut registry = CHECKPOINTS.lock().unwrap_or_else(|e| e.into_inner());
        if registry
            .get(&self.source_id)
            .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self))
        {
            registry.remove(&self.source_id);
        }
    }
}

static CHECKPOINTS: LazyLock<Mutex<HashMap<u64, Weak<Checkpoint>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Called by WaitCheckpointWorker only after RW's checkpoint is durable.
pub(crate) fn commit_offset(source_id: u64, encoded: &str) -> anyhow::Result<()> {
    let offset = decode_offset(encoded)?;
    ensure!(
        offset["sourcePartition"]["server"] == format!("RW_CDC_{source_id}"),
        "native PostgreSQL checkpoint belongs to a different source"
    );
    let lsn = resume_lsn(&offset)?;
    let checkpoint = CHECKPOINTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&source_id)
        .and_then(Weak::upgrade)
        .context("native PostgreSQL reader is no longer running")?;
    ensure!(
        offset["native_pg_slot"] == checkpoint.slot,
        "native PostgreSQL checkpoint belongs to a different replication slot"
    );
    ensure!(
        lsn <= checkpoint.offered.load(Ordering::Acquire),
        "cannot acknowledge PostgreSQL WAL that has not been emitted"
    );
    checkpoint.acknowledged.fetch_max(lsn, Ordering::AcqRel);
    Ok(())
}

fn decode_offset(encoded: &str) -> anyhow::Result<Value> {
    let offset: Value = serde_json::from_str(encoded).context("invalid PostgreSQL CDC offset")?;
    ensure!(
        offset["native_pg_version"] == OFFSET_VERSION,
        "this PostgreSQL offset was not produced by the native reader; \
         recreate the source and backfill before switching from Debezium"
    );
    Ok(offset)
}

fn resume_lsn(offset: &Value) -> anyhow::Result<u64> {
    offset["sourceOffset"]["native_commit_lsn"]
        .as_u64()
        .context("native PostgreSQL offset has no committed WAL position")
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn required<'a>(props: &'a BTreeMap<String, String>, key: &str) -> anyhow::Result<&'a str> {
    props
        .get(key)
        .filter(|v| !v.is_empty())
        .map(String::as_str)
        .with_context(|| format!("missing {key} in native PostgreSQL CDC properties"))
}

/// Validate using the existing SQL client. No JVM is involved.
pub(crate) async fn validate_source(
    props: &BTreeMap<String, String>,
    is_source_job: bool,
    is_backfill_table: bool,
) -> anyhow::Result<()> {
    ensure!(
        is_source_job || is_backfill_table,
        "native PostgreSQL CDC uses a shared source and RisingWave snapshot backfill"
    );
    let config = pg_connection_config_from_properties(props)?;
    let client = create_pg_client(&config, None, Some("risingwave-native-cdc-validate")).await?;
    let wal_level: String = client.query_one("SHOW wal_level", &[]).await?.get(0);
    let version: String = client
        .query_one("SHOW server_version_num", &[])
        .await?
        .get(0);
    let version: u32 = version
        .parse()
        .context("invalid PostgreSQL server version")?;
    // PostgreSQL 19 raises effective_wal_level when its first logical slot is created.
    ensure!(
        wal_level == "logical" || (version >= 190000 && wal_level == "replica"),
        "PostgreSQL wal_level must be logical (PostgreSQL 19 also accepts replica)"
    );
    let publication = required(props, "publication.name")?;
    let exists = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?
        .is_some();
    if !exists {
        ensure!(
            props
                .get("publication.create.enable")
                .is_none_or(|v| v == "true"),
            "PostgreSQL publication {publication} does not exist"
        );
        // Schema publications also include tables added after source creation.
        // These are catalog identifiers, not SQL or Debezium regular expressions.
        let schemas = required(props, "schema.name")?
            .split(',')
            .map(str::trim)
            .map(|schema| {
                ensure!(!schema.is_empty(), "empty PostgreSQL schema name");
                Ok(quote_identifier(schema))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join(", ");
        client
            .batch_execute(&format!(
                "CREATE PUBLICATION {} FOR TABLES IN SCHEMA {schemas}",
                quote_identifier(publication)
            ))
            .await
            .context("cannot create PostgreSQL schema publication (requires PostgreSQL 15+)")?;
    }
    if is_backfill_table {
        let schema = required(props, "schema.name")?;
        let table = required(props, "table.name")?;
        let row = client
            .query_opt(
                "SELECT c.relreplident::text, EXISTS ( \
                   SELECT 1 FROM pg_publication_tables p \
                   WHERE p.pubname = $3 AND p.schemaname = n.nspname AND p.tablename = c.relname) \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')",
                &[&schema, &table, &publication],
            )
            .await?
            .context("PostgreSQL CDC table does not exist")?;
        ensure!(
            row.get::<_, String>(0) == "f",
            "native PostgreSQL CDC requires REPLICA IDENTITY FULL for {}.{}",
            quote_identifier(schema),
            quote_identifier(table)
        );
        ensure!(
            row.get::<_, bool>(1),
            "PostgreSQL CDC table is not in the publication"
        );
    }
    Ok(())
}

/// Dropping either a reader or a failed startup closes its replication connection.
pub(crate) struct ReaderTask(JoinHandle<()>);

impl Drop for ReaderTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn replication_client(
    config: &PgConnectionConfig,
) -> anyhow::Result<(tokio_postgres_replication::Client, ReaderTask)> {
    let mut pg = tokio_postgres_replication::Config::new();
    pg.host(&config.host)
        .port(config.port)
        .user(&config.user)
        .password(&config.password)
        .dbname(&config.database)
        .application_name("risingwave-native-postgres-cdc")
        .replication_mode(ReplicationMode::Logical)
        .connect_timeout(Duration::from_secs(10))
        .options("-c TimeZone=UTC -c DateStyle=ISO -c IntervalStyle=postgres -c bytea_output=hex -c standard_conforming_strings=on");

    // Reuse the TLS backend already linked by RW's PostgreSQL SQL client.
    let mut ssl = SslConnector::builder(SslMethod::tls())?;
    pg.ssl_mode(match config.ssl_mode {
        SslMode::Disabled => DriverSslMode::Disable,
        SslMode::Preferred => DriverSslMode::Prefer,
        _ => DriverSslMode::Require,
    });
    if matches!(
        config.ssl_mode,
        SslMode::Disabled | SslMode::Preferred | SslMode::Required
    ) {
        ssl.set_verify(SslVerifyMode::NONE);
    } else if let Some(cert) = &config.ssl_root_cert {
        ssl.set_ca_file(cert)?;
    }
    let mut tls = MakeTlsConnector::new(ssl.build());
    if matches!(config.ssl_mode, SslMode::VerifyCa) {
        tls.set_callback(|config, _| {
            config.set_verify_hostname(false);
            Ok(())
        });
    }
    let (client, connection) = pg.connect(tls).await?;
    let task = ReaderTask(tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "native PostgreSQL replication connection closed");
        }
    }));
    Ok((client, task))
}

pub(crate) async fn start(
    source_id: u64,
    props: &BTreeMap<String, String>,
    start_offset: Option<&str>,
) -> anyhow::Result<(
    mpsc::Receiver<anyhow::Result<GetEventStreamResponse>>,
    ReaderTask,
)> {
    let config = pg_connection_config_from_properties(props)?;
    let slot = required(props, "slot.name")?.to_owned();
    ensure!(
        slot.len() <= 63
            && slot
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_'),
        "invalid PostgreSQL replication slot name"
    );
    let publication = required(props, "publication.name")?;
    let saved = start_offset.map(decode_offset).transpose()?;
    if let Some(offset) = &saved {
        ensure!(
            offset["native_pg_slot"] == slot
                && offset["sourcePartition"]["server"] == format!("RW_CDC_{source_id}"),
            "native PostgreSQL resume offset does not match the source and slot"
        );
    }
    let sql = create_pg_client(&config, None, Some("risingwave-native-cdc-slot")).await?;
    let slot_row = sql
        .query_opt(
            "SELECT plugin, database, confirmed_flush_lsn, wal_status \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await?;
    let confirmed = if let Some(row) = slot_row {
        ensure!(
            row.get::<_, Option<String>>(0).as_deref() == Some("pgoutput")
                && row.get::<_, Option<String>>(1).as_deref() == Some(config.database.as_str()),
            "replication slot uses a different plugin or database"
        );
        ensure!(
            row.get::<_, Option<String>>(3).as_deref() != Some("lost"),
            "PostgreSQL replication slot has lost required WAL; a new backfill is necessary"
        );
        row.get::<_, Option<PgLsn>>(2)
            .map(u64::from)
            .context("PostgreSQL replication slot has no consistent position")?
    } else {
        ensure!(
            saved.is_none(),
            "cannot resume: PostgreSQL replication slot is missing"
        );
        let row = sql
            .query_one(
                "SELECT lsn FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .await?;
        u64::from(row.get::<_, PgLsn>(0))
    };
    let start_lsn = saved
        .as_ref()
        .map(resume_lsn)
        .transpose()?
        .unwrap_or(confirmed);
    ensure!(
        confirmed <= start_lsn,
        "PostgreSQL slot was acknowledged beyond RW's durable checkpoint; refusing to skip changes"
    );
    let (client, connection_task) = replication_client(&config).await?;
    let copy = client
        .copy_both_simple::<Bytes>(&format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names {})",
            quote_identifier(&slot),
            PgLsn::from(start_lsn),
            quote_literal(&quote_identifier(publication))
        ))
        .await
        .context("cannot start native PostgreSQL logical replication")?;

    let checkpoint = Arc::new(Checkpoint {
        source_id,
        slot: slot.clone(),
        offered: AtomicU64::new(start_lsn),
        acknowledged: AtomicU64::new(start_lsn),
    });
    CHECKPOINTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(source_id, Arc::downgrade(&checkpoint));

    let (tx, rx) = mpsc::channel(16);
    let task = ReaderTask(tokio::spawn(async move {
        let _connection_task = connection_task;
        let _client = client;
        let result = run(
            source_id,
            config.database,
            slot,
            start_lsn,
            LogicalReplicationStream::new(copy),
            &checkpoint,
            &tx,
        )
        .await;
        if let Err(error) = result {
            let _ = tx.send(Err(error)).await;
        }
    }));
    Ok((rx, task))
}

async fn send(
    tx: &mpsc::Sender<anyhow::Result<GetEventStreamResponse>>,
    source_id: u64,
    event: CdcMessage,
) -> anyhow::Result<()> {
    tx.send(Ok(GetEventStreamResponse {
        source_id,
        events: vec![event],
        control: None,
    }))
    .await
    .context("native PostgreSQL CDC reader was dropped")
}

async fn run(
    source_id: u64,
    database: String,
    slot: String,
    start_lsn: u64,
    stream: LogicalReplicationStream,
    checkpoint: &Checkpoint,
    tx: &mpsc::Sender<anyhow::Result<GetEventStreamResponse>>,
) -> anyhow::Result<()> {
    let mut decoder = Decoder::new(source_id, database, slot, start_lsn);
    send(tx, source_id, decoder.heartbeat()).await?;
    tokio::pin!(stream);
    let mut feedback = tokio::time::interval(Duration::from_secs(5));
    feedback.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let reply = tokio::select! {
            _ = tx.closed() => return Ok(()),
            _ = feedback.tick() => {
                if decoder.transaction.is_none() {
                    send(tx, source_id, decoder.heartbeat()).await?;
                }
                true
            }
            message = stream.next() => {
                match message.context("PostgreSQL replication stream ended")?? {
                    ReplicationMessage::XLogData(body) => {
                        for event in decoder.decode(body.wal_start(), body.into_data())? {
                            checkpoint.offered.store(decoder.completed_lsn, Ordering::Release);
                            send(tx, source_id, event).await?;
                        }
                        false
                    }
                    ReplicationMessage::PrimaryKeepAlive(body) => body.reply() != 0,
                    _ => bail!("unsupported PostgreSQL replication message"),
                }
            }
        };
        if reply {
            let lsn = checkpoint.acknowledged.load(Ordering::Acquire).into();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH + Duration::from_secs(POSTGRES_EPOCH_SECS))?
                .as_micros()
                .try_into()
                .context("PostgreSQL feedback timestamp overflow")?;
            stream
                .as_mut()
                .standby_status_update(lsn, lsn, lsn, now, 0)
                .await?;
        }
    }
}

struct Decoder {
    source_id: u64,
    database: String,
    slot: String,
    relations: HashMap<u32, RelationBody>,
    // xid, final commit LSN, PostgreSQL commit timestamp
    transaction: Option<(u32, u64, i64)>,
    completed_lsn: u64,
    resume_lsn: u64,
}

impl Decoder {
    fn new(source_id: u64, database: String, slot: String, lsn: u64) -> Self {
        Self {
            source_id,
            database,
            slot,
            relations: HashMap::new(),
            transaction: None,
            completed_lsn: lsn,
            resume_lsn: lsn,
        }
    }

    fn event(
        &self,
        payload: Value,
        table: String,
        kind: CdcMessageType,
        lsn: u64,
        commit_lsn: u64,
        xid: u32,
        timestamp: i64,
    ) -> CdcMessage {
        // RW persists the last data row, not the transaction-control marker. The
        // BEGIN message already gives its commit-record LSN; the parser emits
        // these rows only after COMMIT, with transaction splitting disabled.
        let durable_lsn = if kind == CdcMessageType::Data {
            commit_lsn
        } else {
            self.completed_lsn
        };
        CdcMessage {
            payload: if kind == CdcMessageType::Heartbeat {
                String::new()
            } else {
                json!({"schema": null, "payload": payload}).to_string()
            },
            partition: self.source_id.to_string(),
            offset: json!({
                "native_pg_version": OFFSET_VERSION,
                "native_pg_slot": self.slot,
                "sourcePartition": {"server": format!("RW_CDC_{}", self.source_id)},
                "sourceOffset": {
                    "lsn": lsn, "lsn_proc": lsn, "lsn_commit": commit_lsn,
                    "txId": xid, "last_snapshot_record": true,
                    "native_commit_lsn": durable_lsn,
                },
                "isHeartbeat": kind == CdcMessageType::Heartbeat,
            })
            .to_string(),
            full_table_name: table,
            source_ts_ms: timestamp,
            msg_type: kind as i32,
            key: String::new(),
            source_type: SourceType::Postgres as i32,
        }
    }

    fn heartbeat(&self) -> CdcMessage {
        self.event(
            Value::Null,
            String::new(),
            CdcMessageType::Heartbeat,
            self.completed_lsn,
            self.completed_lsn,
            0,
            0,
        )
    }

    fn decode(
        &mut self,
        lsn: u64,
        message: LogicalReplicationMessage,
    ) -> anyhow::Result<Vec<CdcMessage>> {
        use LogicalReplicationMessage::*;
        match message {
            Begin(begin) => {
                ensure!(self.transaction.is_none(), "nested PostgreSQL transaction");
                self.transaction = Some((begin.xid(), begin.final_lsn(), begin.timestamp()));
                if begin.final_lsn() <= self.resume_lsn {
                    return Ok(Vec::new());
                }
                Ok(vec![self.event(
                    json!({"status": "BEGIN", "id": format!("{}:{}", begin.xid(), begin.final_lsn())}),
                    String::new(), CdcMessageType::TransactionMeta,
                    lsn, begin.final_lsn(), begin.xid(), pg_timestamp_ms(begin.timestamp())?,
                )])
            }
            Commit(commit) => {
                let (xid, final_lsn, _) =
                    self.transaction.take().context("COMMIT without BEGIN")?;
                ensure!(
                    final_lsn == commit.commit_lsn(),
                    "PostgreSQL commit LSN does not match BEGIN"
                );
                if commit.commit_lsn() <= self.resume_lsn {
                    return Ok(Vec::new());
                }
                ensure!(
                    commit.end_lsn() >= self.completed_lsn,
                    "PostgreSQL commit position regressed"
                );
                self.completed_lsn = commit.end_lsn();
                Ok(vec![self.event(
                    json!({"status": "END", "id": format!("{xid}:{}", commit.commit_lsn())}),
                    String::new(),
                    CdcMessageType::TransactionMeta,
                    commit.end_lsn(),
                    commit.commit_lsn(),
                    xid,
                    pg_timestamp_ms(commit.timestamp())?,
                )])
            }
            Relation(relation) => {
                ensure!(
                    matches!(relation.replica_identity(), ReplicaIdentity::Full),
                    "native PostgreSQL CDC requires REPLICA IDENTITY FULL for {}.{}",
                    relation.namespace()?,
                    relation.name()?
                );
                self.relations.insert(relation.rel_id(), relation);
                Ok(Vec::new())
            }
            Type(_) | Origin(_) | Message(_) => Ok(Vec::new()),
            Truncate(_) => bail!("TRUNCATE is not supported by native PostgreSQL CDC"),
            message => {
                let (xid, commit_lsn, timestamp) = self
                    .transaction
                    .context("row outside PostgreSQL transaction")?;
                if commit_lsn <= self.resume_lsn {
                    return Ok(Vec::new());
                }
                let (rel_id, op, before_tuple, after_tuple) = match &message {
                    Insert(row) => (row.rel_id(), "c", None, Some(row.tuple())),
                    Update(row) => (
                        row.rel_id(),
                        "u",
                        Some(
                            row.old_tuple()
                                .context("UPDATE requires a full old tuple")?,
                        ),
                        Some(row.new_tuple()),
                    ),
                    Delete(row) => (
                        row.rel_id(),
                        "d",
                        Some(
                            row.old_tuple()
                                .context("DELETE requires a full old tuple")?,
                        ),
                        None,
                    ),
                    _ => bail!("unsupported PostgreSQL logical replication message"),
                };
                let relation = self
                    .relations
                    .get(&rel_id)
                    .context("row refers to an unknown PostgreSQL relation")?;
                let before = before_tuple
                    .map(|tuple| tuple_json(relation, tuple, None))
                    .transpose()?;
                let after = after_tuple
                    .map(|tuple| tuple_json(relation, tuple, before.as_ref()))
                    .transpose()?;
                let timestamp = pg_timestamp_ms(timestamp)?;
                let event = |before: Option<Value>,
                             after: Option<Value>,
                             op: &str|
                 -> anyhow::Result<CdcMessage> {
                    let payload = json!({
                        "before": before, "after": after, "op": op, "ts_ms": timestamp,
                        "source": {"db": self.database, "schema": relation.namespace()?,
                            "table": relation.name()?, "lsn": lsn, "txId": xid, "ts_ms": timestamp},
                    });
                    Ok(self.event(
                        payload,
                        format!("{}.{}", relation.namespace()?, relation.name()?),
                        CdcMessageType::Data,
                        lsn,
                        commit_lsn,
                        xid,
                        timestamp,
                    ))
                };
                if op == "u" {
                    // RW interprets Debezium updates as upserts, so the before key is
                    // otherwise lost on a primary-key change. FULL identity provides
                    // both rows; keep delete/insert inside the same transaction.
                    Ok(vec![event(before, None, "d")?, event(None, after, "c")?])
                } else {
                    Ok(vec![event(before, after, op)?])
                }
            }
        }
    }
}

fn pg_timestamp_ms(micros: i64) -> anyhow::Result<i64> {
    micros
        .div_euclid(1000)
        .checked_add(POSTGRES_EPOCH_SECS as i64 * 1000)
        .context("PostgreSQL commit timestamp overflow")
}

fn tuple_json(
    relation: &RelationBody,
    tuple: &Tuple,
    old: Option<&Value>,
) -> anyhow::Result<Value> {
    ensure!(
        relation.columns().len() == tuple.tuple_data().len(),
        "PostgreSQL tuple does not match the relation schema"
    );
    let mut values = Map::new();
    for (column, value) in relation.columns().iter().zip(tuple.tuple_data()) {
        let name = column.name()?;
        let value = match value {
            TupleData::Null => Value::Null,
            TupleData::Text(text) => {
                value_json(column.type_id() as u32, std::str::from_utf8(text)?)?
            }
            TupleData::UnchangedToast => old
                .and_then(|row| row.get(name))
                .cloned()
                .with_context(|| format!("unchanged TOAST column {name} has no full old value"))?,
            TupleData::Binary(_) => bail!("unexpected binary tuple in PostgreSQL text replication"),
        };
        values.insert(name.to_owned(), value);
    }
    Ok(Value::Object(values))
}

fn scalar_type(pg: &Type) -> Option<DataType> {
    Some(match *pg {
        Type::BOOL => DataType::Boolean,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 | Type::OID => DataType::Int64,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::NUMERIC => DataType::Decimal,
        Type::BYTEA => DataType::Bytea,
        Type::DATE => DataType::Date,
        Type::TIME => DataType::Time,
        Type::TIMESTAMP => DataType::Timestamp,
        Type::TIMESTAMPTZ => DataType::Timestamptz,
        Type::INTERVAL => DataType::Interval,
        Type::JSON | Type::JSONB => DataType::Jsonb,
        _ => match pg.kind() {
            Kind::Array(element) => scalar_type(element)?.list(),
            Kind::Simple | Kind::Enum(_) => DataType::Varchar,
            _ => return None,
        },
    })
}

fn value_json(oid: u32, text: &str) -> anyhow::Result<Value> {
    let Some(pg) = Type::from_oid(oid) else {
        // User-defined enums, composite-as-varchar and vectors use their text form.
        return Ok(Value::String(text.to_owned()));
    };
    if pg == Type::POINT {
        let (x, y) = text
            .strip_prefix('(')
            .and_then(|s| s.strip_suffix(')'))
            .and_then(|s| s.split_once(','))
            .context("invalid PostgreSQL point")?;
        return Ok(json!({
            "x": value_json(Type::FLOAT8.oid(), x.trim())?,
            "y": value_json(Type::FLOAT8.oid(), y.trim())?,
        }));
    }
    let data_type = scalar_type(&pg).context("unsupported PostgreSQL CDC column type")?;
    let scalar = ScalarImpl::from_text(text, &data_type).map_err(|error| {
        anyhow::anyhow!("cannot decode PostgreSQL {} value: {error}", pg.name())
    })?;
    scalar_json(scalar.as_scalar_ref_impl())
}

fn scalar_json(value: ScalarRefImpl<'_>) -> anyhow::Result<Value> {
    use ScalarRefImpl::*;
    Ok(match value {
        Bool(value) => Value::Bool(value),
        Int16(value) => json!(value),
        Int32(value) => json!(value),
        Int64(value) => json!(value),
        Float32(value) => Value::Number(
            serde_json::Number::from_f64(value.0 as f64)
                .context("non-finite PostgreSQL float is unsupported")?,
        ),
        Float64(value) => Value::Number(
            serde_json::Number::from_f64(value.0)
                .context("non-finite PostgreSQL float is unsupported")?,
        ),
        Utf8(value) => Value::String(value.to_owned()),
        Bytea(value) => Value::String(base64::engine::general_purpose::STANDARD.encode(value)),
        Decimal(value) => Value::String(value.to_string()),
        Date(value) => Value::String(value.to_string()),
        Time(value) => Value::String(value.to_string()),
        Timestamp(value) => Value::String(value.to_string()),
        Timestamptz(value) => Value::String(value.to_string()),
        Interval(value) => Value::String(value.to_string()),
        Jsonb(value) => Value::String(value.to_string()),
        List(value) => Value::Array(
            value
                .iter()
                .map(|value| {
                    value
                        .map(scalar_json)
                        .transpose()
                        .map(|v| v.unwrap_or(Value::Null))
                })
                .collect::<anyhow::Result<_>>()?,
        ),
        _ => bail!("unsupported PostgreSQL CDC scalar"),
    })
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};

    use super::*;

    fn frame(tag: u8, body: impl FnOnce(&mut BytesMut)) -> LogicalReplicationMessage {
        let mut bytes = BytesMut::new();
        bytes.put_u8(tag);
        body(&mut bytes);
        LogicalReplicationMessage::parse(&bytes.freeze()).unwrap()
    }

    fn relation() -> LogicalReplicationMessage {
        frame(b'R', |b| {
            b.put_u32(42);
            b.extend_from_slice(b"public\0orders\0");
            b.put_u8(b'f');
            b.put_u16(2);
            for (name, oid) in [("id", 23), ("body", 25)] {
                b.put_u8(1);
                b.extend_from_slice(name.as_bytes());
                b.put_u8(0);
                b.put_i32(oid);
                b.put_i32(-1);
            }
        })
    }

    fn begin(lsn: u64) -> LogicalReplicationMessage {
        frame(b'B', |b| {
            b.put_u64(lsn);
            b.put_i64(1_000_000);
            b.put_u32(7);
        })
    }

    fn commit(lsn: u64, end: u64) -> LogicalReplicationMessage {
        frame(b'C', |b| {
            b.put_u8(0);
            b.put_u64(lsn);
            b.put_u64(end);
            b.put_i64(1_000_000);
        })
    }

    fn tuple(b: &mut BytesMut, values: &[TupleData]) {
        b.put_u16(values.len() as u16);
        for value in values {
            match value {
                TupleData::Null => b.put_u8(b'n'),
                TupleData::UnchangedToast => b.put_u8(b'u'),
                TupleData::Text(text) => {
                    b.put_u8(b't');
                    b.put_i32(text.len() as i32);
                    b.extend_from_slice(text);
                }
                TupleData::Binary(_) => unreachable!(),
            }
        }
    }

    fn text(value: &'static str) -> TupleData {
        TupleData::Text(Bytes::from_static(value.as_bytes()))
    }

    fn payload(event: &CdcMessage) -> Value {
        serde_json::from_str::<Value>(&event.payload).unwrap()["payload"].clone()
    }

    #[test]
    fn pgoutput_rows_transactions_and_durable_acknowledgements() {
        let source_id = 9_000_001;
        let mut decoder = Decoder::new(source_id, "db".into(), "test_slot".into(), 100);
        let checkpoint = Arc::new(Checkpoint {
            source_id,
            slot: "test_slot".into(),
            offered: AtomicU64::new(100),
            acknowledged: AtomicU64::new(100),
        });
        CHECKPOINTS
            .lock()
            .unwrap()
            .insert(source_id, Arc::downgrade(&checkpoint));

        decoder.decode(100, relation()).unwrap();
        let begin = decoder.decode(110, begin(200)).unwrap().pop().unwrap();
        assert_eq!(payload(&begin)["status"], "BEGIN");

        let insert = decoder
            .decode(
                120,
                frame(b'I', |b| {
                    b.put_u32(42);
                    b.put_u8(b'N');
                    tuple(b, &[text("1"), text("large unchanged body")]);
                }),
            )
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            payload(&insert)["after"],
            json!({"id": 1, "body": "large unchanged body"})
        );
        assert_eq!(insert.full_table_name, "public.orders");
        assert_eq!(insert.source_type, SourceType::Postgres as i32);

        // A key-changing update must retain both keys and preserve unchanged TOAST.
        let update = decoder
            .decode(
                140,
                frame(b'U', |b| {
                    b.put_u32(42);
                    b.put_u8(b'O');
                    tuple(b, &[text("1"), text("large unchanged body")]);
                    b.put_u8(b'N');
                    tuple(b, &[text("2"), TupleData::UnchangedToast]);
                }),
            )
            .unwrap();
        assert_eq!(update.len(), 2);
        assert_eq!(payload(&update[0])["op"], "d");
        assert_eq!(payload(&update[0])["before"]["id"], 1);
        assert_eq!(payload(&update[1])["op"], "c");
        let update = &update[1];
        assert_eq!(
            payload(&update)["after"],
            json!({"id": 2, "body": "large unchanged body"})
        );
        assert_eq!(
            resume_lsn(&decode_offset(&update.offset).unwrap()).unwrap(),
            200
        );
        assert!(commit_offset(source_id, &update.offset).is_err());
        assert_eq!(checkpoint.acknowledged.load(Ordering::Acquire), 100);

        let delete = decoder
            .decode(
                160,
                frame(b'D', |b| {
                    b.put_u32(42);
                    b.put_u8(b'O');
                    tuple(b, &[text("2"), text("large unchanged body")]);
                }),
            )
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(payload(&delete)["op"], "d");
        assert_eq!(payload(&delete)["before"]["id"], 2);
        assert!(payload(&delete)["after"].is_null());

        let commit = decoder
            .decode(200, commit(200, 201))
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(payload(&commit)["status"], "END");
        assert!(commit_offset(source_id, &commit.offset).is_err());
        checkpoint.offered.store(201, Ordering::Release);
        // Merely reading COMMIT must not release WAL.
        assert_eq!(checkpoint.acknowledged.load(Ordering::Acquire), 100);
        // The persisted data-row offset is sufficient, even without a heartbeat.
        commit_offset(source_id, &update.offset).unwrap();
        assert_eq!(checkpoint.acknowledged.load(Ordering::Acquire), 200);
        commit_offset(source_id, &commit.offset).unwrap();
        assert_eq!(checkpoint.acknowledged.load(Ordering::Acquire), 201);
        commit_offset(source_id, &insert.offset).unwrap();
        assert_eq!(checkpoint.acknowledged.load(Ordering::Acquire), 201);

        // A checkpoint offset retains the existing backfill parser contract.
        let parsed = crate::source::cdc::external::postgres::PostgresOffset::parse_debezium_offset(
            &commit.offset,
        )
        .unwrap();
        assert_eq!(parsed.lsn, 201);
        assert_eq!(parsed.lsn_commit, Some(200));
        assert_eq!(parsed.txid, 7);
    }

    #[test]
    fn restart_skips_only_already_committed_transactions() {
        // Resume from either the data-row commit record or a later heartbeat end.
        for resume in [200, 201] {
            let mut decoder = Decoder::new(2, "db".into(), "slot".into(), resume);
            decoder.decode(0, relation()).unwrap();
            assert!(decoder.decode(110, begin(200)).unwrap().is_empty());
            assert!(
                decoder
                    .decode(
                        120,
                        frame(b'I', |b| {
                            b.put_u32(42);
                            b.put_u8(b'N');
                            tuple(b, &[text("1"), text("old")]);
                        })
                    )
                    .unwrap()
                    .is_empty()
            );
            assert!(decoder.decode(200, commit(200, 201)).unwrap().is_empty());
            assert!(!decoder.decode(210, begin(300)).unwrap().is_empty());
            let next = decoder
                .decode(300, commit(300, 301))
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(
                resume_lsn(&decode_offset(&next.offset).unwrap()).unwrap(),
                301
            );
        }
    }

    #[test]
    fn invalid_protocol_and_legacy_offsets_fail_closed() {
        assert!(decode_offset(r#"{"sourceOffset":{"lsn":123}}"#).is_err());
        let mut decoder = Decoder::new(3, "db".into(), "slot".into(), 100);
        assert!(decoder.decode(200, commit(200, 201)).is_err());
        decoder.decode(110, begin(200)).unwrap();
        assert!(
            decoder
                .decode(
                    120,
                    frame(b'I', |b| {
                        b.put_u32(999);
                        b.put_u8(b'N');
                        tuple(b, &[text("1"), text("unknown relation")]);
                    })
                )
                .is_err()
        );
        decoder.decode(120, relation()).unwrap();
        assert!(
            decoder
                .decode(
                    130,
                    frame(b'I', |b| {
                        b.put_u32(42);
                        b.put_u8(b'N');
                        tuple(b, &[text("1"), TupleData::UnchangedToast]);
                    })
                )
                .is_err()
        );
    }

    #[test]
    fn postgres_text_values_preserve_precision_nulls_and_binary_data() {
        assert_eq!(
            value_json(20, "9223372036854775807").unwrap(),
            json!(i64::MAX)
        );
        assert_eq!(
            value_json(1700, "12345678901234567890.123456789").unwrap(),
            json!("12345678901234567890.123456789")
        );
        assert_eq!(value_json(16, "t").unwrap(), json!(true));
        assert_eq!(value_json(17, r"\x0001ff").unwrap(), json!("AAH/"));
        assert_eq!(value_json(1007, "{1,NULL,3}").unwrap(), json!([1, null, 3]));
        assert_eq!(
            value_json(1009, r#"{"a,b","quoted\"text",NULL}"#).unwrap(),
            json!(["a,b", "quoted\"text", null])
        );
        assert_eq!(
            value_json(3802, r#"{"value":1}"#).unwrap(),
            json!(r#"{"value": 1}"#)
        );
        assert_eq!(
            value_json(1114, "2026-11-10 12:34:56.123456").unwrap(),
            json!("2026-11-10 12:34:56.123456")
        );
        assert!(value_json(701, "NaN").is_err());
        assert_eq!(
            value_json(600, "(1.5,-2)").unwrap(),
            json!({"x": 1.5, "y": -2.0})
        );
        assert!(value_json(600, "(NaN,1)").is_err());
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_literal("a'b"), "'a''b'");
    }
}
