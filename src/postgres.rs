use crate::event_stream::EventStream;
use crate::grpc::plugin::{
    self, DeleteEvent, InsertEvent, ServerMessage, TruncateEvent, UpdateEvent,
};
use futures::StreamExt;
use postgres_replication::LogicalReplicationStream;
use postgres_replication::protocol::{
    LogicalReplicationMessage, ReplicationMessage, Tuple, TupleData,
};
use serde_json::Value;
use std::error::Error;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use tracing::{debug, error, info};

/// Number of seconds between Unix epoch (1970-01-01) and Postgres epoch (2000-01-01)
const POSTGRES_EPOCH_OFFSET_SECONDS: u64 = 946_684_800;

/// Postgres epoch (2000-01-01 00:00:00 UTC) for timestamp calculations
static POSTGRES_EPOCH: LazyLock<SystemTime> =
    LazyLock::new(|| UNIX_EPOCH + Duration::from_secs(POSTGRES_EPOCH_OFFSET_SECONDS));

#[derive(Debug)]
pub struct RelationInfo {
    pub schema: String,
    pub table: String,
    pub columns: Vec<String>,
}

pub struct ReplicationInstance {
    event_stream: EventStream,
    connection_string: String,
    last_ack_lsn_sent: PgLsn,
    last_status_update: SystemTime,
}

impl ReplicationInstance {
    pub fn new(event_stream: EventStream, connection_string: String) -> Self {
        Self {
            event_stream,
            connection_string,
            last_ack_lsn_sent: PgLsn::from(0),
            last_status_update: SystemTime::now(),
        }
    }

    pub async fn ensure_replication_slot(
        &self,
        client: &Client,
        replication_slot: &str,
        temporary: bool,
    ) -> Result<(String, PgLsn), Box<dyn Error>> {
        // Query existing replication slot
        let existing_slots = client
            .simple_query(&format!(
                "SELECT slot_name, restart_lsn FROM pg_replication_slots WHERE slot_name = '{}'",
                replication_slot
            ))
            .await?;

        let rows: Vec<_> = existing_slots
            .into_iter()
            .filter_map(|msg| {
                if let SimpleQueryMessage::Row(row) = msg {
                    Some(row)
                } else {
                    None
                }
            })
            .collect();

        if rows.is_empty() {
            info!(
                "No replication slot named '{}', creating a new {} slot",
                replication_slot,
                if temporary { "temporary" } else { "permanent" }
            );

            let create_result = client
                .simple_query(&format!(
                    "SELECT pg_create_logical_replication_slot('{}', 'pgoutput', {})",
                    replication_slot, temporary
                ))
                .await;

            if let Err(e) = create_result {
                return Err(Box::new(e) as Box<dyn Error>);
            }

            // Newly created replication slot always starts at 0/0
            return Ok((replication_slot.to_string(), PgLsn::from(0)));
        }

        let rows = client
            .simple_query(&format!(
                "SELECT slot_name, restart_lsn FROM pg_replication_slots WHERE slot_name = '{}'",
                replication_slot
            ))
            .await?;

        for message in rows {
            if let SimpleQueryMessage::Row(row) = message {
                let slot_name: String = row.get(0).ok_or("slot_name is NULL")?.to_string();
                let restart_lsn_str: &str = row.get(1).ok_or("restart_lsn is NULL")?;
                let restart_lsn = restart_lsn_str.parse::<PgLsn>().unwrap();
                return Ok((slot_name, restart_lsn));
            }
        }

        Err("Failed to fetch replication slot info".into())
    }

    pub async fn list_replication_slots(self) -> Result<(), Box<dyn std::error::Error>> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;

        // Background I/O task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Connection error: {}", e);
            }
        });

        for row in client
            .query("SELECT * FROM pg_replication_slots", &[])
            .await?
        {
            let slot_name: &str = row.get(0);
            let slot_type: &str = row.get(2);
            let database: &str = row.get(4);
            let temporary: bool = row.get(5);
            let active: bool = row.get(6);
            let restart_lsn: PgLsn = row.get(10);
            let confirmed_flush_lsn: PgLsn = row.get(11);
            let wal_status: &str = row.get(12);
            let conflicting: bool = row.get(17);

            println!(
                "Name: {} | Database: {} | Type: {} | Temporary: {} | Active: {} | Restart LSN: {} | Confirmed flush LSN: {} | Status: {} | Conflicting: {}",
                slot_name,
                database,
                slot_type,
                temporary,
                active,
                restart_lsn,
                confirmed_flush_lsn,
                wal_status,
                conflicting
            );
        }

        Ok(())
    }

    pub async fn delete_replication_slot(
        self,
        replication_slot: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Connection error: {}", e);
            }
        });

        match client.simple_query(
            &format!(
                "SELECT pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{}'",
                replication_slot
            )
        ).await {
                Ok(_) => {
                    println!("Replication slot '{}' succesfully deleted", replication_slot);
                }
                Err(e) => {
                    error!("Error: {}", e);
                }
        }

        Ok(())
    }

    pub async fn start(mut self, publication: &str, replication_slot: &str, temporary: bool) {
        loop {
            match self
                .stream_once(publication, replication_slot, temporary)
                .await
            {
                Ok(new_lsn) => {
                    info!("Stream cycle ended, restart_lsn = {}", new_lsn);
                }
                Err(e) => {
                    error!("Replication error: {}", e);
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    async fn stream_once(
        &mut self,
        publication: &str,
        replication_slot: &str,
        temporary: bool,
    ) -> Result<PgLsn, Box<dyn std::error::Error>> {
        let (client, connection) = tokio_postgres::connect(
            &format!("{}?replication=database", self.connection_string),
            NoTls,
        )
        .await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Connection error: {}", e);
            }
        });

        let (_, start_lsn) = self
            .ensure_replication_slot(&client, replication_slot, temporary)
            .await?;

        info!("Waiting clients before starting replication stream...");
        while self.event_stream.tx.receiver_count() == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        info!("Starting replication stream...");

        let replication_query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}')",
            replication_slot, start_lsn, publication
        );

        let copy_stream = client
            .copy_both_simple::<bytes::Bytes>(&replication_query)
            .await?;

        let stream = LogicalReplicationStream::new(copy_stream);
        tokio::pin!(stream);

        let mut relation_store: std::collections::HashMap<u32, RelationInfo> =
            std::collections::HashMap::new();

        let last_received_lsn = self.event_stream.last_received_lsn.subscribe();
        let last_sent_lsn = self.event_stream.last_sent_lsn.subscribe();
        let last_ack_lsn = self.event_stream.last_ack_lsn.subscribe();

        info!("Connected to replication slot '{}'", replication_slot);
        while let Some(msg) = stream.next().await {
            let msg = msg?;

            match msg {
                ReplicationMessage::XLogData(xlog) => {
                    if self.event_stream.tx.receiver_count() == 0 {
                        info!(
                            "No clients connected, pause replication steam until next connection"
                        );
                        if self.event_stream.tx.receiver_count() == 0 {
                            break;
                        }
                    }

                    self.event_stream
                        .last_received_lsn
                        .send(PgLsn::from(xlog.wal_end()))
                        .ok();

                    match xlog.data() {
                        LogicalReplicationMessage::Relation(body) => {
                            debug!("Relation {:#?}", body);

                            relation_store.insert(
                                body.rel_id(),
                                RelationInfo {
                                    schema: body.namespace()?.into(),
                                    table: body.name()?.into(),
                                    columns: body
                                        .columns()
                                        .iter()
                                        .map(|c| c.name().unwrap().to_string())
                                        .collect::<Vec<String>>(),
                                },
                            );
                        }
                        LogicalReplicationMessage::Insert(body) => {
                            let relation = relation_store.get(&body.rel_id()).unwrap();
                            let json_payload = tuple_to_json(&relation.columns, body.tuple());

                            debug!("Insert {:#?} ", body);

                            match self.event_stream.tx.send(ServerMessage {
                                msg: Some(plugin::server_message::Msg::Insert(InsertEvent {
                                    pg_lsn: xlog.wal_end(),
                                    schema: relation.schema.clone(),
                                    table: relation.table.clone(),
                                    json_payload: json_payload.to_string(),
                                })),
                            }) {
                                Ok(n) => {
                                    self.event_stream
                                        .last_sent_lsn
                                        .send(PgLsn::from(xlog.wal_end()))
                                        .ok();
                                    debug!("Sent event to {} subscribers", n);
                                }
                                Err(_) => (),
                            }
                        }
                        LogicalReplicationMessage::Update(body) => {
                            let relation = relation_store.get(&body.rel_id()).unwrap();
                            let json_payload = tuple_to_json(&relation.columns, body.new_tuple());

                            debug!("Update {:#?}", body);

                            match self.event_stream.tx.send(ServerMessage {
                                msg: Some(plugin::server_message::Msg::Update(UpdateEvent {
                                    pg_lsn: xlog.wal_end(),
                                    schema: relation.schema.clone(),
                                    table: relation.table.clone(),
                                    json_payload: json_payload.to_string(),
                                })),
                            }) {
                                Ok(n) => {
                                    self.event_stream
                                        .last_sent_lsn
                                        .send(PgLsn::from(xlog.wal_end()))
                                        .ok();
                                    debug!("Sent event to {} subscribers", n);
                                }
                                Err(_) => (),
                            }
                        }
                        LogicalReplicationMessage::Delete(body) => {
                            let relation = relation_store.get(&body.rel_id()).unwrap();
                            let json_payload =
                                tuple_to_json(&relation.columns, body.key_tuple().unwrap());

                            debug!("Delete {} {:#?}", relation.table, body);

                            match self.event_stream.tx.send(ServerMessage {
                                msg: Some(plugin::server_message::Msg::Delete(DeleteEvent {
                                    pg_lsn: xlog.wal_end(),
                                    schema: relation.schema.clone(),
                                    table: relation.table.clone(),
                                    json_payload: json_payload.to_string(),
                                })),
                            }) {
                                Ok(n) => {
                                    self.event_stream
                                        .last_sent_lsn
                                        .send(PgLsn::from(xlog.wal_end()))
                                        .ok();
                                    debug!("Sent event to {} subscribers", n);
                                }
                                Err(_) => (),
                            }
                        }
                        LogicalReplicationMessage::Truncate(truncate) => {
                            let relations = truncate
                                .rel_ids()
                                .iter()
                                .map(|id| relation_store.get(id).unwrap())
                                .collect::<Vec<&RelationInfo>>();

                            debug!("Truncate {:#?}", truncate);

                            for relation in relations.iter() {
                                match self.event_stream.tx.send(ServerMessage {
                                    msg: Some(plugin::server_message::Msg::Truncate(
                                        TruncateEvent {
                                            pg_lsn: xlog.wal_end(),
                                            schema: relation.schema.clone(),
                                            table: relation.table.clone(),
                                        },
                                    )),
                                }) {
                                    Ok(n) => {
                                        self.event_stream
                                            .last_sent_lsn
                                            .send(PgLsn::from(xlog.wal_end()))
                                            .ok();
                                        debug!("Sent event to {} subscribers", n);
                                    }
                                    Err(_) => (),
                                }
                            }
                        }
                        _ => debug!("Unsupported replication message body"),
                    }
                }
                ReplicationMessage::PrimaryKeepAlive(_message) => {
                    let last_received_lsn_val = *last_received_lsn.borrow(); // = last WAL received
                    let last_sent_lsn_val = *last_sent_lsn.borrow(); // = last WAL sent to gRPC
                    let last_ack_lsn_val = *last_ack_lsn.borrow(); // = last WAL the gRPC client acked

                    let now = SystemTime::now();
                    let elapsed = now
                        .duration_since(self.last_status_update)
                        .unwrap_or_default();

                    if self.last_ack_lsn_sent != last_ack_lsn_val
                        || elapsed > Duration::from_secs(10)
                    {
                        let ts: i64 = POSTGRES_EPOCH
                            .elapsed()
                            .map_err(|e: std::time::SystemTimeError| {
                                error!("Invalid PostgreSQL epoch {}", e.to_string());
                                Box::new(e) as Box<dyn Error> // Wrap error in Box
                            })
                            .expect("msg")
                            .as_micros() as i64;

                        let _ = stream
                            .as_mut()
                            .standby_status_update(
                                last_received_lsn_val,
                                last_sent_lsn_val,
                                last_ack_lsn_val,
                                ts,
                                0,
                            )
                            .await;

                        self.last_ack_lsn_sent = last_ack_lsn_val;
                        self.last_status_update = now;

                        debug!(
                            "Sent standby_status_update, last_received_lsn:{} last_sent_lsn:{} last_ack_lsn:{}",
                            last_received_lsn_val, last_sent_lsn_val, last_ack_lsn_val
                        );
                    }
                }
                _ => {}
            }
        }

        let start_lsn = *last_ack_lsn.borrow();
        Ok(start_lsn) // Re-start replication from the last processec event
    }
}

fn tuple_to_json(columns: &[String], tuple: &Tuple) -> Value {
    let mut obj = serde_json::Map::new();

    for (col, field) in columns.iter().zip(tuple.tuple_data().iter()) {
        let value = match field {
            TupleData::Null => Value::Null,
            TupleData::Text(bytes) => {
                let s = String::from_utf8_lossy(bytes).to_string();
                parse_json_value(&s)
            }
            TupleData::Binary(bytes) => {
                serde_json::Value::String(String::from_utf8_lossy(bytes).to_string())
            }
            TupleData::UnchangedToast => Value::Null,
        };

        obj.insert(col.clone(), value);
    }

    Value::Object(obj)
}

fn parse_json_value(s: &str) -> Value {
    if s.eq_ignore_ascii_case("null") {
        return Value::Null;
    }
    if s.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if s.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }
    if let Ok(i) = s.parse::<i64>() {
        return Value::Number(i.into());
    }
    if let Ok(f) = s.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Value::Number(n);
        }
    }
    Value::String(s.to_string())
}
