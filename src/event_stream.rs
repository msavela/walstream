use crate::grpc::plugin::ServerMessage;
use tokio::sync::{broadcast, watch};
use tokio_postgres::types::PgLsn;

#[derive(Debug, Clone)]
pub struct EventStream {
    pub tx: tokio::sync::broadcast::Sender<ServerMessage>,

    pub last_received_lsn: tokio::sync::watch::Sender<PgLsn>, // = last WAL received
    pub last_sent_lsn: tokio::sync::watch::Sender<PgLsn>,     // = last WAL sent to gRPC
    pub last_ack_lsn: tokio::sync::watch::Sender<PgLsn>,      // = last WAL the gRPC client acked
}

impl EventStream {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel::<ServerMessage>(1024);
        let (last_received_lsn, _) = watch::channel(PgLsn::from(0));
        let (last_sent_lsn, _) = watch::channel(PgLsn::from(0));
        let (last_ack_lsn, _) = watch::channel(PgLsn::from(0));
        Self {
            tx: tx,
            last_received_lsn,
            last_sent_lsn,
            last_ack_lsn,
        }
    }
}
