use crate::event_stream::EventStream;
use plugin::{
    ClientAck, ClientMessage, ServerMessage,
    plugin_service_server::{PluginService, PluginServiceServer},
};
use tokio_postgres::types::PgLsn;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, transport::Server};
use tracing::{debug, error, info};

pub mod plugin {
    tonic::include_proto!("plugin");
}

#[derive(Debug)]
pub struct PluginServer {
    event_stream: EventStream,
}

#[tonic::async_trait]
impl PluginService for PluginServer {
    type SessionStream = ReceiverStream<Result<ServerMessage, Status>>;

    async fn session(
        &self,
        request: Request<tonic::Streaming<ClientMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let addr = request.remote_addr().unwrap().to_string();

        info!("Client connected: {}", addr);

        // Signal when client disconnects
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let (client_tx, client_rx) =
            tokio::sync::mpsc::channel::<Result<ServerMessage, Status>>(32);

        // Spawn task to read client messages
        let mut stream = request.into_inner();
        let tx_last_ack_lsn = self.event_stream.last_ack_lsn.clone();
        tokio::spawn(async move {
            while let Ok(Some(client_msg)) = stream.message().await {
                match client_msg.msg {
                    Some(plugin::client_message::Msg::Ack(ClientAck { pg_lsn })) => {
                        tx_last_ack_lsn.send(PgLsn::from(pg_lsn)).ok();
                    }
                    _ => {}
                }
            }

            info!("Client disconnected: {}", addr);
            let _ = shutdown_tx.send(());
        });

        let mut broadcast_rx = self.event_stream.tx.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => {
                        debug!("Stopping event forwarder task (client disconnected)");
                        break;
                    }
                    msg = broadcast_rx.recv() => {
                        match msg {
                            Ok(ev) => {
                                if client_tx.send(Ok(ev)).await.is_err() {
                                    info!("Client channel closed — stopping forwarder");
                                    break;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                error!("Broadcast channel closed");
                                break;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                error!("Client lagged behind, skipped {} messages", n);
                            }
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(client_rx)))
    }
}

pub async fn start(
    event_stream: EventStream,
    port: u32,
    host: String,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Listening on {}:{}", host, port);

    let plugin_server: PluginServer = PluginServer { event_stream };
    Server::builder()
        .add_service(PluginServiceServer::new(plugin_server))
        .serve(format!("{}:{}", host, port).parse()?)
        .await?;

    Ok(())
}
