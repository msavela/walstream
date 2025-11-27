use tokio_stream::StreamExt;
use tonic::Request;
use tonic::transport::Endpoint;
use walstream::plugin_service_client::PluginServiceClient;
use walstream::{ClientAck, ClientMessage, ServerMessage};

pub mod walstream {
    tonic::include_proto!("plugin");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let uri = "http://127.0.0.1:50051";

    loop {
        match start(uri).await {
            Ok(_) => {
                println!("Session ended normally.");
                break;
            }
            Err(e) => {
                eprintln!("Connection lost: {e}");
                eprintln!("Reconnecting in 2 seconds...");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }

    Ok(())
}

async fn start(uri: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = PluginServiceClient::new(
        Endpoint::from_shared(uri.to_string())?
            .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(std::time::Duration::from_secs(10))
            .connect()
            .await?,
    );

    let (tx, rx) = tokio::sync::mpsc::channel::<ClientMessage>(32);
    let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);

    let mut stream = client.session(Request::new(outbound)).await?.into_inner();

    println!("Connected, listening for events...");

    let ack_tx = tx.clone();

    while let Some(msg) = stream.next().await {
        match msg {
            Ok(ServerMessage { msg: Some(m) }) => match m {
                walstream::server_message::Msg::Insert(ev) => {
                    println!("INSERT {}.{}: {}", ev.schema, ev.table, ev.json_payload);

                    ack_tx
                        .send(ClientMessage {
                            msg: Some(walstream::client_message::Msg::Ack(ClientAck {
                                pg_lsn: ev.pg_lsn,
                            })),
                        })
                        .await
                        .unwrap();
                }
                walstream::server_message::Msg::Update(ev) => {
                    println!("UPDATE {}.{}: {}", ev.schema, ev.table, ev.json_payload);

                    ack_tx
                        .send(ClientMessage {
                            msg: Some(walstream::client_message::Msg::Ack(ClientAck {
                                pg_lsn: ev.pg_lsn,
                            })),
                        })
                        .await
                        .unwrap();
                }
                walstream::server_message::Msg::Delete(ev) => {
                    println!("DELETE {}.{}: {}", ev.schema, ev.table, ev.json_payload);

                    ack_tx
                        .send(ClientMessage {
                            msg: Some(walstream::client_message::Msg::Ack(ClientAck {
                                pg_lsn: ev.pg_lsn,
                            })),
                        })
                        .await
                        .unwrap();
                }
                walstream::server_message::Msg::Truncate(ev) => {
                    println!("TRUNCATE {}.{}", ev.schema, ev.table);

                    ack_tx
                        .send(ClientMessage {
                            msg: Some(walstream::client_message::Msg::Ack(ClientAck {
                                pg_lsn: ev.pg_lsn,
                            })),
                        })
                        .await
                        .unwrap();
                }
            },
            Ok(_) => {}
            Err(e) => {
                return Err(Box::new(e));
            }
        }
    }

    Ok(())
}
