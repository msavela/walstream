mod cli;
mod event_stream;
mod grpc;
mod postgres;

use clap::Parser;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .init();

    let args = cli::Cli::parse();

    let event_stream = event_stream::EventStream::new();
    let replication_instance =
        postgres::ReplicationInstance::new(event_stream.clone(), args.connection.clone());

    match args.command {
        cli::Commands::Start {
            publication,
            slot,
            temporary,
            port,
            host,
            help: _,
        } => {
            tokio::select! {
                res = grpc::start(event_stream.clone(), port.unwrap(), host.unwrap()) => res,
                res = replication_instance.start(publication.as_str(), slot.as_str(),temporary.unwrap()) => Ok(res),
            }?;
        }
        cli::Commands::List { help: _ } => {
            replication_instance.list_replication_slots().await?;
        }
        cli::Commands::Delete { slot, help: _ } => {
            replication_instance
                .delete_replication_slot(slot.as_str())
                .await?;
        }
    }

    Ok(())
}
