use clap::{ArgAction, Parser, Subcommand};

#[derive(Parser)]
#[command(version)]
#[command(name = "walstream")]
#[command(about = "PostgreSQL Change Data Capture (CDC) via gRPC", long_about = None, disable_help_flag = true, disable_version_flag = true)]
#[command(disable_help_flag = true)]
#[command(disable_version_flag = true)]
#[command(subcommand_required(true), arg_required_else_help(true))]
pub struct Cli {
    #[arg(
        short,
        long,
        value_name = "CONNECTION",
        env = "CONNECTION",
        help = "Connection string, e.g. 'postgresql://postgres:postgres@localhost/postgres'"
    )]
    pub connection: String,

    #[arg(long = "help", action = ArgAction::Help, value_parser = clap::value_parser!(bool))]
    help: (),

    #[arg(long = "version", action = ArgAction::Version, value_parser = clap::value_parser!(bool))]
    version: (),

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start/create replication slot
    Start {
        /// Publication name, e.g. 'wal_publication'
        #[arg(value_name = "PUBLICATION")]
        publication: String,

        /// Replication slot name, e.g. 'wal_listener'
        #[arg(value_name = "SLOT")]
        slot: String,

        #[arg(
            short,
            long,
            value_name = "TEMPORARY",
            env = "TEMPORARY",
            help = "Use temporary replication slot",
            default_value = "true"
        )]
        temporary: Option<bool>,

        #[arg(
            short = 'p',
            long = "port",
            value_name = "PORT",
            env = "PORT",
            help = "Custom port",
            default_value = "50051"
        )]
        port: Option<u32>,

        #[arg(
            short = 'h',
            long = "host",
            value_name = "HOST",
            env = "HOST",
            help = "Custom host",
            default_value = "0.0.0.0"
        )]
        host: Option<String>,

        #[arg(long = "help", action = ArgAction::Help, value_parser = clap::value_parser!(bool))]
        help: (),
    },

    /// List replication slots
    List {
        #[arg(long = "help", action = ArgAction::Help, value_parser = clap::value_parser!(bool))]
        help: (),
    },

    /// Delete permanent replication slots
    Delete {
        /// Replication slot to delete
        slot: String,

        #[arg(long = "help", action = ArgAction::Help, value_parser = clap::value_parser!(bool))]
        help: (),
    },
}
