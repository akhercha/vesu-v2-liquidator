// starknet-rust macros expand to `::starknet_rust::*`
extern crate starknet as starknet_rust;

pub mod bindings;
pub mod cli;
pub mod config;
pub mod services;
pub mod storage;
pub mod types;
pub mod utils;

use std::sync::Arc;

use clap::Parser;
use pragma_common::services::{Service, ServiceGroup};
use pragma_common::starknet::FallbackProvider;
use pragma_common::telemetry::init_telemetry;
use tokio::sync::{mpsc, oneshot};

use crate::cli::RunCmd;
use crate::services::indexer::task::IndexerTask;
use crate::services::monitoring::task::MonitoringTask;
use crate::services::oracle::task::OracleTask;
use crate::storage::Storage;
use crate::types::account::StarknetAccount;

const DEFAULT_STARTING_BLOCK: u64 = 2383614;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_telemetry("vesu-v2-liquidator", None).expect("Could not init telemetry");

    let _ = dotenvy::dotenv();

    let mut run_cmd = RunCmd::parse();
    run_cmd.validate()?;

    print_app_title();

    let storage = Arc::new(Storage::open(&run_cmd.storage_path)?);

    let stored_block = storage.get_last_block().ok().flatten();
    let starting_block = match (run_cmd.starting_block, stored_block) {
        (Some(cli), Some(stored)) => cli.max(stored),
        (Some(cli), None) => cli,
        (None, Some(stored)) => stored,
        (None, None) => DEFAULT_STARTING_BLOCK,
    };

    tracing::info!(
        "[🚀 Main] Starting from block {starting_block} (stored: {:?}, cli: {:?})",
        storage.get_last_block().ok().flatten(),
        run_cmd.starting_block,
    );

    let provider = FallbackProvider::new(run_cmd.rpc_url.clone())
        .expect("Could not init the Starknet provider");

    let account = StarknetAccount::from_cli(provider.clone(), run_cmd.clone())?;

    let oracle_service = OracleTask::new(provider.clone());

    let (meet_with_monitoring, wait_for_indexer) = oneshot::channel::<()>();
    let (tx_to_monitoring, rx_from_indexer) = mpsc::unbounded_channel();

    let indexer_service = IndexerTask::new(
        starting_block,
        run_cmd.apibara_api_key,
        run_cmd.apibara_dna_url,
        provider.clone(),
        tx_to_monitoring,
        meet_with_monitoring,
        Arc::clone(&storage),
    );

    let monitoring_service = MonitoringTask::new(
        account,
        provider.clone(),
        rx_from_indexer,
        wait_for_indexer,
        Arc::clone(&storage),
        run_cmd.liquidate_contract_address,
    );

    ServiceGroup::default()
        .with_critical(oracle_service)
        .with_critical(indexer_service)
        .with_critical(monitoring_service)
        .start_and_drive_to_end()
        .await?;

    Ok(())
}

/// Prints information about the bot parameters.
fn print_app_title() {
    println!("\n
██╗   ██╗███████╗███████╗██╗   ██╗    ██╗     ██╗ ██████╗ ██╗   ██╗██╗██████╗  █████╗ ████████╗ ██████╗ ██████╗
██║   ██║██╔════╝██╔════╝██║   ██║    ██║     ██║██╔═══██╗██║   ██║██║██╔══██╗██╔══██╗╚══██╔══╝██╔═══██╗██╔══██╗
██║   ██║█████╗  ███████╗██║   ██║    ██║     ██║██║   ██║██║   ██║██║██║  ██║███████║   ██║   ██║   ██║██████╔╝
╚██╗ ██╔╝██╔══╝  ╚════██║██║   ██║    ██║     ██║██║▄▄ ██║██║   ██║██║██║  ██║██╔══██║   ██║   ██║   ██║██╔══██╗
 ╚████╔╝ ███████╗███████║╚██████╔╝    ███████╗██║╚██████╔╝╚██████╔╝██║██████╔╝██║  ██║   ██║   ╚██████╔╝██║  ██║
  ╚═══╝  ╚══════╝╚══════╝ ╚═════╝     ╚══════╝╚═╝ ╚══▀▀═╝  ╚═════╝ ╚═╝╚═════╝ ╚═╝  ╚═╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝

  -----------------------------------------------------
  ");
}
