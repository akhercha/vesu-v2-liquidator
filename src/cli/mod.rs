pub mod account;

use std::path::PathBuf;

use anyhow::{Result, anyhow};
use url::Url;

use crate::cli::account::AccountParams;

fn parse_url(s: &str) -> Result<Url> {
    s.parse()
        .map_err(|_| anyhow!("Could not convert {s} to Url"))
}

#[derive(Clone, Debug, clap::Parser)]
pub struct RunCmd {
    #[allow(missing_docs)]
    #[clap(flatten)]
    pub account_params: AccountParams,

    /// The rpc endpoint urls (comma-separated for fallback).
    #[clap(
        long,
        value_parser = parse_url,
        value_name = "RPC URL",
        env = "RPC_URL",
        num_args = 1..,
        value_delimiter = ','
    )]
    pub rpc_url: Vec<Url>,

    /// The block you want to start syncing from (overrides stored block).
    #[clap(long, short, value_name = "BLOCK NUMBER", env = "STARTING_BLOCK")]
    pub starting_block: Option<u64>,

    /// Apibara API Key for indexing.
    #[clap(long, value_name = "APIBARA API KEY", env = "APIBARA_API_KEY")]
    pub apibara_api_key: String,

    #[clap(
        long,
        value_name = "APIBARA DNA URL",
        env = "APIBARA_DNA_URL",
        default_value = "https://mainnet.starknet.a5a.ch"
    )]
    pub apibara_dna_url: String,

    #[clap(
        long,
        value_name = "LIQUIDATE CONTRACT ADDRESS",
        env = "LIQUIDATE_CONTRACT_ADDRESS",
        default_value = "0x6b895ba904fb8f02ed0d74e343161de48e611e9e771be4cc2c997501dbfb418"
    )]
    pub liquidate_contract_address: String,

    /// Path to the local storage directory.
    #[clap(
        long,
        value_name = "STORAGE PATH",
        env = "STORAGE_PATH",
        default_value = "./data/vesu-v2"
    )]
    pub storage_path: PathBuf,
}

impl RunCmd {
    pub fn validate(&mut self) -> Result<()> {
        self.account_params.validate()?;
        Ok(())
    }
}
