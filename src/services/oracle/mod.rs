pub mod task;
pub mod vesu_prices;

use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::Result;
use futures_util::future::join_all;
use num_traits::pow::Pow;
use pragma_common::starknet::fallback_provider::FallbackProvider;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use starknet::core::types::{BlockId, BlockTag, Felt, FunctionCall};
use starknet::macros::{felt_hex, selector};
use starknet::providers::Provider;

use crate::config::onchain_assets::OnchainAssetConfig;
use crate::services::oracle::vesu_prices::VESU_PRICES;

#[derive(Clone)]
pub struct OracleService {
    starknet_provider: FallbackProvider,
    last_heartbeat: Instant,
    update_cycles: u32,
}

impl OracleService {
    const PRICES_UPDATE_INTERVAL: Duration = Duration::from_secs(2);

    pub fn new(starknet_provider: FallbackProvider) -> Self {
        Self {
            starknet_provider,
            last_heartbeat: Instant::now(),
            update_cycles: 0,
        }
    }

    pub async fn run_forever(mut self) -> Result<()> {
        loop {
            self.update_prices().await?;
            self.update_cycles += 1;
            self.log_heartbeat();
            tokio::time::sleep(Self::PRICES_UPDATE_INTERVAL).await;
        }
    }

    fn log_heartbeat(&mut self) {
        const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

        let now = Instant::now();
        if now.duration_since(self.last_heartbeat) < HEARTBEAT_INTERVAL {
            return;
        }

        tracing::info!(
            "[🔮 Oracle] {} prices updated ({} cycles)",
            VESU_PRICES.0.len(),
            self.update_cycles,
        );

        self.update_cycles = 0;
        self.last_heartbeat = now;
    }

    async fn update_prices(&self) -> Result<()> {
        let assets: Vec<OnchainAssetConfig> = VESU_PRICES
            .0
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        let fetch_tasks = assets.into_iter().map(|asset| async move {
            let vesu_price = self.vesu_price_in_usd(&asset).await;
            (asset, vesu_price)
        });

        let results = join_all(fetch_tasks).await;

        for (asset, vesu_price_result) in results {
            if let Ok(vesu_price) = vesu_price_result {
                VESU_PRICES.0.insert(asset, vesu_price);
            }
        }

        Ok(())
    }

    async fn vesu_price_in_usd(&self, base_asset: &OnchainAssetConfig) -> Result<Decimal> {
        const VESU_ORACLE_ADDRESS: Felt =
            felt_hex!("0xfe4bfb1b353ba51eb34dff963017f94af5a5cf8bdf3dfc191c504657f3c05");

        const VESU_SCALE: Decimal = dec!(18);

        let price_request = FunctionCall {
            contract_address: VESU_ORACLE_ADDRESS,
            entry_point_selector: selector!("price"),
            calldata: vec![base_asset.address],
        };

        let call_result = self
            .starknet_provider
            .call(price_request, BlockId::Tag(BlockTag::PreConfirmed))
            .await?;

        // NOTE: Works for now since prices always fit in the low part.
        let asset_price_low = Decimal::from_str(&call_result[0].to_string())?;

        let is_valid = u128::from_str(&call_result[2].to_string())?;
        if is_valid == 0 {
            anyhow::bail!("Vesu price is not valid");
        }

        let asset_price = asset_price_low / Decimal::TEN.pow(VESU_SCALE);

        Ok(asset_price)
    }
}
