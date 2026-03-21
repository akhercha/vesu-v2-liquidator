pub mod task;

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use apibara_dna_protocol::dna::stream::DataFinality;
use apibara_dna_sdk::{
    StaticBearerToken, StreamClient, StreamDataRequestBuilder,
    proto::{Cursor, DnaMessage},
    starknet::{Block, Event, EventFilterBuilder, FieldElement, FilterBuilder},
};
use num_bigint::BigUint;
use pragma_common::starknet::fallback_provider::FallbackProvider;
use prost::Message;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use starknet::core::types::{BlockId, BlockTag, Felt, FunctionCall};
use starknet::macros::selector;
use starknet::providers::Provider;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tonic::transport::Uri;

use crate::storage::Storage;
use crate::types::{
    currency::Currency,
    pool::{PoolDetails, PoolName},
};

const MODIFY_POSITION_KEY: Felt = selector!("ModifyPosition");
const LIQUIDATE_POSITION_KEY: Felt = selector!("LiquidatePosition");

const SCALE_1E18: Decimal = dec!(1e18);

#[derive(Debug, Clone)]
pub struct EventMetadata {
    pub from_address: Felt,
    pub block_number: u64,
}

#[derive(Debug, Clone)]
pub struct PositionDelta {
    pub collateral_address: Felt,
    pub debt_address: Felt,
    pub user_address: Felt,
    pub collateral_delta: Decimal,
    pub debt_delta: Decimal,
}

pub struct IndexerService {
    pub current_block: u64,
    pub apibara_api_key: String,
    pub apibara_dna_url: String,
    pub provider: FallbackProvider,
    pub tx_to_monitoring: mpsc::UnboundedSender<(EventMetadata, PositionDelta)>,
    meet_with_monitoring: Option<oneshot::Sender<()>>,
    scale_cache: HashMap<Felt, Decimal>,
    storage: Arc<Storage>,
}

impl IndexerService {
    pub fn new(
        starting_block: u64,
        apibara_api_key: String,
        apibara_dna_url: String,
        provider: FallbackProvider,
        tx_to_monitoring: mpsc::UnboundedSender<(EventMetadata, PositionDelta)>,
        meet_with_monitoring: oneshot::Sender<()>,
        storage: Arc<Storage>,
    ) -> Self {
        Self {
            current_block: starting_block,
            apibara_api_key,
            apibara_dna_url,
            provider,
            tx_to_monitoring,
            meet_with_monitoring: Some(meet_with_monitoring),
            scale_cache: HashMap::new(),
            storage,
        }
    }

    async fn run_forever(&mut self) -> Result<()> {
        let filter = Self::build_filter();

        let stream_request = StreamDataRequestBuilder::new()
            .with_starting_cursor(Cursor::new_with_block_number(self.current_block))
            .with_finality(DataFinality::Pending)
            .add_filter(filter)
            .build();

        let url: Uri = self.apibara_dna_url.parse()?;

        let mut client = StreamClient::builder()
            .with_bearer_token_provider(Arc::new(StaticBearerToken::new(
                self.apibara_api_key.clone(),
            )))
            .connect(url)
            .await
            .map_err(|e| anyhow::anyhow!("Could not connect to Apibara DNA: {e}"))?;

        let mut stream = client
            .stream_data(stream_request)
            .await
            .map_err(|e| anyhow::anyhow!("Could not start Apibara DNA stream: {e}"))?;

        tracing::info!(
            "[🔢 Indexer] 🔌 Connected to Vesu! (from block {})",
            self.current_block
        );

        let mut reached_live = false;

        loop {
            match stream.try_next().await {
                Ok(Some(msg)) => match msg {
                    DnaMessage::Data(data) => {
                        const DATA_PRODUCTION_LIVE: i32 = 2;
                        if !reached_live && data.production == DATA_PRODUCTION_LIVE {
                            reached_live = true;
                            tracing::info!(
                                "[🔢 Indexer] 🥳 Vesu indexer reached the tip of the chain!"
                            );
                            if let Some(meet) = self.meet_with_monitoring.take() {
                                meet.send(()).expect("Rendezvous from Indexer dropped?");
                            }
                        }

                        for block_bytes in data.data {
                            let block = Block::decode(block_bytes)?;
                            let block_number =
                                block.header.as_ref().map(|h| h.block_number).unwrap_or(0);

                            for event in block.events {
                                if let Err(e) = self.process_event(block_number, event).await {
                                    tracing::warn!(
                                        "[🔢 Indexer] Error processing event at block {block_number}: {e}"
                                    );
                                }
                            }
                        }

                        if let Err(e) = self.storage.set_last_block(self.current_block) {
                            tracing::warn!("[🔢 Indexer] Failed to persist last block: {e}");
                        }
                    }
                    DnaMessage::Invalidate(invalidated) => {
                        if let Some(cursor) = invalidated.cursor {
                            tracing::warn!(
                                "[🔢 Indexer] Received invalidate at block {}",
                                cursor.order_key
                            );
                        }
                    }
                    DnaMessage::Finalize(_)
                    | DnaMessage::Heartbeat(_)
                    | DnaMessage::SystemMessage(_) => {}
                },
                Ok(None) => continue,
                Err(e) => {
                    anyhow::bail!("[🔢 Indexer] Fatal error while streaming: {e}");
                }
            }
        }
    }

    async fn process_event(&mut self, block_number: u64, event: Event) -> Result<()> {
        let from_address = match event.from_address.as_ref() {
            Some(addr) => apibara_field_as_felt(addr),
            None => return Ok(()),
        };

        let selector = match event.keys.first() {
            Some(key) => apibara_field_as_felt(key),
            None => return Ok(()),
        };

        if selector != MODIFY_POSITION_KEY && selector != LIQUIDATE_POSITION_KEY {
            return Ok(());
        }

        let collateral_address = event
            .keys
            .get(1)
            .map(apibara_field_as_felt)
            .context("Missing collateral address key")?;

        let debt_address = event
            .keys
            .get(2)
            .map(apibara_field_as_felt)
            .context("Missing debt address key")?;

        let user_address = event
            .keys
            .get(3)
            .map(apibara_field_as_felt)
            .context("Missing user address key")?;

        let data: Vec<Felt> = event.data.iter().map(apibara_field_as_felt).collect();

        let mut collateral_delta =
            parse_signed_i256(&data, 0).context("Failed to parse collateral_delta")?;
        let mut debt_delta = parse_signed_i256(&data, 6).context("Failed to parse debt_delta")?;

        if let Some(scale_divisor) = self.get_asset_scale(from_address, collateral_address).await
            && !scale_divisor.is_zero()
        {
            collateral_delta /= scale_divisor;
        }
        if let Some(scale_divisor) = self.get_asset_scale(from_address, debt_address).await
            && !scale_divisor.is_zero()
        {
            debt_delta /= scale_divisor;
        }

        self.current_block = block_number + 1;
        self.tx_to_monitoring.send((
            EventMetadata {
                from_address,
                block_number,
            },
            PositionDelta {
                collateral_address,
                debt_address,
                user_address,
                collateral_delta,
                debt_delta,
            },
        ))?;

        Ok(())
    }

    async fn get_asset_scale(
        &mut self,
        pool_address: Felt,
        asset_address: Felt,
    ) -> Option<Decimal> {
        if let Some(scale) = self.scale_cache.get(&asset_address) {
            return Some(*scale);
        }

        match self.fetch_asset_scale(pool_address, asset_address).await {
            Ok(scale) => {
                self.scale_cache.insert(asset_address, scale);
                Some(scale)
            }
            Err(e) => {
                tracing::warn!("Failed to fetch scale for asset {:#x}: {e}", asset_address);
                None
            }
        }
    }

    async fn fetch_asset_scale(&self, pool_address: Felt, asset_address: Felt) -> Result<Decimal> {
        let call_data = FunctionCall {
            contract_address: pool_address,
            entry_point_selector: selector!("asset_config"),
            calldata: vec![asset_address],
        };

        let result = self
            .provider
            .call(call_data, BlockId::Tag(BlockTag::Latest))
            .await
            .with_context(|| {
                format!("RPC call to asset_config failed for asset {asset_address:#x}")
            })?;

        if result.len() < 12 {
            anyhow::bail!(
                "asset_config result too short: expected >= 12 felts, got {}",
                result.len()
            );
        }

        let scale_raw = felt_pair_to_decimal(&result[10], &result[11]);
        let scale = if scale_raw.is_zero() {
            Decimal::ONE
        } else {
            scale_raw
        };

        Ok(scale / SCALE_1E18)
    }

    fn build_filter() -> apibara_dna_sdk::starknet::Filter {
        let modify_key = felt_as_apibara_field(&MODIFY_POSITION_KEY);
        let liquidate_key = felt_as_apibara_field(&LIQUIDATE_POSITION_KEY);

        let mut filter = FilterBuilder::new();

        for pool_details in Self::monitored_pools() {
            let pool_fe = felt_as_apibara_field(&pool_details.pool_address.0);
            let coll_fe = felt_as_apibara_field(&pool_details.collateral_address.0);
            let debt_fe = felt_as_apibara_field(&pool_details.debt_address.0);

            filter = filter
                .add_event(
                    EventFilterBuilder::single_contract(pool_fe)
                        .with_keys(false, vec![Some(modify_key), Some(coll_fe), Some(debt_fe)])
                        .with_transaction()
                        .build(),
                )
                .add_event(
                    EventFilterBuilder::single_contract(pool_fe)
                        .with_keys(
                            false,
                            vec![Some(liquidate_key), Some(coll_fe), Some(debt_fe)],
                        )
                        .with_transaction()
                        .build(),
                );
        }

        filter.build()
    }

    fn monitored_pools() -> HashSet<PoolDetails> {
        [
            PoolName::Re7USDCCore.pool_details(Currency::uniBTC, Currency::USDC),
            PoolName::Re7USDCCore.pool_details(Currency::LBTC, Currency::USDC),
            PoolName::Re7USDCCore.pool_details(Currency::tBTC, Currency::USDC),
            PoolName::Re7USDCCore.pool_details(Currency::solvBTC, Currency::USDC),
            PoolName::Re7USDCCore.pool_details(Currency::xWBTC, Currency::USDC),
            PoolName::Re7USDCCore.pool_details(Currency::xLBTC, Currency::USDC),
            PoolName::Re7USDCCore.pool_details(Currency::xsBTC, Currency::USDC),
            PoolName::Re7USDCCore.pool_details(Currency::xtBTC, Currency::USDC),
            PoolName::Re7USDCCore.pool_details(Currency::WBTC, Currency::USDC),
            PoolName::Re7USDCCore.pool_details(Currency::LBTC, Currency::USDC_E),
            PoolName::Re7USDCCore.pool_details(Currency::tBTC, Currency::USDC_E),
            PoolName::Re7USDCCore.pool_details(Currency::solvBTC, Currency::USDC_E),
            PoolName::Re7USDCPrime.pool_details(Currency::WBTC, Currency::USDC),
            PoolName::Re7USDCPrime.pool_details(Currency::WBTC, Currency::USDC_E),
            PoolName::Re7xBTC.pool_details(Currency::xtBTC, Currency::solvBTC),
            PoolName::Re7xBTC.pool_details(Currency::mRe7BTC, Currency::solvBTC),
            PoolName::Re7xBTC.pool_details(Currency::xsBTC, Currency::solvBTC),
            PoolName::Re7xBTC.pool_details(Currency::xWBTC, Currency::solvBTC),
            PoolName::Re7xBTC.pool_details(Currency::xLBTC, Currency::solvBTC),
            PoolName::Re7xBTC.pool_details(Currency::xtBTC, Currency::tBTC),
            PoolName::Re7xBTC.pool_details(Currency::mRe7BTC, Currency::tBTC),
            PoolName::Re7xBTC.pool_details(Currency::xsBTC, Currency::tBTC),
            PoolName::Re7xBTC.pool_details(Currency::xWBTC, Currency::tBTC),
            PoolName::Re7xBTC.pool_details(Currency::xLBTC, Currency::tBTC),
            PoolName::Re7xBTC.pool_details(Currency::xtBTC, Currency::LBTC),
            PoolName::Re7xBTC.pool_details(Currency::mRe7BTC, Currency::LBTC),
            PoolName::Re7xBTC.pool_details(Currency::xsBTC, Currency::LBTC),
            PoolName::Re7xBTC.pool_details(Currency::xWBTC, Currency::LBTC),
            PoolName::Re7xBTC.pool_details(Currency::xLBTC, Currency::LBTC),
            PoolName::Re7xBTC.pool_details(Currency::xtBTC, Currency::WBTC),
            PoolName::Re7xBTC.pool_details(Currency::mRe7BTC, Currency::WBTC),
            PoolName::Re7xBTC.pool_details(Currency::xsBTC, Currency::WBTC),
            PoolName::Re7xBTC.pool_details(Currency::xWBTC, Currency::WBTC),
            PoolName::Re7xBTC.pool_details(Currency::xLBTC, Currency::WBTC),
            PoolName::Re7USDCFrontier.pool_details(Currency::YBTC_B, Currency::USDC),
            PoolName::Re7USDCFrontier.pool_details(Currency::YBTC_B, Currency::USDC_E),
            PoolName::Re7USDCStableCore.pool_details(Currency::mRe7YIELD, Currency::USDC),
            PoolName::Re7USDCStableCore.pool_details(Currency::sUSN, Currency::USDC),
            PoolName::Re7USDCStableCore.pool_details(Currency::mRe7YIELD, Currency::USDC_E),
            PoolName::Prime.pool_details(Currency::wstETH, Currency::ETH),
            PoolName::Prime.pool_details(Currency::WBTC, Currency::ETH),
            PoolName::Prime.pool_details(Currency::STRK, Currency::ETH),
            PoolName::Prime.pool_details(Currency::USDC, Currency::ETH),
            PoolName::Prime.pool_details(Currency::USDT, Currency::ETH),
            PoolName::Prime.pool_details(Currency::USDC_E, Currency::ETH),
            PoolName::Prime.pool_details(Currency::wstETH, Currency::STRK),
            PoolName::Prime.pool_details(Currency::WBTC, Currency::STRK),
            PoolName::Prime.pool_details(Currency::ETH, Currency::STRK),
            PoolName::Prime.pool_details(Currency::USDC, Currency::STRK),
            PoolName::Prime.pool_details(Currency::USDT, Currency::STRK),
            PoolName::Prime.pool_details(Currency::USDC_E, Currency::STRK),
            PoolName::Prime.pool_details(Currency::wstETH, Currency::USDC),
            PoolName::Prime.pool_details(Currency::WBTC, Currency::USDC),
            PoolName::Prime.pool_details(Currency::STRK, Currency::USDC),
            PoolName::Prime.pool_details(Currency::ETH, Currency::USDC),
            PoolName::Prime.pool_details(Currency::USDT, Currency::USDC),
            PoolName::Prime.pool_details(Currency::wstETH, Currency::USDT),
            PoolName::Prime.pool_details(Currency::WBTC, Currency::USDT),
            PoolName::Prime.pool_details(Currency::STRK, Currency::USDT),
            PoolName::Prime.pool_details(Currency::ETH, Currency::USDT),
            PoolName::Prime.pool_details(Currency::USDC, Currency::USDT),
            PoolName::Prime.pool_details(Currency::wstETH, Currency::WBTC),
            PoolName::Prime.pool_details(Currency::STRK, Currency::WBTC),
            PoolName::Prime.pool_details(Currency::ETH, Currency::WBTC),
            PoolName::Prime.pool_details(Currency::USDC, Currency::WBTC),
            PoolName::Prime.pool_details(Currency::USDT, Currency::WBTC),
            PoolName::Prime.pool_details(Currency::USDC_E, Currency::WBTC),
            PoolName::Prime.pool_details(Currency::WBTC, Currency::wstETH),
            PoolName::Prime.pool_details(Currency::STRK, Currency::wstETH),
            PoolName::Prime.pool_details(Currency::ETH, Currency::wstETH),
            PoolName::Prime.pool_details(Currency::USDC, Currency::wstETH),
            PoolName::Prime.pool_details(Currency::USDT, Currency::wstETH),
            PoolName::Prime.pool_details(Currency::USDC_E, Currency::wstETH),
            PoolName::Prime.pool_details(Currency::xSTRK, Currency::USDC),
            PoolName::Prime.pool_details(Currency::xSTRK, Currency::STRK),
            PoolName::Prime.pool_details(Currency::xSTRK, Currency::USDT),
            PoolName::Prime.pool_details(Currency::xSTRK, Currency::USDC_E),
            PoolName::Prime.pool_details(Currency::xWBTC, Currency::USDC),
            PoolName::Prime.pool_details(Currency::xWBTC, Currency::WBTC),
            PoolName::Prime.pool_details(Currency::xWBTC, Currency::USDT),
            PoolName::Prime.pool_details(Currency::xWBTC, Currency::USDC_E),
            PoolName::Prime.pool_details(Currency::USDT, Currency::USDC_E),
            PoolName::Prime.pool_details(Currency::WBTC, Currency::USDC_E),
            PoolName::Prime.pool_details(Currency::wstETH, Currency::USDC_E),
            PoolName::Prime.pool_details(Currency::STRK, Currency::USDC_E),
            PoolName::Prime.pool_details(Currency::ETH, Currency::USDC_E),
            PoolName::Prime.pool_details(Currency::USDC_E, Currency::USDT),
            PoolName::Re7ETH.pool_details(Currency::wstETH, Currency::ETH),
            PoolName::Re7STRK.pool_details(Currency::xSTRK, Currency::STRK),
            PoolName::ClearstarUSDCReactor.pool_details(Currency::sUSN, Currency::USDC),
            PoolName::ClearstarUSDCReactor.pool_details(Currency::solvBTC, Currency::USDC),
            PoolName::ClearstarUSDCReactor.pool_details(Currency::WBTC, Currency::USDC),
            PoolName::ClearstarUSDCReactor.pool_details(Currency::tBTC, Currency::USDC),
        ]
        .into()
    }
}

fn felt_as_apibara_field(value: &Felt) -> FieldElement {
    FieldElement::from_bytes(&value.to_bytes_be())
}

fn apibara_field_as_felt(value: &FieldElement) -> Felt {
    Felt::from_bytes_be(&value.to_bytes())
}

fn felt_to_u128(felt: &Felt) -> u128 {
    let bytes = felt.to_bytes_be();
    u128::from_be_bytes(bytes[16..32].try_into().expect("16 bytes for u128"))
}

fn felt_pair_to_decimal(low: &Felt, high: &Felt) -> Decimal {
    let low_u128 = felt_to_u128(low);
    let high_u128 = felt_to_u128(high);
    let big: BigUint = BigUint::from(high_u128) << 128 | BigUint::from(low_u128);
    Decimal::from_str(&big.to_string()).unwrap_or(Decimal::ZERO)
}

fn parse_signed_i256(data: &[Felt], offset: usize) -> Result<Decimal> {
    let low = data.get(offset).context("Missing i256 low part")?;
    let high = data.get(offset + 1).context("Missing i256 high part")?;
    let sign = data.get(offset + 2).context("Missing i256 sign flag")?;

    let low_u128 = felt_to_u128(low);
    let high_u128 = felt_to_u128(high);
    let is_negative = felt_to_u128(sign) != 0;

    let abs_value: BigUint = BigUint::from(high_u128) << 128 | BigUint::from(low_u128);
    let decimal =
        Decimal::from_str(&abs_value.to_string()).context("i256 value too large for Decimal")?;

    Ok(if is_negative { -decimal } else { decimal })
}
