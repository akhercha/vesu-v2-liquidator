use std::str::FromStr;

use anyhow::Context;
use pragma_common::starknet::fallback_provider::FallbackProvider;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use starknet::core::types::{BlockId, BlockTag, Felt, FunctionCall};
use starknet::macros::selector;
use starknet::providers::Provider;

const SCALE: Decimal = dec!(1e18);

#[derive(Debug, Clone)]
pub struct PairConfig {
    pub max_ltv: Decimal,
    pub liquidation_factor: Decimal,
    pub debt_cap: Decimal,
}

#[derive(Debug)]
pub struct VesuClient {
    provider: FallbackProvider,
}

impl VesuClient {
    pub fn new(provider: FallbackProvider) -> Self {
        Self { provider }
    }

    pub async fn pair_config(
        &self,
        pool_address: Felt,
        collateral_address: Felt,
        debt_address: Felt,
        block_id: Option<BlockId>,
    ) -> anyhow::Result<PairConfig> {
        let block_id = block_id.unwrap_or(BlockId::Tag(BlockTag::Latest));

        let call_data = FunctionCall {
            contract_address: pool_address,
            entry_point_selector: selector!("pair_config"),
            calldata: vec![collateral_address, debt_address],
        };

        let result_felts = self
            .provider
            .call(call_data, block_id)
            .await
            .with_context(|| {
                format!(
                    "RPC call to pair_config failed for collateral {collateral_address:#x} and debt {debt_address:#x}",
                )
            })?;

        if result_felts.len() < 3 {
            anyhow::bail!(
                "pair_config result too short: expected >= 3 felts, got {}",
                result_felts.len()
            );
        }

        let max_ltv = felt_to_decimal(&result_felts[0])? / SCALE;
        let liquidation_factor = felt_to_decimal(&result_felts[1])? / SCALE;
        let debt_cap = felt_to_decimal(&result_felts[2])?;

        Ok(PairConfig {
            max_ltv,
            liquidation_factor,
            debt_cap,
        })
    }
}

fn felt_to_decimal(felt: &Felt) -> anyhow::Result<Decimal> {
    let bytes = felt.to_bytes_be();
    let value = u128::from_be_bytes(bytes[16..32].try_into().expect("16 bytes for u128"));
    Decimal::from_str(&value.to_string()).context("Felt value too large for Decimal")
}
