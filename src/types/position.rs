use std::hash::Hash;
use std::hash::Hasher;
use std::str::FromStr;
use std::sync::Arc;

use crate::services::indexer::EventMetadata;
use crate::types::vesu_client::VesuClient;
use cainome::cairo_serde::U256;

use num_traits::Pow;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use serde::Serialize;
use starknet::core::types::Call;
use starknet::core::types::Felt;

use crate::bindings::liquidate::Liquidate;
use crate::bindings::liquidate::LiquidateParams;
use crate::config::onchain_assets::ONCHAIN_ASSETS;
use crate::services::indexer::PositionDelta;
use crate::services::monitoring::ekubo::get_ekubo_route;
use crate::types::account::StarknetSingleOwnerAccount;
use crate::types::currency::Currency;
use crate::types::pool::PoolName;

const VESU_SCALE: Decimal = dec!(18);

#[derive(Clone, Hash, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct VesuPosition {
    pub user_address: Felt,
    pub pool_name: PoolName,
    pub collateral: Asset,
    pub debt: Asset,
    pub lltv: Decimal,
}

impl VesuPosition {
    /// Creates a new Position from a Vesu Event.
    pub async fn new(
        event_metadata: &EventMetadata,
        vesu_client: &Arc<VesuClient>,
        event: PositionDelta,
    ) -> anyhow::Result<Self> {
        let mut new_position = Self {
            user_address: event.user_address,
            pool_name: PoolName::try_from(&event_metadata.from_address).expect(
                "Unsupported pool. Should never happen if we indexed the pools using `PoolName`",
            ),
            collateral: Asset::from_address(event.collateral_address),
            debt: Asset::from_address(event.debt_address),
            lltv: Decimal::ZERO,
        };

        new_position.update_lltv(vesu_client).await?;
        anyhow::ensure!(!new_position.lltv.is_zero(), "LLTV cannot be zero.");

        new_position.update_from_delta(event);

        Ok(new_position)
    }

    /// Given a new delta event, update the position.
    pub fn update_from_delta(&mut self, delta: PositionDelta) {
        let collateral_delta = scale(delta.collateral_delta, VESU_SCALE);
        self.collateral.apply_delta(collateral_delta);
        let debt_delta = scale(delta.debt_delta, VESU_SCALE);
        self.debt.apply_delta(debt_delta);
    }

    /// Updates the LLTV of the position.
    async fn update_lltv(&mut self, vesu_client: &Arc<VesuClient>) -> anyhow::Result<()> {
        let pair_config = vesu_client
            .pair_config(
                self.pool_name.pool_address(),
                self.collateral.address,
                self.debt.address,
                None,
            )
            .await?;

        self.lltv = pair_config.max_ltv;

        if pair_config.max_ltv.is_zero() {
            tracing::warn!(
                "For {} {}-{} ; max LTV is zero...?",
                self.pool_name,
                self.collateral.currency,
                self.debt.currency,
            );
        }

        Ok(())
    }

    /// Check if the current position is closed.
    pub fn is_closed(&self) -> bool {
        self.collateral.amount.is_zero() || self.collateral.amount.is_sign_negative()
    }

    /// Returns the position id.
    /// It is NOT unique accross multiple positions of the same pool & assets!
    pub fn position_id(&self) -> String {
        let mut hasher = std::hash::DefaultHasher::new();
        let data = vec![
            self.pool_name.pool_address(),
            self.collateral.address,
            self.debt.address,
            self.user_address,
        ];
        data.hash(&mut hasher);
        hasher.finish().to_string()
    }

    /// Computes the liquidation price in USD for the collateral asset.
    /// The position gets liquidated when the collateral price drops to this value.
    /// Formula: (debt_amount * debt_price) / (collateral_amount * lltv)
    pub fn liquidation_price(&self) -> Decimal {
        let debt_price = self.debt.currency.price();
        (self.debt.amount * debt_price) / (self.collateral.amount * self.lltv)
    }

    /// Returns the position value in usd.
    pub fn value_in_usd(&self) -> Decimal {
        let collateral_value = self.collateral_value_in_usd();
        let debt_value = self.debt_value_in_usd();
        collateral_value - debt_value
    }

    /// Returns the collateral value in usd.
    pub fn collateral_value_in_usd(&self) -> Decimal {
        let collateral_price = self.collateral.currency.price();
        self.collateral.amount * collateral_price
    }

    /// Returns the debt value in usd.
    pub fn debt_value_in_usd(&self) -> Decimal {
        let debt_price = self.debt.currency.price();
        self.debt.amount * debt_price
    }

    /// Returns the current LTV.
    pub fn ltv(&self) -> Decimal {
        self.debt_value_in_usd() / self.collateral_value_in_usd()
    }

    pub fn is_liquidable(&self) -> bool {
        if self.lltv.is_zero() {
            return false;
        }

        let ltv = self.ltv();
        if ltv.is_zero() {
            return !self.debt.amount.is_zero();
        }

        ltv >= self.lltv
    }

    pub fn is_near_liquidation(&self) -> bool {
        const THRESHOLD: Decimal = dec!(0.1);
        if self.lltv.is_zero() || self.is_closed() || self.collateral_value_in_usd().is_zero() {
            return false;
        }
        let ltv = self.ltv();
        ltv >= self.lltv - THRESHOLD && ltv < self.lltv
    }

    /// Returns the TX necessary to liquidate this position using the Vesu Liquidate
    /// contract.
    pub async fn get_vesu_liquidate_tx(
        &self,
        liquidate_contract: &Arc<Liquidate<StarknetSingleOwnerAccount>>,
        liquidator_address: &Felt,
    ) -> anyhow::Result<Call> {
        let (liquidate_swap, liquidate_swap_weights) = get_ekubo_route(
            self.debt.address,
            self.collateral.address,
            &self.debt.amount,
            self.debt.decimals,
        )
        .await?;

        let liquidate_params = LiquidateParams {
            pool: cainome::cairo_serde::ContractAddress(self.pool_name.pool_address()),
            collateral_asset: cainome::cairo_serde::ContractAddress(self.collateral.address),
            debt_asset: cainome::cairo_serde::ContractAddress(self.debt.address),
            user: cainome::cairo_serde::ContractAddress(self.user_address),
            recipient: cainome::cairo_serde::ContractAddress(*liquidator_address),
            min_collateral_to_receive: U256 { low: 0, high: 0 },
            debt_to_repay: U256 { low: 0, high: 0 },
            liquidate_swap,
            liquidate_swap_weights,
            liquidate_swap_limit_amount: u128::MAX,
            withdraw_swap: vec![],
            withdraw_swap_limit_amount: 0,
            withdraw_swap_weights: vec![],
        };

        Ok(liquidate_contract.liquidate_getcall(&liquidate_params))
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Asset {
    pub name: String,
    pub currency: Currency,
    pub address: Felt,
    pub decimals: Decimal,
    pub amount: Decimal,
}

impl Asset {
    pub fn from_address(address: Felt) -> Self {
        let config = &ONCHAIN_ASSETS[&address];

        let currency =
            Currency::from_str(&config.ticker).expect("Could not convert ticker -> Currency");

        Self {
            name: config.name.clone(),
            decimals: currency.d_decimals(),
            address: currency.address(),
            currency,
            amount: Decimal::ZERO,
        }
    }

    pub fn apply_delta(&mut self, amount_delta: Decimal) {
        self.amount += amount_delta;
    }
}

fn scale(nb: Decimal, scale: Decimal) -> Decimal {
    nb / Decimal::TEN.pow(scale)
}

impl std::fmt::Display for VesuPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Position #{} with {} {} of collateral and {} {} of debt",
            self.position_id(),
            self.collateral
                .amount
                .round_dp(self.collateral.decimals.try_into().expect("Must fit")),
            self.collateral.currency,
            self.debt
                .amount
                .round_dp(self.debt.decimals.try_into().expect("Must fit")),
            self.debt.currency,
        )
    }
}
