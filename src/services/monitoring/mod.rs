pub mod ekubo;
pub mod task;

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rust_decimal_macros::dec;

use crate::services::indexer::EventMetadata;
use crate::storage::Storage;
use crate::types::vesu_client::VesuClient;
use pragma_common::starknet::FallbackProvider;
use starknet::core::types::Felt;
use tokio::sync::{mpsc, oneshot};

use crate::bindings::liquidate::Liquidate;
use crate::services::indexer::PositionDelta;
use crate::services::oracle::vesu_prices::VESU_PRICES;
use crate::types::account::StarknetSingleOwnerAccount;
use crate::types::pool::PoolName;
use crate::types::{account::StarknetAccount, position::VesuPosition};

pub struct MonitoringService {
    pub vesu_client: Arc<VesuClient>,
    pub rx_from_indexer: mpsc::UnboundedReceiver<(EventMetadata, PositionDelta)>,
    pub current_positions: HashMap<(PoolName, String), VesuPosition>,
    wait_for_indexer: Option<oneshot::Receiver<()>>,
    liquidate_contract: Arc<Liquidate<StarknetSingleOwnerAccount>>,
    account: StarknetAccount,
    storage: Arc<Storage>,
    last_summary_log: Instant,
    near_liquidation_cooldowns: HashMap<String, Instant>,
}

impl MonitoringService {
    pub fn new(
        provider: FallbackProvider,
        account: StarknetAccount,
        rx_from_indexer: mpsc::UnboundedReceiver<(EventMetadata, PositionDelta)>,
        wait_for_indexer: oneshot::Receiver<()>,
        storage: Arc<Storage>,
        liquidate_contract_address: Felt,
    ) -> Self {
        let current_positions = match storage.load_positions() {
            Ok(positions) => {
                if !positions.is_empty() {
                    tracing::info!(
                        "[🔭 Monitoring] Restored {} positions from storage",
                        positions.len()
                    );
                }
                positions
            }
            Err(e) => {
                tracing::warn!(
                    "[🔭 Monitoring] Failed to load positions from storage: {e}. Starting fresh."
                );
                HashMap::new()
            }
        };

        Self {
            vesu_client: Arc::new(VesuClient::new(provider)),
            rx_from_indexer,
            current_positions,
            wait_for_indexer: Some(wait_for_indexer),
            liquidate_contract: Arc::new(Liquidate::new(
                liquidate_contract_address,
                account.0.clone(),
            )),
            account,
            storage,
            last_summary_log: Instant::now(),
            near_liquidation_cooldowns: HashMap::new(),
        }
    }

    pub async fn run_forever(mut self) -> anyhow::Result<()> {
        tracing::info!("[🔭 Monitoring] Waiting for first vesu prices");
        VESU_PRICES.wait_for_first_prices().await;
        tracing::info!("[🔭 Monitoring] 😁 Got first prices!");

        let wait_for_indexer = self
            .wait_for_indexer
            .take()
            .expect("wait_for_indexer should be present in the Option. The task is ran only once!");

        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                maybe_msg = self.rx_from_indexer.recv() => {
                    if let Some((metadata, event)) = maybe_msg {
                        let pool = PoolName::try_from(&metadata.from_address)?;
                        let position_key = Self::compute_position_key(metadata.from_address, &event);

                        if let Some(position) = self.current_positions.get_mut(&(pool, position_key.clone())) {
                            position.update_from_delta(event);
                            self.persist_position(&pool, &position_key);
                        } else {
                            match VesuPosition::new(&metadata, &self.vesu_client, event).await {
                                Ok(position) => {
                                    let pos_id = position.position_id();
                                    self.current_positions.insert((pool, pos_id.clone()), position);
                                    self.persist_position(&pool, &pos_id);
                                }
                                Err(e) => {
                                    tracing::error!("[🔭 Monitoring] Could not create position: {e}");
                                }
                            };
                        }

                        let to_close = if let Some(position) = self.current_positions.get(&(pool, position_key.clone())) {
                            position.is_closed()
                        } else {
                            false
                        };

                        if to_close {
                            self.current_positions.remove(&(pool, position_key.clone()));
                            self.near_liquidation_cooldowns.remove(&position_key);
                            if let Err(e) = self.storage.remove_position(&pool, &position_key) {
                                tracing::warn!("[🔭 Monitoring] Failed to remove position from storage: {e}");
                            }
                        }
                    }
                },
                _ = interval.tick() => {
                    if wait_for_indexer.is_empty() || !self.rx_from_indexer.is_empty() {
                        continue;
                    }

                    let now = Instant::now();
                    let mut near_liquidation_count = 0u32;
                    const NEAR_LIQUIDATION_COOLDOWN: Duration = Duration::from_secs(60);

                    let mut to_liquidate: Vec<VesuPosition> = Vec::new();

                    for p in self.current_positions.values() {
                        if p.is_closed() {
                            continue;
                        }

                        if p.is_liquidable() {
                            to_liquidate.push(p.clone());
                            continue;
                        }

                        if p.is_near_liquidation() {
                            near_liquidation_count += 1;
                            let pos_id = p.position_id();
                            let on_cooldown = self
                                .near_liquidation_cooldowns
                                .get(&pos_id)
                                .is_some_and(|last| now.duration_since(*last) < NEAR_LIQUIDATION_COOLDOWN);
                            if !on_cooldown {
                                tracing::warn!(
                                    "[🔭 Monitoring] ⚠️ #{} {}/{} ({}) LTV {:.2}%/{:.2}% | debt ${:.0}",
                                    pos_id,
                                    p.collateral.currency,
                                    p.debt.currency,
                                    p.pool_name,
                                    p.ltv() * dec!(100),
                                    p.lltv * dec!(100),
                                    p.debt_value_in_usd(),
                                );
                                self.near_liquidation_cooldowns.insert(pos_id, now);
                            }
                        }
                    }

                    to_liquidate.sort_by_key(|p| std::cmp::Reverse(p.debt_value_in_usd()));

                    for position in &to_liquidate {
                        Self::liquidate_position(&self.account, &self.liquidate_contract, position).await;
                    }

                    self.log_summary(now, near_liquidation_count);
                }
            }
        }
    }

    fn persist_position(&self, pool: &PoolName, position_key: &str) {
        if let Some(position) = self
            .current_positions
            .get(&(*pool, position_key.to_string()))
            && let Err(e) = self.storage.save_position(pool, position_key, position)
        {
            tracing::warn!("[🔭 Monitoring] Failed to persist position: {e}");
        }
    }

    fn compute_position_key(from_address: Felt, position_event: &PositionDelta) -> String {
        let mut hasher = std::hash::DefaultHasher::new();
        vec![
            from_address,
            position_event.collateral_address,
            position_event.debt_address,
            position_event.user_address,
        ]
        .hash(&mut hasher);
        hasher.finish().to_string()
    }

    fn log_summary(&mut self, now: Instant, near_liquidation_count: u32) {
        const SUMMARY_INTERVAL: Duration = Duration::from_secs(30);

        if now.duration_since(self.last_summary_log) < SUMMARY_INTERVAL {
            return;
        }

        let active = self
            .current_positions
            .values()
            .filter(|p| !p.is_closed())
            .count();

        tracing::info!(
            "[🔭 Monitoring] {} active positions | {} near liquidation",
            active,
            near_liquidation_count,
        );

        self.last_summary_log = now;
    }

    async fn liquidate_position(
        account: &StarknetAccount,
        liquidate_contract: &Arc<Liquidate<StarknetSingleOwnerAccount>>,
        position: &VesuPosition,
    ) {
        tracing::info!("[🔭 Monitoring] 🔫 Liquidating {position}");
        let started_at = std::time::Instant::now();

        let liquidation_tx = match position
            .get_vesu_liquidate_tx(liquidate_contract, &account.account_address())
            .await
        {
            Ok(tx) => tx,
            Err(e) => {
                tracing::error!("[🔭 Monitoring] Failed to build liquidation TX: {e}");
                return;
            }
        };

        match account.execute_txs(&[liquidation_tx]).await {
            Ok(tx_hash) => {
                tracing::info!(
                    "[🔭 Monitoring] ✅ Liquidated #{}! (tx {tx_hash:#064x}) - ⌛ {:?}",
                    position.position_id(),
                    started_at.elapsed()
                );
            }
            Err(e) => {
                if e.to_string().contains("not-undercollateralized") {
                    tracing::warn!("[🔭 Monitoring] Position was not undercollateralized");
                } else {
                    tracing::error!(error = %e, "[🔭 Monitoring] 😨 Could not liquidate position");
                }
            }
        }
    }
}
