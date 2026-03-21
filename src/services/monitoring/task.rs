use std::sync::Arc;

use crate::services::indexer::EventMetadata;
use pragma_common::{
    services::{Service, ServiceRunner},
    starknet::FallbackProvider,
};
use tokio::sync::{mpsc, oneshot};

use starknet::core::types::Felt;

use crate::{
    services::{indexer::PositionDelta, monitoring::MonitoringService},
    storage::Storage,
    types::account::StarknetAccount,
};

pub struct MonitoringTask {
    account: StarknetAccount,
    provider: FallbackProvider,
    rx_from_indexer: Option<mpsc::UnboundedReceiver<(EventMetadata, PositionDelta)>>,
    wait_for_indexer: Option<oneshot::Receiver<()>>,
    storage: Arc<Storage>,
    liquidate_contract_address: Felt,
}

impl MonitoringTask {
    pub fn new(
        account: StarknetAccount,
        provider: FallbackProvider,
        rx_from_indexer: mpsc::UnboundedReceiver<(EventMetadata, PositionDelta)>,
        wait_for_indexer: oneshot::Receiver<()>,
        storage: Arc<Storage>,
        liquidate_contract_address: Felt,
    ) -> Self {
        Self {
            account,
            provider,
            rx_from_indexer: Some(rx_from_indexer),
            wait_for_indexer: Some(wait_for_indexer),
            storage,
            liquidate_contract_address,
        }
    }
}

#[async_trait::async_trait]
impl Service for MonitoringTask {
    async fn start<'a>(&mut self, mut runner: ServiceRunner<'a>) -> anyhow::Result<()> {
        let account = self.account.clone();
        let provider = self.provider.clone();
        let rx_from_indexer = self
            .rx_from_indexer
            .take()
            .expect("MonitoringTask cannot be launched twice");
        let wait_for_indexer = self
            .wait_for_indexer
            .take()
            .expect("MonitoringTask cannot be launched twice");
        let storage = Arc::clone(&self.storage);
        let liquidate_contract_address = self.liquidate_contract_address;

        runner.spawn_loop(move |ctx| async move {
            let monitoring_service = MonitoringService::new(
                provider,
                account,
                rx_from_indexer,
                wait_for_indexer,
                storage,
                liquidate_contract_address,
            );
            if let Some(result) = ctx
                .run_until_cancelled(monitoring_service.run_forever())
                .await
            {
                result?;
            }

            anyhow::Ok(())
        });

        Ok(())
    }
}
