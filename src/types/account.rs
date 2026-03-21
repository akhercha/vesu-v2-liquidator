use std::path::PathBuf;

use anyhow::Result;
use pragma_common::starknet::FallbackProvider;
use starknet::{
    accounts::{Account, ExecutionEncoding, SingleOwnerAccount},
    core::{
        chain_id,
        types::{BlockId, BlockTag, Call, Felt},
    },
    signers::{LocalWallet, SigningKey},
};

use crate::cli::RunCmd;

const GAS_ESTIMATE_MULTIPLIER: u8 = 3;

pub type StarknetSingleOwnerAccount = SingleOwnerAccount<FallbackProvider, LocalWallet>;

#[derive(Debug, Clone)]
pub struct StarknetAccount(pub StarknetSingleOwnerAccount);

impl StarknetAccount {
    /// Creates a StarknetAccount from the CLI args
    pub fn from_cli(rpc_client: FallbackProvider, run_cmd: RunCmd) -> Result<StarknetAccount> {
        let account_builder = StarknetAccountBuilder::default()
            .as_account(run_cmd.account_params.account_address)
            .on_mainnet()
            .with_provider(rpc_client);

        if let Some(private_key) = run_cmd.account_params.private_key {
            account_builder.from_secret(private_key)
        } else {
            account_builder.from_keystore(
                run_cmd
                    .account_params
                    .keystore_path
                    .expect("Keystore is expected to exist if private key is not provided"),
                &run_cmd
                    .account_params
                    .keystore_password
                    .expect("Keystore is expected to exist if private key is not provided"),
            )
        }
    }

    /// Returns the account_address of the Account.
    pub fn account_address(&self) -> Felt {
        self.0.address()
    }

    /// Executes a set of transactions with boosted gas for priority inclusion.
    pub async fn execute_txs(&self, txs: &[Call]) -> Result<Felt> {
        let estimation = self
            .0
            .execute_v3(txs.to_vec())
            .estimate_fee()
            .await
            .map_err(|e| anyhow::anyhow!("Fee estimation failed: {e:?}"))?;

        let boosted_gas = estimation.l1_gas_consumed * GAS_ESTIMATE_MULTIPLIER as u64;
        let boosted_gas_price = estimation.l1_gas_price * GAS_ESTIMATE_MULTIPLIER as u128;

        let res = self
            .0
            .execute_v3(txs.to_vec())
            .l1_gas(boosted_gas)
            .l1_gas_price(boosted_gas_price)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!(format!("{:?}", e)))?;

        Ok(res.transaction_hash)
    }
}

#[derive(Debug, Default)]
pub struct StarknetAccountBuilder {
    account_address: Option<Felt>,
    chain_id: Option<Felt>,
    rpc_client: Option<FallbackProvider>,
}

impl StarknetAccountBuilder {
    pub fn new() -> Self {
        StarknetAccountBuilder::default()
    }

    pub fn on_mainnet(mut self) -> Self {
        self.chain_id = Some(chain_id::MAINNET);
        self
    }

    pub fn on_sepolia(mut self) -> Self {
        self.chain_id = Some(chain_id::SEPOLIA);
        self
    }
    pub fn as_account(mut self, account_address: Felt) -> Self {
        self.account_address = Some(account_address);
        self
    }

    pub fn with_provider(mut self, rpc_client: FallbackProvider) -> Self {
        self.rpc_client = Some(rpc_client);
        self
    }

    pub fn from_secret(self, private_key: Felt) -> Result<StarknetAccount> {
        let signing_key = SigningKey::from_secret_scalar(private_key);
        let signer = LocalWallet::from(signing_key);
        self.build(signer)
    }

    pub fn from_keystore(
        self,
        keystore_path: PathBuf,
        keystore_password: &str,
    ) -> Result<StarknetAccount> {
        let signing_key = SigningKey::from_keystore(keystore_path, keystore_password)?;
        let signer = LocalWallet::from(signing_key);
        self.build(signer)
    }

    fn build(self, signer: LocalWallet) -> Result<StarknetAccount> {
        let mut account = SingleOwnerAccount::new(
            self.rpc_client.unwrap(),
            signer,
            self.account_address.unwrap(),
            self.chain_id.unwrap(),
            ExecutionEncoding::New,
        );

        account.set_block_id(BlockId::Tag(BlockTag::Latest));

        Ok(StarknetAccount(account))
    }
}
