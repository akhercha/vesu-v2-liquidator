use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use heed::types::{SerdeBincode, Str};
use heed::{Database, Env, EnvOpenOptions};

use crate::types::pool::PoolName;
use crate::types::position::VesuPosition;

const MAX_DBS: u32 = 2;
const MAP_SIZE: usize = 100 * 1024 * 1024;

pub struct Storage {
    env: Env,
    metadata: Database<Str, SerdeBincode<u64>>,
    positions: Database<Str, SerdeBincode<VesuPosition>>,
}

impl Storage {
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)
            .with_context(|| format!("Failed to create storage directory: {}", path.display()))?;

        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(MAX_DBS)
                .map_size(MAP_SIZE)
                .open(path)
                .context("Failed to open LMDB environment")?
        };

        let mut txn = env.write_txn()?;
        let metadata = env.create_database(&mut txn, Some("metadata"))?;
        let positions = env.create_database(&mut txn, Some("positions"))?;
        txn.commit()?;

        tracing::info!("[💾 Storage] Opened at {}", path.display());

        Ok(Self {
            env,
            metadata,
            positions,
        })
    }

    pub fn get_last_block(&self) -> Result<Option<u64>> {
        let txn = self.env.read_txn()?;
        Ok(self.metadata.get(&txn, "last_block")?)
    }

    pub fn set_last_block(&self, block: u64) -> Result<()> {
        let mut txn = self.env.write_txn()?;
        self.metadata.put(&mut txn, "last_block", &block)?;
        txn.commit()?;
        Ok(())
    }

    pub fn load_positions(&self) -> Result<HashMap<(PoolName, String), VesuPosition>> {
        let txn = self.env.read_txn()?;
        let mut positions = HashMap::new();

        for result in self.positions.iter(&txn)? {
            let (key, position) = result?;
            if let Some((pool_name, position_id)) = parse_position_key(key) {
                positions.insert((pool_name, position_id), position);
            }
        }

        Ok(positions)
    }

    pub fn save_position(
        &self,
        pool_name: &PoolName,
        position_id: &str,
        position: &VesuPosition,
    ) -> Result<()> {
        let key = format_position_key(pool_name, position_id);
        let mut txn = self.env.write_txn()?;
        self.positions.put(&mut txn, &key, position)?;
        txn.commit()?;
        Ok(())
    }

    pub fn remove_position(&self, pool_name: &PoolName, position_id: &str) -> Result<()> {
        let key = format_position_key(pool_name, position_id);
        let mut txn = self.env.write_txn()?;
        self.positions.delete(&mut txn, &key)?;
        txn.commit()?;
        Ok(())
    }

    pub fn position_count(&self) -> Result<usize> {
        let txn = self.env.read_txn()?;
        Ok(self.positions.len(&txn)? as usize)
    }
}

fn format_position_key(pool_name: &PoolName, position_id: &str) -> String {
    format!("{pool_name}:{position_id}")
}

fn parse_position_key(key: &str) -> Option<(PoolName, String)> {
    let (pool_str, position_id) = key.split_once(':')?;
    let pool_name = pool_str.parse::<PoolName>().ok()?;
    Some((pool_name, position_id.to_string()))
}
