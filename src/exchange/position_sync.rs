//! Position Reconciliation - Syncs position with exchange on startup and periodically

use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use tracing::{info, warn};

use super::rest::KucoinRestClient as RestClient;

/// Position reconciler - keeps position in sync with exchange
pub struct PositionReconciler {
    rest_client: Arc<RestClient>,
    symbol: String,
    base_currency: String,
    initial_balance: f64,
    last_reconciled_position: f64,
    last_sync: Instant,
    sync_interval: Duration,
}

impl PositionReconciler {
    pub fn new(
        rest_client: Arc<RestClient>,
        symbol: String,
        initial_balance: f64,
    ) -> Self {
        let base_currency = symbol.split('-').next().unwrap_or("SOL").to_string();
        
        info!("[POSITION-SYNC] Initialized for {} with initial balance: {:.4} {}",
              symbol, initial_balance, base_currency);

        Self {
            rest_client,
            symbol,
            base_currency,
            initial_balance,
            last_reconciled_position: 0.0,
            last_sync: Instant::now(),
            sync_interval: Duration::from_secs(60),
        }
    }

    pub async fn get_exchange_position(&self) -> Result<f64> {
        let balance = self.rest_client.get_balance(&self.base_currency).await?;
        Ok(balance - self.initial_balance)
    }

    pub async fn reconcile(&mut self, local_position: f64) -> Result<(f64, f64, f64)> {
        let exchange_position = self.get_exchange_position().await?;
        let discrepancy = (exchange_position - local_position).abs();
        
        self.last_reconciled_position = exchange_position;
        self.last_sync = Instant::now();

        if discrepancy > 0.001 {
            warn!("[POSITION-SYNC] Discrepancy! Exchange: {:.4} | Local: {:.4}", 
                  exchange_position, local_position);
        } else {
            info!("[POSITION-SYNC] Position synced: {:.4}", exchange_position);
        }

        Ok((exchange_position, local_position, discrepancy))
    }

    pub fn should_sync(&self) -> bool {
        self.last_sync.elapsed() >= self.sync_interval
    }

    pub fn last_position(&self) -> f64 {
        self.last_reconciled_position
    }
}

pub async fn get_initial_balance(rest_client: &RestClient, symbol: &str) -> Result<f64> {
    let base_currency = symbol.split('-').next().unwrap_or("SOL");
    let balance = rest_client.get_balance(base_currency).await?;
    info!("[INIT] Initial {} balance: {:.4}", base_currency, balance);
    Ok(balance)
}
