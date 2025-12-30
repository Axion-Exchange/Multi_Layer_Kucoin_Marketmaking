//! Exchange Abstraction Layer for Multi-Exchange Support
//! 
//! Traits that define the interface for exchange connectors.
//! Each exchange implements these traits to provide a unified API.

use async_trait::async_trait;
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::broadcast;

// ======================= TYPES =======================

/// Order identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderId {
    pub exchange_id: Option<String>,
    pub client_id: String,
}

/// Order side
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

/// Order type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
    PostOnly,
}

/// Order request for placing new orders
#[derive(Debug, Clone)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Option<f64>,
    pub size: f64,
    pub client_id: String,
}

/// Order status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Pending,
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

/// Order info returned from exchange
#[derive(Debug, Clone)]
pub struct Order {
    pub id: OrderId,
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub filled_size: f64,
    pub status: OrderStatus,
}

/// Fill/execution info
#[derive(Debug, Clone)]
pub struct Fill {
    pub order_id: OrderId,
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub fee: f64,
    pub fee_currency: String,
    pub timestamp_ms: u64,
}

/// Account balance
#[derive(Debug, Clone)]
pub struct Balance {
    pub currency: String,
    pub available: f64,
    pub locked: f64,
}

/// Order book update
#[derive(Debug, Clone)]
pub struct BookUpdate {
    pub symbol: String,
    pub best_bid: f64,
    pub best_ask: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub timestamp_ms: u64,
}

// ======================= TRAITS =======================

/// Core exchange connector trait
#[async_trait]
pub trait Exchange: Send + Sync {
    /// Exchange name (e.g., "kucoin", "binance")
    fn name(&self) -> &str;
    
    /// Check if connected
    fn is_connected(&self) -> bool;
    
    /// Place an order
    async fn place_order(&self, req: OrderRequest) -> Result<OrderId>;
    
    /// Cancel an order by ID
    async fn cancel_order(&self, symbol: &str, order_id: &OrderId) -> Result<()>;
    
    /// Cancel all orders for a symbol
    async fn cancel_all_orders(&self, symbol: &str) -> Result<()>;
    
    /// Modify an existing order (atomic cancel + replace)
    async fn modify_order(&self, symbol: &str, order_id: &OrderId, new_price: Option<f64>, new_size: Option<f64>) -> Result<OrderId>;
    
    /// Get current balances
    async fn get_balances(&self) -> Result<HashMap<String, Balance>>;
    
    /// Get open orders for a symbol
    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<Order>>;
}

/// Market data feed trait
#[async_trait]
pub trait MarketDataFeed: Send + Sync {
    /// Feed name
    fn name(&self) -> &str;
    
    /// Subscribe to book updates for a symbol
    fn subscribe(&self, symbol: &str) -> broadcast::Receiver<BookUpdate>;
    
    /// Get current best bid for a symbol
    fn best_bid(&self, symbol: &str) -> Option<f64>;
    
    /// Get current best ask for a symbol
    fn best_ask(&self, symbol: &str) -> Option<f64>;
    
    /// Get current mid price for a symbol
    fn mid_price(&self, symbol: &str) -> Option<f64> {
        match (self.best_bid(symbol), self.best_ask(symbol)) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            _ => None,
        }
    }
}

/// Fill stream trait for receiving execution notifications
#[async_trait]
pub trait FillStream: Send + Sync {
    /// Subscribe to fill notifications
    fn subscribe(&self) -> broadcast::Receiver<Fill>;
}

/// Combined connector with all capabilities
pub trait ExchangeConnector: Exchange + MarketDataFeed + FillStream {}

// Blanket implementation
impl<T: Exchange + MarketDataFeed + FillStream> ExchangeConnector for T {}
