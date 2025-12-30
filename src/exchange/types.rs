//! KuCoin exchange types
//!
//! Core types for orders, fills, and market data.

use serde::{Deserialize, Serialize};

// ======================= ENDPOINTS =======================

/// KuCoin endpoint configuration with colocation support
#[derive(Debug, Clone)]
pub struct KucoinEndpoints {
    /// REST API base URL
    pub rest_url: String,
    /// Public WebSocket URL (orderbook)
    pub ws_public_url: String,
    /// Private WebSocket URL (orders)
    pub ws_private_url: String,
}

impl KucoinEndpoints {
    /// Standard endpoints (normal latency)
    pub fn standard() -> Self {
        Self {
            rest_url: "https://api.kucoin.com".to_string(),
            ws_public_url: "wss://ws-api-spot.kucoin.com".to_string(),
            ws_private_url: "wss://wsapi.kucoin.com".to_string(),
        }
    }

    /// Colocation endpoints (ultra-low latency)
    pub fn colocation() -> Self {
        Self {
            rest_url: "https://jvqklyxz.kucoin.com".to_string(),
            ws_public_url: "wss://wzxqoklm.kucoin.com".to_string(),
            ws_private_url: "wss://fgtyhceu.kucoin.com/v1/priv".to_string(),
        }
    }
}

// ======================= ORDER SIDE =======================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }

    pub fn opposite(&self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

// ======================= ORDER TYPE =======================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderType {
    Limit,
    Market,
}

// ======================= TIME IN FORCE =======================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    GTC,  // Good Till Cancelled
    GTT,  // Good Till Time
    IOC,  // Immediate or Cancel
    FOK,  // Fill or Kill
}

impl Default for TimeInForce {
    fn default() -> Self {
        TimeInForce::GTC
    }
}

// ======================= ORDER =======================

/// Order to place on KuCoin
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderRequest {
    pub client_oid: String,
    pub side: Side,
    #[serde(rename = "type")]
    pub order_type: OrderType,
    pub symbol: String,
    pub price: String,
    pub size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<TimeInForce>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hidden: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iceberg: Option<bool>,
}

impl OrderRequest {
    /// Create a new limit order (market maker)
    pub fn limit(
        client_oid: String,
        symbol: String,
        side: Side,
        price: f64,
        size: f64,
        post_only: bool,
    ) -> Self {
        Self {
            client_oid,
            side,
            order_type: OrderType::Limit,
            symbol,
            price: format!("{:.8}", price),
            size: format!("{:.8}", size),
            time_in_force: Some(TimeInForce::GTC),
            post_only: Some(post_only),
            hidden: None,
            iceberg: None,
        }
    }
}

// ======================= ORDER RESPONSE =======================

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderResponse {
    pub order_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiResponse<T> {
    pub code: String,
    pub data: Option<T>,
    pub msg: Option<String>,
}

// ======================= CANCEL =======================

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelRequest {
    pub order_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelResponse {
    pub cancelled_order_ids: Vec<String>,
}

// ======================= FILL =======================

#[derive(Debug, Clone)]
pub struct Fill {
    pub order_id: String,
    pub client_oid: String,
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub fee: f64,
    pub fee_currency: String,
    pub timestamp: u64,
}

// ======================= ORDERBOOK =======================

#[derive(Debug, Clone, Default)]
pub struct OrderBook {
    pub symbol: String,
    pub bids: Vec<(f64, f64)>, // (price, size)
    pub asks: Vec<(f64, f64)>, // (price, size)
    pub sequence: u64,
    pub timestamp: u64,
}

impl OrderBook {
    pub fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids.first().cloned()
    }

    pub fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks.first().cloned()
    }

    pub fn mid(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some((bid, _)), Some((ask, _))) => Some((bid + ask) / 2.0),
            _ => None,
        }
    }

    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some((bid, _)), Some((ask, _))) => Some(ask - bid),
            _ => None,
        }
    }

    pub fn spread_bps(&self) -> Option<f64> {
        match (self.spread(), self.mid()) {
            (Some(spread), Some(mid)) if mid > 0.0 => Some(spread / mid * 10_000.0),
            _ => None,
        }
    }
}

// ======================= BALANCE =======================

#[derive(Debug, Clone, Deserialize)]
pub struct Balance {
    pub currency: String,
    pub available: String,
    pub holds: String,
}

// ======================= WS TOKEN =======================

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsToken {
    pub token: String,
    pub instance_servers: Vec<WsInstance>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsInstance {
    pub endpoint: String,
    pub encrypt: bool,
    pub protocol: String,
    pub ping_interval: u64,
    pub ping_timeout: u64,
}
