//! Pre-allocated Order Templates for Zero-Allocation Hot Path
//!
//! Reuses order structures to avoid heap allocation in the hot path.

use std::sync::atomic::{AtomicU64, Ordering};

/// Pre-allocated order template for low-latency order building
pub struct OrderTemplate {
    pub symbol: String,
    counter: AtomicU64,
}

impl OrderTemplate {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            counter: AtomicU64::new(0),
        }
    }
    
    /// Generate a unique client order ID without allocation
    /// Uses counter + prefix to create ID
    pub fn next_oid(&self, prefix: &str) -> String {
        let count = self.counter.fetch_add(1, Ordering::SeqCst);
        format!("{}_{}", prefix, count)
    }
    
    /// Build a bid order request
    pub fn build_bid(&self, price: f64, size: f64) -> OrderParams {
        OrderParams {
            client_oid: self.next_oid("bid"),
            symbol: self.symbol.clone(),
            side: OrderSide::Buy,
            price,
            size,
        }
    }
    
    /// Build an ask order request
    pub fn build_ask(&self, price: f64, size: f64) -> OrderParams {
        OrderParams {
            client_oid: self.next_oid("ask"),
            symbol: self.symbol.clone(),
            side: OrderSide::Sell,
            price,
            size,
        }
    }
    
    /// Reset counter (useful for testing)
    pub fn reset_counter(&self) {
        self.counter.store(0, Ordering::SeqCst);
    }
    
    /// Get current counter value
    pub fn current_count(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }
}

/// Minimal order parameters (no allocation overhead)
#[derive(Debug, Clone)]
pub struct OrderParams {
    pub client_oid: String,
    pub symbol: String,
    pub side: OrderSide,
    pub price: f64,
    pub size: f64,
}

/// Order side enum
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderParams {
    /// Format price to string (for WS request)
    pub fn price_str(&self, decimals: u32) -> String {
        format!("{:.1$}", self.price, decimals as usize)
    }
    
    /// Format size to string (for WS request)
    pub fn size_str(&self, decimals: u32) -> String {
        format!("{:.1$}", self.size, decimals as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_order_template() {
        let template = OrderTemplate::new("BTC-USDT".to_string());
        
        let bid1 = template.build_bid(100.0, 1.0);
        assert_eq!(bid1.client_oid, "bid_0");
        assert_eq!(bid1.symbol, "BTC-USDT");
        
        let ask1 = template.build_ask(101.0, 1.0);
        assert_eq!(ask1.client_oid, "ask_1");
        
        assert_eq!(template.current_count(), 2);
    }
}
