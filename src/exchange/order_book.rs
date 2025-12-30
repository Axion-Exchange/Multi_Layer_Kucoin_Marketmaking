//! Order Book with depth tracking and queue position estimation
//! Used for HFT market making to track bid/ask depth and estimate fill probability

use std::time::Instant;

/// Order book side
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BookSide {
    Bid,
    Ask,
}

/// A single price level in the order book
#[derive(Debug, Clone)]
pub struct PriceLevel {
    pub price: f64,
    pub size: f64,
}

/// Order book with depth tracking
#[derive(Debug, Clone)]
pub struct OrderBook {
    pub symbol: String,
    /// Bids sorted by price descending (best bid first)
    bids: Vec<PriceLevel>,
    /// Asks sorted by price ascending (best ask first)  
    asks: Vec<PriceLevel>,
    /// Timestamp of last update
    pub last_update: Instant,
    /// Sequence number for detecting gaps
    pub sequence: u64,
}

impl OrderBook {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            bids: Vec::with_capacity(50),
            asks: Vec::with_capacity(50),
            last_update: Instant::now(),
            sequence: 0,
        }
    }

    /// Update from L2 snapshot (50 levels)
    pub fn update_snapshot(&mut self, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>, seq: u64) {
        self.bids = bids.into_iter()
            .map(|(p, s)| PriceLevel { price: p, size: s })
            .collect();
        self.asks = asks.into_iter()
            .map(|(p, s)| PriceLevel { price: p, size: s })
            .collect();
        self.sequence = seq;
        self.last_update = Instant::now();
    }

    /// Update from delta (incremental update)
    pub fn apply_delta(&mut self, side: BookSide, price: f64, size: f64) {
        let levels = match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        };

        if size == 0.0 {
            // Remove level
            levels.retain(|l| (l.price - price).abs() > 1e-10);
        } else {
            // Update or insert
            if let Some(level) = levels.iter_mut().find(|l| (l.price - price).abs() < 1e-10) {
                level.size = size;
            } else {
                levels.push(PriceLevel { price, size });
                // Re-sort
                match side {
                    BookSide::Bid => levels.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap()),
                    BookSide::Ask => levels.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap()),
                }
            }
        }
        self.last_update = Instant::now();
    }

    // === Price Accessors ===

    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first().map(|l| l.price)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first().map(|l| l.price)
    }

    pub fn mid_price(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            _ => None,
        }
    }

    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }

    pub fn spread_bps(&self) -> Option<f64> {
        match (self.spread(), self.mid_price()) {
            (Some(spread), Some(mid)) if mid > 0.0 => Some(spread / mid * 10000.0),
            _ => None,
        }
    }

    // === Depth Analysis ===

    /// Total bid size in top N levels
    pub fn bid_depth(&self, levels: usize) -> f64 {
        self.bids.iter().take(levels).map(|l| l.size).sum()
    }

    /// Total ask size in top N levels
    pub fn ask_depth(&self, levels: usize) -> f64 {
        self.asks.iter().take(levels).map(|l| l.size).sum()
    }

    /// Depth imbalance: (bid_depth - ask_depth) / (bid_depth + ask_depth)
    /// Positive = more buy pressure, Negative = more sell pressure
    pub fn depth_imbalance(&self, levels: usize) -> f64 {
        let bid_d = self.bid_depth(levels);
        let ask_d = self.ask_depth(levels);
        let total = bid_d + ask_d;
        if total > 0.0 {
            (bid_d - ask_d) / total
        } else {
            0.0
        }
    }

    /// Weighted mid price based on depth imbalance
    pub fn weighted_mid(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => {
                let bid_size = self.bids.first().map(|l| l.size).unwrap_or(0.0);
                let ask_size = self.asks.first().map(|l| l.size).unwrap_or(0.0);
                let total = bid_size + ask_size;
                if total > 0.0 {
                    // Weight towards the side with more size
                    Some((bid * ask_size + ask * bid_size) / total)
                } else {
                    Some((bid + ask) / 2.0)
                }
            }
            _ => None,
        }
    }

    // === Queue Position Estimation ===

    /// Get total volume at a specific price level
    pub fn volume_at_price(&self, price: f64, side: BookSide) -> f64 {
        let levels = match side {
            BookSide::Bid => &self.bids,
            BookSide::Ask => &self.asks,
        };
        levels.iter()
            .find(|l| (l.price - price).abs() < 1e-10)
            .map(|l| l.size)
            .unwrap_or(0.0)
    }

    /// Estimate volume ahead of us at a price level
    /// This is a simplistic model - assumes we're at the back of the queue
    pub fn volume_ahead_at_price(&self, price: f64, side: BookSide) -> f64 {
        self.volume_at_price(price, side)
    }

    /// Get all bid levels
    pub fn bids(&self) -> &[PriceLevel] {
        &self.bids
    }

    /// Get all ask levels
    pub fn asks(&self) -> &[PriceLevel] {
        &self.asks
    }

    /// Age of the book since last update
    pub fn age_ms(&self) -> u64 {
        self.last_update.elapsed().as_millis() as u64
    }

    /// Check if book is stale (no update for X ms)
    pub fn is_stale(&self, max_age_ms: u64) -> bool {
        self.age_ms() > max_age_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_book_basics() {
        let mut book = OrderBook::new("BTC-USDT".to_string());
        book.update_snapshot(
            vec![(100.0, 1.0), (99.0, 2.0), (98.0, 3.0)],
            vec![(101.0, 1.5), (102.0, 2.5), (103.0, 3.5)],
            1,
        );

        assert_eq!(book.best_bid(), Some(100.0));
        assert_eq!(book.best_ask(), Some(101.0));
        assert_eq!(book.mid_price(), Some(100.5));
        assert_eq!(book.spread(), Some(1.0));
        assert_eq!(book.bid_depth(3), 6.0);
        assert_eq!(book.ask_depth(3), 7.5);
    }

    #[test]
    fn test_depth_imbalance() {
        let mut book = OrderBook::new("BTC-USDT".to_string());
        book.update_snapshot(
            vec![(100.0, 10.0)],
            vec![(101.0, 5.0)],
            1,
        );
        
        let imbalance = book.depth_imbalance(1);
        assert!((imbalance - 0.333).abs() < 0.01);
    }
}
