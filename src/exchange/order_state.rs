//! Order State Manager
//!
//! Tracks open orders, fills, position, and realized P&L.
//! Provides reconciliation between WebSocket updates and REST polling.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;
use tokio::sync::RwLock;
use std::sync::Arc;
use tracing::debug;

/// Order side
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Side {
    Buy,
    Sell,
}

/// Order status from exchange
#[derive(Debug, Clone, PartialEq)]
pub enum OrderStatus {
    Open,
    PartialFill,
    Filled,
    Cancelled,
    Unknown,
}

/// Tracked order
#[derive(Debug, Clone)]
pub struct TrackedOrder {
    pub order_id: String,
    pub client_oid: String,
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub original_size: f64,
    pub filled_size: f64,
    pub remaining_size: f64,
    pub status: OrderStatus,
    pub created_at: Instant,
    pub last_update: Instant,
}

/// Fill event
#[derive(Debug, Clone)]
pub struct Fill {
    pub order_id: String,
    pub trade_id: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub fee: f64,
    pub fee_currency: String,
    pub timestamp: u64,
}

/// FIFO entry for position tracking
#[derive(Debug, Clone)]
pub struct FifoEntry {
    pub price: f64,
    pub size: f64,
    pub side: Side,
}

/// Detailed PnL breakdown
#[derive(Debug, Clone, Default)]
pub struct PnLBreakdown {
    pub spread_pnl: f64,      // P&L from bid-ask spread
    pub rebates: f64,         // Maker rebates earned
    pub taker_fees: f64,      // Taker fees paid
    pub total_realized: f64,  // Net realized P&L
}

/// Order Manager - tracks all order state
pub struct OrderManager {
    /// Active orders: order_id -> TrackedOrder
    active_orders: HashMap<String, TrackedOrder>,
    /// Client OID to Order ID mapping
    client_to_order: HashMap<String, String>,
    /// Orders pending cancellation
    pending_cancels: HashSet<String>,
    /// Current position in base asset
    position: f64,
    /// Realized P&L in quote asset (excluding rebates)
    realized_pnl: f64,
    /// Total rebates earned from maker fills
    total_rebates: f64,
    /// Average entry price (for unrealized P&L calc)
    avg_entry_price: f64,
    /// Session stats
    fills_count: u64,
    volume_base: f64,
    volume_quote: f64,
    /// Maker fee (negative = rebate)
    maker_fee: f64,
    /// Last fill timestamp
    last_fill_time: Option<Instant>,
    
    // === DETAILED PNL TRACKING ===
    /// FIFO queue for long entries
    long_entries: VecDeque<FifoEntry>,
    /// FIFO queue for short entries  
    short_entries: VecDeque<FifoEntry>,
    /// Spread P&L (from closing positions)
    spread_pnl: f64,
    /// Taker fees paid (from fill.fee field)
    taker_fees: f64,
}

impl OrderManager {
    pub fn new(maker_fee: f64) -> Self {
        Self {
            active_orders: HashMap::new(),
            client_to_order: HashMap::new(),
            pending_cancels: HashSet::new(),
            position: 0.0,
            realized_pnl: 0.0,
            total_rebates: 0.0,
            avg_entry_price: 0.0,
            fills_count: 0,
            volume_base: 0.0,
            volume_quote: 0.0,
            maker_fee,
            last_fill_time: None,
            long_entries: VecDeque::new(),
            short_entries: VecDeque::new(),
            spread_pnl: 0.0,
            taker_fees: 0.0,
        }
    }

    /// Register a new order
    pub fn register_order(&mut self, order_id: String, client_oid: String, symbol: String,
                          side: Side, price: f64, size: f64) {
        let order = TrackedOrder {
            order_id: order_id.clone(),
            client_oid: client_oid.clone(),
            symbol,
            side,
            price,
            original_size: size,
            filled_size: 0.0,
            remaining_size: size,
            status: OrderStatus::Open,
            created_at: Instant::now(),
            last_update: Instant::now(),
        };
        
        self.active_orders.insert(order_id.clone(), order);
        self.client_to_order.insert(client_oid, order_id);
    }

    /// Process a fill from WebSocket or REST
    pub fn on_fill(&mut self, fill: &Fill) {
        // Update stats
        self.fills_count += 1;
        self.volume_base += fill.size;
        self.volume_quote += fill.price * fill.size;
        self.last_fill_time = Some(Instant::now());
        
        // Track fees from fill (taker fees are positive, maker rebates are negative)
        if fill.fee > 0.0 {
            self.taker_fees += fill.fee;
        } else {
            self.total_rebates += -fill.fee;
        }

        // Update position with FIFO tracking
        match fill.side {
            Side::Buy => {
                let old_pos = self.position;
                self.position += fill.size;
                
                if old_pos >= 0.0 {
                    // Adding to long position - push to FIFO queue
                    self.long_entries.push_back(FifoEntry {
                        price: fill.price,
                        size: fill.size,
                        side: Side::Buy,
                    });
                    // Update average entry price
                    let old_cost = old_pos * self.avg_entry_price;
                    let new_cost = fill.size * fill.price;
                    if self.position > 0.0 {
                        self.avg_entry_price = (old_cost + new_cost) / self.position;
                    }
                } else {
                    // Covering short with FIFO
                    let mut remaining = fill.size;
                    while remaining > 0.0 && !self.short_entries.is_empty() {
                        let entry = self.short_entries.front_mut().unwrap();
                        let close_size = remaining.min(entry.size);
                        
                        // Spread P&L = (entry_price - exit_price) * size for shorts
                        let pnl = close_size * (entry.price - fill.price);
                        self.spread_pnl += pnl;
                        self.realized_pnl += pnl;
                        
                        entry.size -= close_size;
                        remaining -= close_size;
                        
                        if entry.size < 0.0001 {
                            self.short_entries.pop_front();
                        }
                    }
                    // Any remaining goes to new long
                    if remaining > 0.0001 {
                        self.long_entries.push_back(FifoEntry {
                            price: fill.price,
                            size: remaining,
                            side: Side::Buy,
                        });
                    }
                }
            }
            Side::Sell => {
                let old_pos = self.position;
                self.position -= fill.size;
                
                if old_pos <= 0.0 {
                    // Adding to short position - push to FIFO queue
                    self.short_entries.push_back(FifoEntry {
                        price: fill.price,
                        size: fill.size,
                        side: Side::Sell,
                    });
                    // Update average entry price
                    let old_cost = (-old_pos) * self.avg_entry_price;
                    let new_cost = fill.size * fill.price;
                    if self.position < 0.0 {
                        self.avg_entry_price = (old_cost + new_cost) / (-self.position);
                    }
                } else {
                    // Closing long with FIFO
                    let mut remaining = fill.size;
                    while remaining > 0.0 && !self.long_entries.is_empty() {
                        let entry = self.long_entries.front_mut().unwrap();
                        let close_size = remaining.min(entry.size);
                        
                        // Spread P&L = (exit_price - entry_price) * size for longs
                        let pnl = close_size * (fill.price - entry.price);
                        self.spread_pnl += pnl;
                        self.realized_pnl += pnl;
                        
                        entry.size -= close_size;
                        remaining -= close_size;
                        
                        if entry.size < 0.0001 {
                            self.long_entries.pop_front();
                        }
                    }
                    // Any remaining goes to new short
                    if remaining > 0.0001 {
                        self.short_entries.push_back(FifoEntry {
                            price: fill.price,
                            size: remaining,
                            side: Side::Sell,
                        });
                    }
                }
            }
        }

        // Update order state
        if let Some(order) = self.active_orders.get_mut(&fill.order_id) {
            order.filled_size += fill.size;
            order.remaining_size = order.original_size - order.filled_size;
            order.last_update = Instant::now();
            
            if order.remaining_size <= 0.0001 {
                order.status = OrderStatus::Filled;
            } else {
                order.status = OrderStatus::PartialFill;
            }
        }

        debug!("[FILL] {} {} @ ${:.4} | Pos: {:.4} | PnL: ${:.2}",
            match fill.side { Side::Buy => "BUY", Side::Sell => "SELL" },
            fill.size, fill.price, self.position, self.realized_pnl);
    }

    /// Mark order as cancelled
    pub fn on_cancel(&mut self, order_id: &str) {
        if let Some(order) = self.active_orders.get_mut(order_id) {
            order.status = OrderStatus::Cancelled;
        }
        self.pending_cancels.remove(order_id);
    }

    /// Remove completed/cancelled orders
    pub fn cleanup_orders(&mut self) {
        self.active_orders.retain(|_, order| {
            matches!(order.status, OrderStatus::Open | OrderStatus::PartialFill)
        });
    }

    /// Get order ID from client OID
    pub fn get_order_id(&self, client_oid: &str) -> Option<&String> {
        self.client_to_order.get(client_oid)
    }

    /// Mark order as pending cancel
    pub fn mark_pending_cancel(&mut self, order_id: &str) {
        self.pending_cancels.insert(order_id.to_string());
    }

    /// Get current position
    pub fn position(&self) -> f64 {
        self.position
    }

    /// Get realized P&L
    pub fn realized_pnl(&self) -> f64 {
        self.realized_pnl
    }

    /// Get total rebates earned
    pub fn rebates(&self) -> f64 {
        self.total_rebates
    }

    /// Get spread P&L (FIFO based)
    pub fn spread_pnl(&self) -> f64 {
        self.spread_pnl
    }

    /// Get taker fees paid
    pub fn taker_fees(&self) -> f64 {
        self.taker_fees
    }

    /// Get unrealized P&L given current market price
    pub fn unrealized_pnl(&self, current_price: f64) -> f64 {
        if self.position > 0.0 {
            self.position * (current_price - self.avg_entry_price)
        } else if self.position < 0.0 {
            (-self.position) * (self.avg_entry_price - current_price)
        } else {
            0.0
        }
    }

    /// Get total P&L given current market price
    pub fn total_pnl(&self, current_price: f64) -> f64 {
        self.realized_pnl + self.unrealized_pnl(current_price)
    }

    /// Get session stats
    pub fn stats(&self) -> (u64, f64, f64) {
        (self.fills_count, self.volume_base, self.volume_quote)
    }

    /// Get active order for a side (if any)
    pub fn active_order_for_side(&self, side: Side) -> Option<&TrackedOrder> {
        self.active_orders.values()
            .find(|o| o.side == side && matches!(o.status, OrderStatus::Open | OrderStatus::PartialFill))
    }

    /// Check if we have a pending cancel for order
    pub fn is_pending_cancel(&self, order_id: &str) -> bool {
        self.pending_cancels.contains(order_id)
    }

    /// Seconds since last fill
    pub fn secs_since_last_fill(&self) -> u64 {
        match self.last_fill_time {
            Some(t) => t.elapsed().as_secs(),
            None => u64::MAX,
        }
    }
}

/// Thread-safe wrapper
pub type SharedOrderManager = Arc<RwLock<OrderManager>>;

pub fn new_shared_order_manager(maker_fee: f64) -> SharedOrderManager {
    Arc::new(RwLock::new(OrderManager::new(maker_fee)))
}
