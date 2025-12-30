//! Order State Machine - Fine-grained order state tracking
//!
//! Tracks orders through their full lifecycle with state transitions
//! and pending order deduplication.

use std::collections::HashMap;
use std::time::Instant;
use tracing::{info, debug};

// ============================================================================
// Order States
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderState {
    /// Order request sent, awaiting exchange ack
    PendingNew,
    /// Order accepted by exchange, on book
    Open,
    /// Order partially filled
    PartiallyFilled,
    /// Modify request sent, awaiting ack
    PendingModify,
    /// Cancel request sent, awaiting ack
    PendingCancel,
    /// Order completely filled
    Filled,
    /// Order cancelled
    Cancelled,
    /// Order rejected by exchange
    Rejected,
    /// Order expired (e.g., IOC not filled)
    Expired,
}

impl OrderState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, 
            OrderState::Filled | 
            OrderState::Cancelled | 
            OrderState::Rejected | 
            OrderState::Expired
        )
    }

    pub fn is_pending(&self) -> bool {
        matches!(self,
            OrderState::PendingNew |
            OrderState::PendingModify |
            OrderState::PendingCancel
        )
    }

    pub fn is_active(&self) -> bool {
        matches!(self,
            OrderState::Open |
            OrderState::PartiallyFilled
        )
    }
}

// ============================================================================
// Order Info
// ============================================================================

#[derive(Debug, Clone)]
pub struct OrderInfo {
    pub order_id: Option<String>,
    pub client_oid: String,
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub original_size: f64,
    pub filled_size: f64,
    pub state: OrderState,
    pub created_at: Instant,
    pub last_update: Instant,
    pub state_history: Vec<(OrderState, Instant)>,
}

impl OrderInfo {
    pub fn new(client_oid: String, symbol: String, side: String, price: f64, size: f64) -> Self {
        let now = Instant::now();
        Self {
            order_id: None,
            client_oid,
            symbol,
            side,
            price,
            original_size: size,
            filled_size: 0.0,
            state: OrderState::PendingNew,
            created_at: now,
            last_update: now,
            state_history: vec![(OrderState::PendingNew, now)],
        }
    }

    pub fn remaining_size(&self) -> f64 {
        self.original_size - self.filled_size
    }

    pub fn fill_pct(&self) -> f64 {
        if self.original_size > 0.0 {
            (self.filled_size / self.original_size) * 100.0
        } else {
            0.0
        }
    }

    pub fn age_ms(&self) -> u128 {
        self.created_at.elapsed().as_millis()
    }
}

// ============================================================================
// State Transition
// ============================================================================

#[derive(Debug, Clone, Copy)]
pub enum StateTransition {
    Acknowledge,    // PendingNew -> Open
    PartialFill,    // Open -> PartiallyFilled
    Fill,           // Open/PartiallyFilled -> Filled
    ModifyRequest,  // Open/PartiallyFilled -> PendingModify
    ModifyAck,      // PendingModify -> Open
    CancelRequest,  // Open/PartiallyFilled -> PendingCancel
    CancelAck,      // PendingCancel -> Cancelled
    Reject,         // PendingNew -> Rejected
    Expire,         // Open -> Expired
}

// ============================================================================
// Order State Machine
// ============================================================================

pub struct OrderStateMachine {
    orders: HashMap<String, OrderInfo>,  // client_oid -> OrderInfo
    order_id_map: HashMap<String, String>,  // order_id -> client_oid
    pending_dedup: HashMap<String, Instant>,  // key -> last_request_time
}

impl OrderStateMachine {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            order_id_map: HashMap::new(),
            pending_dedup: HashMap::new(),
        }
    }

    /// Register a new order
    pub fn register_order(&mut self, client_oid: String, symbol: String, side: String, price: f64, size: f64) {
        let order = OrderInfo::new(client_oid.clone(), symbol, side, price, size);
        self.orders.insert(client_oid, order);
    }

    /// Check if request is duplicate (prevent double-sends)
    pub fn is_duplicate(&mut self, key: &str, dedup_window_ms: u128) -> bool {
        if let Some(last) = self.pending_dedup.get(key) {
            if last.elapsed().as_millis() < dedup_window_ms {
                return true;
            }
        }
        self.pending_dedup.insert(key.to_string(), Instant::now());
        false
    }

    /// Transition order state
    pub fn transition(&mut self, client_oid: &str, transition: StateTransition) -> Result<OrderState, &'static str> {
        let order = self.orders.get_mut(client_oid).ok_or("Order not found")?;
        
        let new_state = match (order.state, transition) {
            // Normal lifecycle
            (OrderState::PendingNew, StateTransition::Acknowledge) => OrderState::Open,
            (OrderState::PendingNew, StateTransition::Reject) => OrderState::Rejected,
            (OrderState::Open, StateTransition::PartialFill) => OrderState::PartiallyFilled,
            (OrderState::Open, StateTransition::Fill) => OrderState::Filled,
            (OrderState::PartiallyFilled, StateTransition::Fill) => OrderState::Filled,
            (OrderState::Open, StateTransition::Expire) => OrderState::Expired,
            
            // Modifications
            (OrderState::Open, StateTransition::ModifyRequest) => OrderState::PendingModify,
            (OrderState::PartiallyFilled, StateTransition::ModifyRequest) => OrderState::PendingModify,
            (OrderState::PendingModify, StateTransition::ModifyAck) => OrderState::Open,
            
            // Cancellations
            (OrderState::Open, StateTransition::CancelRequest) => OrderState::PendingCancel,
            (OrderState::PartiallyFilled, StateTransition::CancelRequest) => OrderState::PendingCancel,
            (OrderState::PendingCancel, StateTransition::CancelAck) => OrderState::Cancelled,
            
            // Invalid transitions
            _ => return Err("Invalid state transition"),
        };
        
        order.state = new_state;
        order.last_update = Instant::now();
        order.state_history.push((new_state, Instant::now()));
        
        debug!("[STATE] {} -> {:?}", client_oid, new_state);
        Ok(new_state)
    }

    /// Set exchange order_id after acknowledgment
    pub fn set_order_id(&mut self, client_oid: &str, order_id: String) {
        if let Some(order) = self.orders.get_mut(client_oid) {
            self.order_id_map.insert(order_id.clone(), client_oid.to_string());
            order.order_id = Some(order_id);
        }
    }

    /// Record a fill
    pub fn record_fill(&mut self, client_oid: &str, fill_size: f64) {
        if let Some(order) = self.orders.get_mut(client_oid) {
            order.filled_size += fill_size;
            order.last_update = Instant::now();
            
            if order.filled_size >= order.original_size {
                let _ = self.transition(client_oid, StateTransition::Fill);
            } else if order.state == OrderState::Open {
                let _ = self.transition(client_oid, StateTransition::PartialFill);
            }
        }
    }

    /// Get order by client_oid
    pub fn get_order(&self, client_oid: &str) -> Option<&OrderInfo> {
        self.orders.get(client_oid)
    }

    /// Get order by exchange order_id
    pub fn get_by_order_id(&self, order_id: &str) -> Option<&OrderInfo> {
        self.order_id_map.get(order_id)
            .and_then(|coid| self.orders.get(coid))
    }

    /// Get all active orders
    pub fn active_orders(&self) -> Vec<&OrderInfo> {
        self.orders.values()
            .filter(|o| o.state.is_active())
            .collect()
    }

    /// Get all pending orders
    pub fn pending_orders(&self) -> Vec<&OrderInfo> {
        self.orders.values()
            .filter(|o| o.state.is_pending())
            .collect()
    }

    /// Remove terminal orders older than max_age
    pub fn cleanup(&mut self, max_age_ms: u128) {
        let _now = Instant::now();
        self.orders.retain(|_, o| {
            !o.state.is_terminal() || o.last_update.elapsed().as_millis() < max_age_ms
        });
        self.pending_dedup.retain(|_, t| t.elapsed().as_millis() < max_age_ms);
    }

    /// Statistics
    pub fn stats(&self) -> OrderStats {
        let mut stats = OrderStats::default();
        for order in self.orders.values() {
            match order.state {
                OrderState::PendingNew => stats.pending_new += 1,
                OrderState::Open => stats.open += 1,
                OrderState::PartiallyFilled => stats.partially_filled += 1,
                OrderState::PendingModify => stats.pending_modify += 1,
                OrderState::PendingCancel => stats.pending_cancel += 1,
                OrderState::Filled => stats.filled += 1,
                OrderState::Cancelled => stats.cancelled += 1,
                OrderState::Rejected => stats.rejected += 1,
                OrderState::Expired => stats.expired += 1,
            }
        }
        stats.total = self.orders.len();
        stats
    }
}

#[derive(Debug, Default, Clone)]
pub struct OrderStats {
    pub total: usize,
    pub pending_new: usize,
    pub open: usize,
    pub partially_filled: usize,
    pub pending_modify: usize,
    pub pending_cancel: usize,
    pub filled: usize,
    pub cancelled: usize,
    pub rejected: usize,
    pub expired: usize,
}

impl OrderStats {
    pub fn log(&self) {
        info!("[ORDER STATS] total: {} | open: {} | partial: {} | pending: {} | filled: {} | cancelled: {} | rejected: {}",
            self.total, self.open, self.partially_filled,
            self.pending_new + self.pending_modify + self.pending_cancel,
            self.filled, self.cancelled, self.rejected);
    }
}

impl Default for OrderStateMachine {
    fn default() -> Self {
        Self::new()
    }
}
