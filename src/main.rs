//! TEST_Multi_layers v10.3: Institutional-Grade Order Management
use anyhow::Result;
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tracing::{info, warn};

mod exchange;
use exchange::auth::KucoinAuth;
use exchange::ws_order_client_v2::{WsOrderClientV2, WsOrderRequest, WsCancelRequest};

// ═══════════════════════════════════════════════════════════════════
// CONFIGURATION - 25 LAYERS PER SIDE
// ═══════════════════════════════════════════════════════════════════
const LEVELS: [(f64, f64); 25] = [
    // Close layers: tighter refresh (+20% vs original)
    (0.55, 0.34), (1.23, 0.74), (1.91, 1.15), (2.59, 1.56), (3.27, 1.97),
    (3.95, 2.38), (4.63, 2.78), (5.31, 3.19), (5.99, 3.60), (6.67, 4.01),
    // Mid layers: moderate refresh (+20%)
    (7.35, 4.8), (8.03, 5.4), (8.71, 6.0), (9.39, 6.6), (10.07, 7.2),
    // Far layers: wider refresh (match spread - no change needed)
    (10.75, 12.90), (11.43, 13.72), (12.11, 14.53), (12.79, 15.35), (13.47, 16.16),
    (14.15, 16.98), (14.83, 17.80), (15.51, 18.61), (16.19, 19.43), (16.87, 20.24)
];
const ORDER_USD: f64 = 10.0;
const MAX_INV_SOL: f64 = 15.0;
const REBATE: f64 = 1.0;
const SYM: &str = "SOL-USDT";
const MAX_ORDERS_PER_SIDE: usize = 25; // 25 bids + 25 asks

// ═══════════════════════════════════════════════════════════════════
// QUANT PARAMETERS
// ═══════════════════════════════════════════════════════════════════
const GAMMA: f64 = 0.1;
const OFI_PAUSE_THRESHOLD: f64 = 0.60;
const OFI_RESUME_THRESHOLD: f64 = 0.35;
const VOL_EWMA_LAMBDA: f64 = 0.94;
const SIGMA_FLOOR: f64 = 0.02;
const MOMENTUM_THRESHOLD: f64 = 0.003;
const MOMENTUM_WINDOW_SECS: u64 = 300;
const ETA: f64 = -0.005;

// V10.3: Cancel timeout - try REST fallback before forcing empty
const CANCEL_TIMEOUT_SECS: u64 = 5;

// V10.3: Orphan cancel rate limiting (prevent cancel storm)
const MAX_ORPHAN_CANCELS_PER_TICK: usize = 5;

// V10.3: Safety buffer for balance checks
const BALANCE_SAFETY_BUFFER_PCT: f64 = 0.02; // 2% buffer

// ═══════════════════════════════════════════════════════════════════
// V10.3: ORDER STATE MACHINE (Enhanced)
// ═══════════════════════════════════════════════════════════════════
#[derive(Clone, Debug)]
enum LevelOrderState {
    Empty,
    Live { order_id: String, price: f64 },
    CancelPending { order_id: String, price: f64, sent_at: Instant, attempts: u8 },
    // V10.3: Order stuck - WS cancel failed, needs REST fallback
    CancelStuck { order_id: String, price: f64 },
}

impl LevelOrderState {
    fn is_empty(&self) -> bool { matches!(self, LevelOrderState::Empty) }
    fn is_live(&self) -> bool { matches!(self, LevelOrderState::Live { .. }) }
    fn is_cancel_pending(&self) -> bool { matches!(self, LevelOrderState::CancelPending { .. }) }
    fn is_cancel_stuck(&self) -> bool { matches!(self, LevelOrderState::CancelStuck { .. }) }
    fn order_id(&self) -> Option<&str> {
        match self {
            LevelOrderState::Live { order_id, .. } => Some(order_id),
            LevelOrderState::CancelPending { order_id, .. } => Some(order_id),
            LevelOrderState::CancelStuck { order_id, .. } => Some(order_id),
            LevelOrderState::Empty => None,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// STRUCTS
// ═══════════════════════════════════════════════════════════════════
#[derive(Clone)]
struct ActiveOrder {
    order_id: String,
    side: String,
    price: f64,
    size: f64,
}

#[derive(Default, Clone)]
struct Balances { sol: f64, usdt: f64 }

// V10.3: Two-layer commitment tracking
#[derive(Default, Clone)]
struct CommitmentTracker {
    // Inflight: just sent, not yet confirmed by recon
    inflight_usdt: f64,
    inflight_sol: f64,
    // Live: confirmed active on exchange via recon  
    live_usdt: f64,
    live_sol: f64,
}

impl CommitmentTracker {
    fn total_usdt(&self) -> f64 { self.inflight_usdt + self.live_usdt }
    fn total_sol(&self) -> f64 { self.inflight_sol + self.live_sol }
    
    fn add_inflight_bid(&mut self, notional: f64) { self.inflight_usdt += notional; }
    fn add_inflight_ask(&mut self, size: f64) { self.inflight_sol += size; }
    
    // Move from inflight to live when recon confirms
    fn confirm_bid(&mut self, notional: f64) {
        self.inflight_usdt = (self.inflight_usdt - notional).max(0.0);
        self.live_usdt += notional;
    }
    fn confirm_ask(&mut self, size: f64) {
        self.inflight_sol = (self.inflight_sol - size).max(0.0);
        self.live_sol += size;
    }
    
    // Remove from live when filled/cancelled
    fn release_bid(&mut self, notional: f64) { self.live_usdt = (self.live_usdt - notional).max(0.0); }
    fn release_ask(&mut self, size: f64) { self.live_sol = (self.live_sol - size).max(0.0); }
    
    // Reset inflight on recon (anything not confirmed is orphan)
    fn reset_inflight(&mut self) { self.inflight_usdt = 0.0; self.inflight_sol = 0.0; }
}

// V10.3: Symmetric inventory gating functions
fn can_place_bid(inv: f64, size: f64) -> bool { inv + size <= MAX_INV_SOL }
fn can_place_ask(inv: f64, size: f64) -> bool { inv - size >= -MAX_INV_SOL }
fn needs_cancel_bid(inv: f64, size: f64, skip_bids: bool) -> bool { skip_bids || inv + size > MAX_INV_SOL }
fn needs_cancel_ask(inv: f64, size: f64) -> bool { inv - size < -MAX_INV_SOL }

struct Entry { px: f64, sz: f64 }
#[derive(Default)]
struct PnL {
    lq: VecDeque<Entry>, sq: VecDeque<Entry>,
    buys: u64, sells: u64, spread: f64, reb: f64,
    matched: u64, wins: u64, losses: u64,
}
impl PnL {
    fn buy(&mut self, px: f64, sz: f64, r: f64) {
        self.buys += 1; self.reb += r;
        let mut rem = sz;
        while rem > 0.0 && !self.sq.is_empty() {
            let e = self.sq.front_mut().unwrap();
            let m = rem.min(e.sz);
            let pnl = m * (e.px - px);
            self.spread += pnl; self.matched += 1;
            if pnl > 0.0 { self.wins += 1; } else { self.losses += 1; }
            e.sz -= m; rem -= m;
            if e.sz < 0.0001 { self.sq.pop_front(); }
        }
        if rem > 0.0001 { self.lq.push_back(Entry { px, sz: rem }); }
    }
    fn sell(&mut self, px: f64, sz: f64, r: f64) {
        self.sells += 1; self.reb += r;
        let mut rem = sz;
        while rem > 0.0 && !self.lq.is_empty() {
            let e = self.lq.front_mut().unwrap();
            let m = rem.min(e.sz);
            let pnl = m * (px - e.px);
            self.spread += pnl; self.matched += 1;
            if pnl > 0.0 { self.wins += 1; } else { self.losses += 1; }
            e.sz -= m; rem -= m;
            if e.sz < 0.0001 { self.lq.pop_front(); }
        }
        if rem > 0.0001 { self.sq.push_back(Entry { px, sz: rem }); }
    }
    fn inv(&self) -> f64 { 
        self.lq.iter().map(|e| e.sz).sum::<f64>() - self.sq.iter().map(|e| e.sz).sum::<f64>() 
    }
    fn net(&self) -> f64 { self.spread + self.reb }
}

#[derive(Default)]
struct MarketData {
    mid: f64, ofi: f64, last_mid: f64, ewma_var: f64,
    price_history: VecDeque<(Instant, f64)>,
    // V10: Track actual update interval for correct sigma annualization
    last_update: Option<Instant>,
    update_interval_ms: f64,
}
impl MarketData {
    fn update(&mut self) {
        let now = Instant::now();
        
        // V10: Track actual update interval
        if let Some(last) = self.last_update {
            let elapsed_ms = now.duration_since(last).as_secs_f64() * 1000.0;
            // EWMA of update interval for stable estimate
            self.update_interval_ms = 0.9 * self.update_interval_ms + 0.1 * elapsed_ms;
        }
        self.last_update = Some(now);
        
        if self.last_mid > 0.0 && self.mid > 0.0 {
            let ret = (self.mid / self.last_mid).ln();
            self.ewma_var = VOL_EWMA_LAMBDA * self.ewma_var + (1.0 - VOL_EWMA_LAMBDA) * ret * ret;
        }
        self.last_mid = self.mid;
        self.price_history.push_back((now, self.mid));
        let cutoff = now - Duration::from_secs(MOMENTUM_WINDOW_SECS);
        while let Some((t, _)) = self.price_history.front() {
            if *t < cutoff { self.price_history.pop_front(); } else { break; }
        }
    }
    fn sigma(&self) -> f64 { 
        // V10: Correct annualization based on actual update interval
        // Default to 100ms if not yet calibrated
        let interval_ms = if self.update_interval_ms > 0.0 { self.update_interval_ms } else { 100.0 };
        let updates_per_day = 86400.0 * 1000.0 / interval_ms;
        (self.ewma_var * updates_per_day * 365.0).sqrt().max(SIGMA_FLOOR) 
    }
    fn momentum(&self) -> f64 {
        if let Some((_, p)) = self.price_history.front() {
            if *p > 0.0 && self.mid > 0.0 { return (self.mid - p) / p; }
        }
        0.0
    }
}

// ═══════════════════════════════════════════════════════════════════
// BINANCE FEED
// ═══════════════════════════════════════════════════════════════════
async fn binance_feed(data: Arc<RwLock<MarketData>>) {
    loop {
        let url = "wss://fstream.binance.com/stream?streams=solusdt@bookTicker/solusdt@depth5@100ms";
        if let Ok((ws, _)) = connect_async(url).await {
            info!("[BN] Connected");
            let (_, mut r) = ws.split();
            while let Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t))) = r.next().await {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&t) {
                    let stream = v["stream"].as_str().unwrap_or("");
                    let d = &v["data"];
                    if stream.contains("bookTicker") {
                        let b: f64 = d["b"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                        let a: f64 = d["a"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                        if b > 0.0 && a > 0.0 { let mut m = data.write().await; m.mid = (b + a) / 2.0; m.update(); }
                    } else if stream.contains("depth5") {
                        let (mut bv, mut av) = (0.0_f64, 0.0_f64);
                        if let Some(bids) = d["b"].as_array() {
                            for (i, b) in bids.iter().enumerate() {
                                if let Some(arr) = b.as_array() {
                                    if arr.len() >= 2 {
                                        let q: f64 = arr[1].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                                        bv += q * (-0.5 * i as f64).exp();
                                    }
                                }
                            }
                        }
                        if let Some(asks) = d["a"].as_array() {
                            for (i, a) in asks.iter().enumerate() {
                                if let Some(arr) = a.as_array() {
                                    if arr.len() >= 2 {
                                        let q: f64 = arr[1].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                                        av += q * (-0.5 * i as f64).exp();
                                    }
                                }
                            }
                        }
                        let t = bv + av;
                        if t > 0.0 { data.write().await.ofi = (bv - av) / t; }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// ═══════════════════════════════════════════════════════════════════
// REST API FUNCTIONS
// ═══════════════════════════════════════════════════════════════════
async fn poll_balances(auth: &KucoinAuth) -> Balances {
    let ep = "/api/v1/accounts?type=trade";
    let (ts, sig, pw, ver) = auth.sign("GET", ep, "");
    let mut bal = Balances::default();
    if let Ok(r) = reqwest::Client::new().get(format!("https://api.kucoin.com{}", ep))
        .header("KC-API-KEY", auth.api_key()).header("KC-API-SIGN", &sig)
        .header("KC-API-TIMESTAMP", &ts).header("KC-API-PASSPHRASE", &pw)
        .header("KC-API-KEY-VERSION", &ver).send().await {
        if let Ok(t) = r.text().await {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&t) {
                if let Some(items) = v["data"].as_array() {
                    for i in items {
                        let cur = i["currency"].as_str().unwrap_or("");
                        let avail: f64 = i["available"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                        match cur { "SOL" => bal.sol = avail, "USDT" => bal.usdt = avail, _ => {} }
                    }
                }
            }
        }
    }
    bal
}

async fn poll_active_orders(auth: &KucoinAuth) -> Vec<ActiveOrder> {
    let ep = "/api/v1/orders?symbol=SOL-USDT&status=active";
    let (ts, sig, pw, ver) = auth.sign("GET", ep, "");
    let mut orders = Vec::new();
    if let Ok(r) = reqwest::Client::new().get(format!("https://api.kucoin.com{}", ep))
        .header("KC-API-KEY", auth.api_key()).header("KC-API-SIGN", &sig)
        .header("KC-API-TIMESTAMP", &ts).header("KC-API-PASSPHRASE", &pw)
        .header("KC-API-KEY-VERSION", &ver).send().await {
        if let Ok(t) = r.text().await {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&t) {
                if let Some(items) = v["data"]["items"].as_array() {
                    for i in items {
                        let id = i["id"].as_str().unwrap_or("").to_string();
                        let side = i["side"].as_str().unwrap_or("").to_string();
                        let price: f64 = i["price"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                        let size: f64 = i["size"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                        if !id.is_empty() {
                            orders.push(ActiveOrder { order_id: id, side, price, size });
                        }
                    }
                }
            }
        }
    }
    orders
}

async fn poll_fills(auth: &KucoinAuth, seen: &mut HashSet<String>) -> Vec<(String, f64, f64)> {
    let ep = "/api/v1/fills?symbol=SOL-USDT&pageSize=20";
    let (ts, sig, pw, ver) = auth.sign("GET", ep, "");
    let mut out = Vec::new();
    if let Ok(r) = reqwest::Client::new().get(format!("https://api.kucoin.com{}", ep))
        .header("KC-API-KEY", auth.api_key()).header("KC-API-SIGN", &sig)
        .header("KC-API-TIMESTAMP", &ts).header("KC-API-PASSPHRASE", &pw)
        .header("KC-API-KEY-VERSION", &ver).send().await {
        if let Ok(t) = r.text().await {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&t) {
                if let Some(items) = v["data"]["items"].as_array() {
                    for i in items {
                        let tid = i["tradeId"].as_str().unwrap_or("").to_string();
                        if seen.contains(&tid) { continue; }
                        seen.insert(tid);
                        let side = i["side"].as_str().unwrap_or("").to_string();
                        let sz: f64 = i["size"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                        let px: f64 = i["price"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                        if sz > 0.0 { out.push((side, sz, px)); }
                    }
                }
            }
        }
    }
    out
}

// V10: REST cancel all orders
async fn cancel_all_orders(auth: &KucoinAuth) {
    let ep = "/api/v1/orders";
    let body = r#"{"symbol":"SOL-USDT"}"#;
    let (ts, sig, pw, ver) = auth.sign("DELETE", ep, body);
    let _ = reqwest::Client::new().delete(format!("https://api.kucoin.com{}", ep))
        .header("KC-API-KEY", auth.api_key()).header("KC-API-SIGN", &sig)
        .header("KC-API-TIMESTAMP", &ts).header("KC-API-PASSPHRASE", &pw)
        .header("KC-API-KEY-VERSION", &ver).header("Content-Type", "application/json")
        .body(body).send().await;
}

// V10.3: REST cancel single order (fallback for stuck WS cancels)
async fn rest_cancel_order(auth: &KucoinAuth, order_id: &str) -> bool {
    let ep = format!("/api/v1/orders/{}", order_id);
    let (ts, sig, pw, ver) = auth.sign("DELETE", &ep, "");
    if let Ok(r) = reqwest::Client::new().delete(format!("https://api.kucoin.com{}", ep))
        .header("KC-API-KEY", auth.api_key()).header("KC-API-SIGN", &sig)
        .header("KC-API-TIMESTAMP", &ts).header("KC-API-PASSPHRASE", &pw)
        .header("KC-API-KEY-VERSION", &ver).send().await {
        return r.status().is_success();
    }
    false
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).with_target(false).init();
    info!("═══ V10.3: Institutional-Grade Order Management ═══");
    
    let auth = KucoinAuth::new(
        std::env::var("KUCOIN_API_KEY")?, std::env::var("KUCOIN_API_SECRET")?,
        std::env::var("KUCOIN_PASSPHRASE")?, true
    );
    let auth2 = auth.clone();
    let auth3 = auth.clone();
    let auth4 = auth.clone();
    let auth_shutdown = auth.clone();
    
    // V10: Remove unnecessary RwLock - WsOrderClientV2 uses internal Arc
    let ws = Arc::new(WsOrderClientV2::new(
        auth, "https://api.kucoin.com".into(), "wss://wsapi.kucoin.com/v1/private".into()
    ));
    { 
        // Note: connect() takes &mut self, we need a workaround
        // Actually looking at ws_order_client_v2.rs, connect() -> start() which takes &self
        // The signature is: pub async fn connect(&mut self) -> Result<JoinHandle<()>>
        // We need to call start() directly which takes &self
        let _ = ws.start().await?; 
    }
    info!("[WS] OK");
    
    let data = Arc::new(RwLock::new(MarketData::default()));
    let balances = Arc::new(RwLock::new(Balances::default()));
    let active_orders = Arc::new(RwLock::new(Vec::<ActiveOrder>::new()));
    
    // Initial fetches
    let bal = poll_balances(&auth2).await;
    info!("[BAL] {:.4} SOL, {:.2} USDT", bal.sol, bal.usdt);
    *balances.write().await = bal;
    
    // Cancel all orders on startup
    cancel_all_orders(&auth3).await;
    info!("[STARTUP] Cancelled all existing orders");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let orders = poll_active_orders(&auth3).await;
    info!("[ORDERS] {} active", orders.len());
    *active_orders.write().await = orders;
    
    let d2 = data.clone();
    tokio::spawn(async move { binance_feed(d2).await; });
    
    loop { if data.read().await.mid > 0.0 { break; } tokio::time::sleep(Duration::from_millis(100)).await; }
    info!("[START] mid={:.2}", data.read().await.mid);
    
    // V10: Order state machine per level - key: level_bps*10, value: (bid_state, ask_state)
    let mut level_orders: HashMap<i32, (LevelOrderState, LevelOrderState)> = HashMap::new();
    for (bps, _) in LEVELS.iter() {
        level_orders.insert((*bps * 10.0) as i32, (LevelOrderState::Empty, LevelOrderState::Empty));
    }
    
    let mut pnl = PnL::default();
    let mut seen: HashSet<String> = HashSet::new();
    let start = Instant::now();
    
    // V10.3: Two-layer commitment tracker
    let mut commitments = CommitmentTracker::default();
    
    // V10.3: Orphan cancel tracking (rate limiting)
    let mut recently_cancelled: HashMap<String, Instant> = HashMap::new();
    
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    let mut log = tokio::time::interval(Duration::from_secs(30));
    let mut fp = tokio::time::interval(Duration::from_secs(5));
    let mut recon = tokio::time::interval(Duration::from_secs(1));
    let mut n: u64 = 0;
    
    let mut ofi_paused = false;
    let mut mom_paused = false;
    
    // V10: Graceful shutdown flag
    let mut shutting_down = false;
    
    loop {
        tokio::select! {
            // V10: Graceful shutdown on Ctrl+C
            _ = tokio::signal::ctrl_c(), if !shutting_down => {
                info!("[SHUTDOWN] Received SIGINT, initiating graceful shutdown...");
                shutting_down = true;
                
                // Stop placing new orders (flag is set)
                // Cancel all orders via REST
                cancel_all_orders(&auth_shutdown).await;
                info!("[SHUTDOWN] Cancelled all orders");
                
                // Final reconciliation
                tokio::time::sleep(Duration::from_millis(500)).await;
                let final_orders = poll_active_orders(&auth_shutdown).await;
                info!("[SHUTDOWN] Final order count: {}", final_orders.len());
                
                // Log final PnL
                let inv = pnl.inv();
                let m = data.read().await.mid;
                info!("═══════════════════════════════════════════════════════════════");
                info!("[SHUTDOWN] FINAL PnL REPORT");
                info!("Runtime: {}s | Buys:{} Sells:{} | Matches:{}", 
                    start.elapsed().as_secs(), pnl.buys, pnl.sells, pnl.matched);
                info!("Inventory: {:.4} SOL (${:.2})", inv, inv * m);
                info!("SPREAD: ${:.4} | REBATE: ${:.4} | NET: ${:.4}", pnl.spread, pnl.reb, pnl.net());
                info!("═══════════════════════════════════════════════════════════════");
                
                break;
            }
            _ = recon.tick(), if !shutting_down => {
                // ═══ V10.3: ORDER RECONCILIATION (Institutional Grade) ═══
                let orders = poll_active_orders(&auth4).await;
                let new_bal = poll_balances(&auth3).await;
                *balances.write().await = new_bal.clone();
                *active_orders.write().await = orders.clone();
                
                // V10.3: Reset inflight commitments (anything not confirmed is orphan)
                commitments.reset_inflight();
                
                // Build set of order IDs active on exchange
                let active_ids: HashSet<String> = orders.iter().map(|o| o.order_id.clone()).collect();
                
                // V10.3: Build set of tracked order IDs and recalculate live commitments
                let mut tracked_ids: HashSet<String> = HashSet::new();
                commitments.live_usdt = 0.0;
                commitments.live_sol = 0.0;
                
                // V10.3: Reconcile level_orders with exchange state
                for (_, (bid_state, ask_state)) in level_orders.iter_mut() {
                    // Handle bid state
                    match bid_state {
                        LevelOrderState::Live { order_id, price } => {
                            if !active_ids.contains(order_id) {
                                // Order filled or cancelled externally
                                *bid_state = LevelOrderState::Empty;
                            } else {
                                tracked_ids.insert(order_id.clone());
                                // Recalculate live commitment from actual order
                                if let Some(o) = orders.iter().find(|o| &o.order_id == order_id) {
                                    commitments.live_usdt += o.size * o.price;
                                }
                            }
                        }
                        LevelOrderState::CancelPending { order_id, price, sent_at, attempts } => {
                            if !active_ids.contains(order_id) {
                                // Cancel confirmed via recon
                                *bid_state = LevelOrderState::Empty;
                            } else if sent_at.elapsed().as_secs() > CANCEL_TIMEOUT_SECS {
                                // V10.3: Don't force empty - transition to CancelStuck for REST fallback
                                if *attempts < 3 {
                                    warn!("[RECON] Cancel timeout for bid {}, attempting REST fallback", order_id);
                                    if rest_cancel_order(&auth4, order_id).await {
                                        *bid_state = LevelOrderState::Empty;
                                    } else {
                                        *bid_state = LevelOrderState::CancelStuck { order_id: order_id.clone(), price: *price };
                                    }
                                } else {
                                    warn!("[RECON] Cancel stuck for bid {}, max attempts reached", order_id);
                                    *bid_state = LevelOrderState::CancelStuck { order_id: order_id.clone(), price: *price };
                                }
                            } else {
                                tracked_ids.insert(order_id.clone());
                            }
                        }
                        LevelOrderState::CancelStuck { order_id, .. } => {
                            if !active_ids.contains(order_id) {
                                *bid_state = LevelOrderState::Empty;
                            } else {
                                // Try REST cancel again
                                if rest_cancel_order(&auth4, order_id).await {
                                    *bid_state = LevelOrderState::Empty;
                                } else {
                                    tracked_ids.insert(order_id.clone());
                                }
                            }
                        }
                        LevelOrderState::Empty => {}
                    }
                    
                    // Handle ask state
                    match ask_state {
                        LevelOrderState::Live { order_id, price } => {
                            if !active_ids.contains(order_id) {
                                *ask_state = LevelOrderState::Empty;
                            } else {
                                tracked_ids.insert(order_id.clone());
                                if let Some(o) = orders.iter().find(|o| &o.order_id == order_id) {
                                    commitments.live_sol += o.size;
                                }
                            }
                        }
                        LevelOrderState::CancelPending { order_id, price, sent_at, attempts } => {
                            if !active_ids.contains(order_id) {
                                *ask_state = LevelOrderState::Empty;
                            } else if sent_at.elapsed().as_secs() > CANCEL_TIMEOUT_SECS {
                                if *attempts < 3 {
                                    warn!("[RECON] Cancel timeout for ask {}, attempting REST fallback", order_id);
                                    if rest_cancel_order(&auth4, order_id).await {
                                        *ask_state = LevelOrderState::Empty;
                                    } else {
                                        *ask_state = LevelOrderState::CancelStuck { order_id: order_id.clone(), price: *price };
                                    }
                                } else {
                                    warn!("[RECON] Cancel stuck for ask {}, max attempts reached", order_id);
                                    *ask_state = LevelOrderState::CancelStuck { order_id: order_id.clone(), price: *price };
                                }
                            } else {
                                tracked_ids.insert(order_id.clone());
                            }
                        }
                        LevelOrderState::CancelStuck { order_id, .. } => {
                            if !active_ids.contains(order_id) {
                                *ask_state = LevelOrderState::Empty;
                            } else {
                                if rest_cancel_order(&auth4, order_id).await {
                                    *ask_state = LevelOrderState::Empty;
                                } else {
                                    tracked_ids.insert(order_id.clone());
                                }
                            }
                        }
                        LevelOrderState::Empty => {}
                    }
                }
                
                // V10.3: Rate-limited orphan cancellation
                let mut orphan_budget = MAX_ORPHAN_CANCELS_PER_TICK;
                // Clean up stale entries from recently_cancelled
                recently_cancelled.retain(|_, t| t.elapsed().as_secs() < 10);
                
                for order in &orders {
                    if !tracked_ids.contains(&order.order_id) && orphan_budget > 0 {
                        if !recently_cancelled.contains_key(&order.order_id) {
                            info!("[ORPHAN] Cancelling untracked order: {} {} @ ${:.2}", 
                                order.side, order.order_id, order.price);
                            let _ = ws.cancel_order(WsCancelRequest {
                                symbol: SYM.into(), order_id: Some(order.order_id.clone()), client_oid: None
                            }).await;
                            recently_cancelled.insert(order.order_id.clone(), Instant::now());
                            orphan_budget -= 1;
                        }
                    }
                }
                
                // Log mismatch if any
                if orders.len() != tracked_ids.len() {
                    info!("[RECON] Active:{} Tracked:{} LiveUSDT:{:.2} LiveSOL:{:.3}", 
                        orders.len(), tracked_ids.len(), commitments.live_usdt, commitments.live_sol);
                }
            }
            _ = fp.tick(), if !shutting_down => {
                for (side, sz, px) in poll_fills(&auth2, &mut seen).await {
                    let r = sz * px * REBATE / 10000.0;
                    if side == "buy" { pnl.buy(px, sz, r); } else { pnl.sell(px, sz, r); }
                }
            }
            _ = tick.tick(), if !shutting_down => {
                n += 1;
                let md = data.read().await;
                let m = md.mid;
                let ofi = md.ofi;
                let sigma = md.sigma();
                let momentum = md.momentum();
                drop(md);
                
                let bal = balances.read().await.clone();
                
                if m <= 0.0 { continue; }
                
                // V10: Count orders from local state (race-free)
                let local_bid_count = level_orders.values()
                    .filter(|(b, _)| !b.is_empty()).count();
                let local_ask_count = level_orders.values()
                    .filter(|(_, a)| !a.is_empty()).count();
                
                // ═══ QUANT 1: OFI ═══
                let (mut skip_bids, mut skip_asks) = if ofi_paused {
                    if ofi.abs() < OFI_RESUME_THRESHOLD { ofi_paused = false; info!("[OFI] Resume"); (false, false) }
                    else { (ofi < 0.0, ofi > 0.0) }
                } else {
                    if ofi.abs() > OFI_PAUSE_THRESHOLD { ofi_paused = true; info!("[OFI] Pause: {:.3}", ofi); }
                    (ofi < -OFI_PAUSE_THRESHOLD, ofi > OFI_PAUSE_THRESHOLD)
                };
                
                // ═══ QUANT 2: Smart Trend Filter ═══
                let downtrend = momentum < -MOMENTUM_THRESHOLD;
                let uptrend = momentum > MOMENTUM_THRESHOLD;
                let inv = pnl.inv();
                
                // Downtrend: pause if not holding long (protect from falling knife)
                if downtrend {
                    if !mom_paused { info!("[TREND] DOWN {:.2}% - selling only", momentum * 100.0); mom_paused = true; }
                    if inv <= 0.05 { continue; }
                } else if !uptrend && mom_paused { 
                    info!("[TREND] Normal"); 
                    mom_paused = false; 
                }
                
                // Uptrend: keep quoting but widen spreads to capture momentum
                let uptrend_multiplier = if uptrend {
                    if !mom_paused { info!("[TREND] UP {:.2}% - widening spreads 1.5x", momentum * 100.0); mom_paused = true; }
                    1.5  // Widen asks by 50% to capture more during rally
                } else { 1.0 };
                
                skip_bids = skip_bids || downtrend;
                
                // ═══ QUANT 3: Inventory Skew ═══
                let skew_bps = inv * GAMMA * sigma * sigma * 10000.0;
                
                // ═══ QUANT 4: Dynamic Sizing ═══
                let base_sz = ((ORDER_USD / m) / 0.01).round() * 0.01;
                let (bid_sz, ask_sz) = if inv > 0.0 {
                    ((base_sz * (ETA * inv).exp()).max(0.01), base_sz)
                } else { (base_sz, (base_sz * (ETA * inv.abs()).exp()).max(0.01)) };
                
                // Process each level
                for (bps, thresh) in LEVELS.iter() {
                    let key = (*bps * 10.0) as i32;
                    let (bid_state, ask_state) = level_orders.get(&key).cloned()
                        .unwrap_or((LevelOrderState::Empty, LevelOrderState::Empty));
                    
                    let max_skew = bps * 0.5;
                    let capped_skew = skew_bps.clamp(-max_skew, max_skew);
                    let bid_bps = bps + capped_skew;
                    // Apply uptrend multiplier to asks (widen during rallies)
                    let ask_bps = (bps - capped_skew) * uptrend_multiplier;
                    
                    let bp = ((m * (1.0 - bid_bps / 10000.0)) / 0.01).round() * 0.01;
                    let ap = ((m * (1.0 + ask_bps / 10000.0)) / 0.01).round() * 0.01;
                    
                    // ═══ REFRESH CHECK: Cancel stale orders beyond threshold ═══
                    // V10: Only transition to CancelPending, don't clear immediately
                    if let LevelOrderState::Live { ref order_id, price } = bid_state {
                        let bps_diff = ((price - bp).abs() / bp) * 10000.0;
                        if bps_diff > *thresh {
                            if let Ok(r) = ws.cancel_order(WsCancelRequest {
                                symbol: SYM.into(), order_id: Some(order_id.clone()), client_oid: None
                            }).await {
                                if r.success {
                                    level_orders.entry(key).or_insert((LevelOrderState::Empty, LevelOrderState::Empty)).0 = LevelOrderState::Empty;
                                } else {
                                    // Cancel sent but not confirmed - transition to CancelPending
                                    level_orders.entry(key).or_insert((LevelOrderState::Empty, LevelOrderState::Empty)).0 = 
                                        LevelOrderState::CancelPending { order_id: order_id.clone(), price, sent_at: Instant::now(), attempts: 1 };
                                }
                            } else {
                                // WS error - still transition to CancelPending
                                level_orders.entry(key).or_insert((LevelOrderState::Empty, LevelOrderState::Empty)).0 = 
                                    LevelOrderState::CancelPending { order_id: order_id.clone(), price, sent_at: Instant::now(), attempts: 1 };
                            }
                        }
                    }
                    
                    if let LevelOrderState::Live { ref order_id, price } = ask_state {
                        let bps_diff = ((price - ap).abs() / ap) * 10000.0;
                        if bps_diff > *thresh {
                            if let Ok(r) = ws.cancel_order(WsCancelRequest {
                                symbol: SYM.into(), order_id: Some(order_id.clone()), client_oid: None
                            }).await {
                                if r.success {
                                    level_orders.entry(key).or_insert((LevelOrderState::Empty, LevelOrderState::Empty)).1 = LevelOrderState::Empty;
                                } else {
                                    level_orders.entry(key).or_insert((LevelOrderState::Empty, LevelOrderState::Empty)).1 = 
                                        LevelOrderState::CancelPending { order_id: order_id.clone(), price, sent_at: Instant::now(), attempts: 1 };
                                }
                            } else {
                                level_orders.entry(key).or_insert((LevelOrderState::Empty, LevelOrderState::Empty)).1 = 
                                    LevelOrderState::CancelPending { order_id: order_id.clone(), price, sent_at: Instant::now(), attempts: 1 };
                            }
                        }
                    }
                    
                    // Re-read after potential cancellation
                    let (bid_state, ask_state) = level_orders.get(&key).cloned()
                        .unwrap_or((LevelOrderState::Empty, LevelOrderState::Empty));
                    
                    // ═══ BID ORDER ═══
                    // V10.3: Use CommitmentTracker with safety buffer
                    let safety_buffer = bal.usdt * BALANCE_SAFETY_BUFFER_PCT;
                    let available_usdt = bal.usdt - commitments.total_usdt() - safety_buffer;
                    if bid_state.is_empty() && !skip_bids && can_place_bid(inv, bid_sz)
                        && available_usdt >= bid_sz * bp && local_bid_count < MAX_ORDERS_PER_SIDE {
                        if let Ok(r) = ws.place_order(WsOrderRequest {
                            symbol: SYM.into(), side: "buy".into(),
                            price: format!("{:.2}", bp), size: format!("{:.2}", bid_sz),
                            client_oid: format!("b{}_{}", key, n),
                            order_type: "limit".into(), time_in_force: Some("GTC".into()),
                            post_only: Some(true)
                        }).await {
                            if r.success {
                                if let Some(ref oid) = r.order_id {
                                    level_orders.entry(key).or_insert((LevelOrderState::Empty, LevelOrderState::Empty)).0 = 
                                        LevelOrderState::Live { order_id: oid.clone(), price: bp };
                                    // V10.3: Track inflight commitment
                                    commitments.add_inflight_bid(bid_sz * bp);
                                }
                            }
                        }
                    } else if bid_state.is_live() && needs_cancel_bid(inv, bid_sz, skip_bids) {
                        // Cancel bid due to skip or inventory
                        if let LevelOrderState::Live { ref order_id, price } = bid_state {
                            if let Ok(r) = ws.cancel_order(WsCancelRequest {
                                symbol: SYM.into(), order_id: Some(order_id.clone()), client_oid: None
                            }).await {
                                if r.success {
                                    level_orders.entry(key).or_insert((LevelOrderState::Empty, LevelOrderState::Empty)).0 = LevelOrderState::Empty;
                                } else {
                                    level_orders.entry(key).or_insert((LevelOrderState::Empty, LevelOrderState::Empty)).0 = 
                                        LevelOrderState::CancelPending { order_id: order_id.clone(), price, sent_at: Instant::now(), attempts: 1 };
                                }
                            }
                        }
                    }
                    
                    // ═══ ASK ORDER ═══
                    let sol_safety_buffer = bal.sol * BALANCE_SAFETY_BUFFER_PCT;
                    let available_sol = bal.sol - commitments.total_sol() - sol_safety_buffer;
                    if ask_state.is_empty() && !skip_asks && can_place_ask(inv, ask_sz)
                        && available_sol >= ask_sz && local_ask_count < MAX_ORDERS_PER_SIDE {
                        if let Ok(r) = ws.place_order(WsOrderRequest {
                            symbol: SYM.into(), side: "sell".into(),
                            price: format!("{:.2}", ap), size: format!("{:.2}", ask_sz),
                            client_oid: format!("a{}_{}", key, n),
                            order_type: "limit".into(), time_in_force: Some("GTC".into()),
                            post_only: Some(true)
                        }).await {
                            if r.success {
                                if let Some(ref oid) = r.order_id {
                                    level_orders.entry(key).or_insert((LevelOrderState::Empty, LevelOrderState::Empty)).1 = 
                                        LevelOrderState::Live { order_id: oid.clone(), price: ap };
                                    // V10.3: Track inflight commitment
                                    commitments.add_inflight_ask(ask_sz);
                                }
                            }
                        }
                    } else if ask_state.is_live() && needs_cancel_ask(inv, ask_sz) {
                        if let LevelOrderState::Live { ref order_id, price } = ask_state {
                            if let Ok(r) = ws.cancel_order(WsCancelRequest {
                                symbol: SYM.into(), order_id: Some(order_id.clone()), client_oid: None
                            }).await {
                                if r.success {
                                    level_orders.entry(key).or_insert((LevelOrderState::Empty, LevelOrderState::Empty)).1 = LevelOrderState::Empty;
                                } else {
                                    level_orders.entry(key).or_insert((LevelOrderState::Empty, LevelOrderState::Empty)).1 = 
                                        LevelOrderState::CancelPending { order_id: order_id.clone(), price, sent_at: Instant::now(), attempts: 1 };
                                }
                            }
                        }
                    }
                }
            }
            _ = log.tick(), if !shutting_down => {
                let md = data.read().await;
                let m = md.mid;
                let ofi = md.ofi;
                let sigma = md.sigma();
                let momentum = md.momentum();
                let update_interval = md.update_interval_ms;
                drop(md);
                
                let bal = balances.read().await.clone();
                let orders = active_orders.read().await.len();
                let inv = pnl.inv();
                let wr = if pnl.matched > 0 { (pnl.wins as f64 / pnl.matched as f64) * 100.0 } else { 0.0 };
                let skew = inv * GAMMA * sigma * sigma * 10000.0;
                
                // V10: Count local states
                let local_bids = level_orders.values().filter(|(b, _)| !b.is_empty()).count();
                let local_asks = level_orders.values().filter(|(_, a)| !a.is_empty()).count();
                
                info!("═══════════════════════════════════════════════════════════════");
                info!("{}s | B:{} S:{} | Matches:{} (W:{} L:{}) WR:{:.0}%", 
                    start.elapsed().as_secs(), pnl.buys, pnl.sells, pnl.matched, pnl.wins, pnl.losses, wr);
                info!("ORDERS:{} (L:{}/{}) | Inv:{:.3} ${:.0} | OFI:{:.3} | σ:{:.3} | Mom:{:.2}%", 
                    orders, local_bids, local_asks, inv, inv * m, ofi, sigma, momentum * 100.0);
                info!("BAL: {:.4} SOL, {:.2} USDT | Skew:{:.1}bps | Interval:{:.0}ms", 
                    bal.sol, bal.usdt, skew, update_interval);
                info!("SPREAD: ${:.4} | REBATE: ${:.4} | NET: ${:.4}", pnl.spread, pnl.reb, pnl.net());
                info!("═══════════════════════════════════════════════════════════════");
            }
        }
    }
    
    Ok(())
}
