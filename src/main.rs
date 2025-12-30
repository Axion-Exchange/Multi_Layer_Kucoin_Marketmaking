//! TEST_Multi_layers v9: Order Reconciliation via REST Polling
use anyhow::Result;
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tracing::info;

mod exchange;
use exchange::auth::KucoinAuth;
use exchange::ws_order_client_v2::{WsOrderClientV2, WsOrderRequest, WsCancelRequest};

// ═══════════════════════════════════════════════════════════════════
// CONFIGURATION - 25 LAYERS PER SIDE
// ═══════════════════════════════════════════════════════════════════
const LEVELS: [(f64, f64); 25] = [
    // Close layers: tighter refresh (50% of spread)
    (0.55, 0.28), (1.23, 0.62), (1.91, 0.96), (2.59, 1.30), (3.27, 1.64),
    (3.95, 1.98), (4.63, 2.32), (5.31, 2.66), (5.99, 3.00), (6.67, 3.34),
    // Mid layers: moderate refresh
    (7.35, 4.0), (8.03, 4.5), (8.71, 5.0), (9.39, 5.5), (10.07, 6.0),
    // Far layers: wider refresh (match spread)
    (10.75, 10.75), (11.43, 11.43), (12.11, 12.11), (12.79, 12.79), (13.47, 13.47),
    (14.15, 14.15), (14.83, 14.83), (15.51, 15.51), (16.19, 16.19), (16.87, 16.87)
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
}
impl MarketData {
    fn update(&mut self) {
        if self.last_mid > 0.0 && self.mid > 0.0 {
            let ret = (self.mid / self.last_mid).ln();
            self.ewma_var = VOL_EWMA_LAMBDA * self.ewma_var + (1.0 - VOL_EWMA_LAMBDA) * ret * ret;
        }
        self.last_mid = self.mid;
        let now = Instant::now();
        self.price_history.push_back((now, self.mid));
        let cutoff = now - Duration::from_secs(MOMENTUM_WINDOW_SECS);
        while let Some((t, _)) = self.price_history.front() {
            if *t < cutoff { self.price_history.pop_front(); } else { break; }
        }
    }
    fn sigma(&self) -> f64 { (self.ewma_var * 86400.0 * 365.0).sqrt().max(SIGMA_FLOOR) }
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).with_target(false).init();
    info!("═══ v9: Order Reconciliation via REST ═══");
    
    let auth = KucoinAuth::new(
        std::env::var("KUCOIN_API_KEY")?, std::env::var("KUCOIN_API_SECRET")?,
        std::env::var("KUCOIN_PASSPHRASE")?, true
    );
    let auth2 = auth.clone();
    let auth3 = auth.clone();
    let auth4 = auth.clone();
    
    let ws = Arc::new(RwLock::new(WsOrderClientV2::new(
        auth, "https://api.kucoin.com".into(), "wss://wsapi.kucoin.com/v1/private".into()
    )));
    { ws.write().await.connect().await?; }
    info!("[WS] OK");
    
    let data = Arc::new(RwLock::new(MarketData::default()));
    let balances = Arc::new(RwLock::new(Balances::default()));
    let active_orders = Arc::new(RwLock::new(Vec::<ActiveOrder>::new()));
    
    // Initial fetches
    let bal = poll_balances(&auth2).await;
    info!("[BAL] {:.4} SOL, {:.2} USDT", bal.sol, bal.usdt);
    *balances.write().await = bal;
    
    // Cancel all orders on startup
    let ep = "/api/v1/orders";
    let body = r#"{"symbol":"SOL-USDT"}"#;
    let (ts2, sig2, pw2, ver2) = auth3.sign("DELETE", ep, body);
    let _ = reqwest::Client::new().delete(format!("https://api.kucoin.com{}", ep))
        .header("KC-API-KEY", auth3.api_key()).header("KC-API-SIGN", &sig2)
        .header("KC-API-TIMESTAMP", &ts2).header("KC-API-PASSPHRASE", &pw2)
        .header("KC-API-KEY-VERSION", &ver2).header("Content-Type", "application/json")
        .body(body).send().await;
    info!("[STARTUP] Cancelled all existing orders");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let orders = poll_active_orders(&auth3).await;
    info!("[ORDERS] {} active", orders.len());
    *active_orders.write().await = orders;
    
    let d2 = data.clone();
    tokio::spawn(async move { binance_feed(d2).await; });
    
    loop { if data.read().await.mid > 0.0 { break; } tokio::time::sleep(Duration::from_millis(100)).await; }
    info!("[START] mid={:.2}", data.read().await.mid);
    
    // Track which levels have orders - key: level_bps, value: (bid_order_id, ask_order_id)
    let mut level_orders: HashMap<i32, (Option<(String, f64)>, Option<(String, f64)>)> = HashMap::new();
    for (bps, _) in LEVELS.iter() {
        level_orders.insert((*bps * 10.0) as i32, (None, None));
    }
    
    let mut pnl = PnL::default();
    let mut seen: HashSet<String> = HashSet::new();
    let start = Instant::now();
    
    let mut tick = tokio::time::interval(Duration::from_millis(500)); // Slower tick
    let mut log = tokio::time::interval(Duration::from_secs(30));
    let mut fp = tokio::time::interval(Duration::from_secs(5));
    let mut recon = tokio::time::interval(Duration::from_secs(1)); // Order reconciliation - 1s for strict limit
    let mut n: u64 = 0;
    
    let mut ofi_paused = false;
    let mut mom_paused = false;
    let mut last_mid = 0.0_f64;
    
    loop {
        tokio::select! {
            _ = recon.tick() => {
                // ═══ ORDER RECONCILIATION ═══
                let orders = poll_active_orders(&auth4).await;
                let new_bal = poll_balances(&auth3).await;
                *balances.write().await = new_bal;
                *active_orders.write().await = orders.clone();
                
                // Build set of known order IDs
                let active_ids: HashSet<String> = orders.iter().map(|o| o.order_id.clone()).collect();
                
                // Update level_orders - remove any that are no longer active
                for (_, (bid_id, ask_id)) in level_orders.iter_mut() {
                    if let Some((ref id, _)) = bid_id {
                        if !active_ids.contains(id) { *bid_id = None; }
                    }
                    if let Some((ref id, _)) = ask_id {
                        if !active_ids.contains(id) { *ask_id = None; }
                    }
                }
                
                // Count tracked orders
                let tracked: usize = level_orders.values().map(|(b, a)| {
                    (if b.is_some() { 1 } else { 0 }) + (if a.is_some() { 1 } else { 0 })
                }).sum();
                
                if orders.len() != tracked {
                    info!("[RECON] Active:{} Tracked:{}", orders.len(), tracked);
                }
                
                // ═══ ENFORCE MAX ORDERS PER SIDE (including orphans) ═══
                // Get all active orders from REST API sorted by price distance from mid
                let buy_orders: Vec<&ActiveOrder> = orders.iter().filter(|o| o.side == "buy").collect();
                let sell_orders: Vec<&ActiveOrder> = orders.iter().filter(|o| o.side == "sell").collect();
                
                // If bids over limit, cancel furthest (lowest price = furthest from mid for bids)
                if buy_orders.len() > MAX_ORDERS_PER_SIDE {
                    let mut sorted_buys = buy_orders.clone();
                    sorted_buys.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));
                    let to_cancel = sorted_buys.len() - MAX_ORDERS_PER_SIDE;
                    for order in sorted_buys.iter().take(to_cancel) {
                        info!("[LIMIT] Cancelling excess bid: ${:.2}", order.price);
                        let _ = ws.read().await.cancel_order(WsCancelRequest {
                            symbol: SYM.into(), order_id: Some(order.order_id.clone()), client_oid: None
                        }).await;
                    }
                }
                
                // If asks over limit, cancel furthest (highest price = furthest from mid for asks)
                if sell_orders.len() > MAX_ORDERS_PER_SIDE {
                    let mut sorted_asks = sell_orders.clone();
                    sorted_asks.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
                    let to_cancel = sorted_asks.len() - MAX_ORDERS_PER_SIDE;
                    for order in sorted_asks.iter().take(to_cancel) {
                        info!("[LIMIT] Cancelling excess ask: ${:.2}", order.price);
                        let _ = ws.read().await.cancel_order(WsCancelRequest {
                            symbol: SYM.into(), order_id: Some(order.order_id.clone()), client_oid: None
                        }).await;
                    }
                }
            }
            _ = fp.tick() => {
                for (side, sz, px) in poll_fills(&auth2, &mut seen).await {
                    let r = sz * px * REBATE / 10000.0;
                    if side == "buy" { pnl.buy(px, sz, r); } else { pnl.sell(px, sz, r); }
                }
            }
            _ = tick.tick() => {
                n += 1;
                let md = data.read().await;
                let m = md.mid;
                let ofi = md.ofi;
                let sigma = md.sigma();
                let momentum = md.momentum();
                drop(md);
                
                let bal = balances.read().await.clone();
                let active = active_orders.read().await.clone();
                let current_bids = active.iter().filter(|o| o.side == "buy").count();
                let current_asks = active.iter().filter(|o| o.side == "sell").count();
                
                if m <= 0.0 { continue; }
                
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
                
                if downtrend {
                    if !mom_paused { info!("[TREND] DOWN {:.2}% - selling only", momentum * 100.0); mom_paused = true; }
                    if inv <= 0.05 { info!("[TREND] Inv cleared, pausing"); continue; }
                } else if uptrend {
                    if !mom_paused { info!("[TREND] UP {:.2}% - buying only", momentum * 100.0); mom_paused = true; }
                    if inv >= -0.05 { info!("[TREND] No short, pausing"); continue; }
                } else if mom_paused { info!("[TREND] Normal"); mom_paused = false; }
                
                skip_bids = skip_bids || downtrend; // Skip bids in downtrend
                // skip_asks = skip_asks || uptrend; // DISABLED - always allow asks to refresh for quick exit
                
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
                    let (bid_id, ask_id) = level_orders.get(&key).cloned().unwrap_or((None, None));
                    
                    let max_skew = bps * 0.5;
                    let capped_skew = skew_bps.clamp(-max_skew, max_skew);
                    let bid_bps = bps + capped_skew;
                    let ask_bps = bps - capped_skew;
                    
                    let bp = ((m * (1.0 - bid_bps / 10000.0)) / 0.01).round() * 0.01;
                    let ap = ((m * (1.0 + ask_bps / 10000.0)) / 0.01).round() * 0.01;
                    
                    // ═══ REFRESH CHECK: Cancel stale orders beyond threshold ═══
                    if let Some((ref id, placed_px)) = bid_id {
                        let bps_diff = ((placed_px - bp).abs() / bp) * 10000.0;
                        if bps_diff > *thresh {
                            let _ = ws.read().await.cancel_order(WsCancelRequest {
                                symbol: SYM.into(), order_id: Some(id.clone()), client_oid: None
                            }).await;
                            level_orders.entry(key).or_insert((None, None)).0 = None;
                        }
                    }
                    if let Some((ref id, placed_px)) = ask_id {
                        let bps_diff = ((placed_px - ap).abs() / ap) * 10000.0;
                        if bps_diff > *thresh {
                            let _ = ws.read().await.cancel_order(WsCancelRequest {
                                symbol: SYM.into(), order_id: Some(id.clone()), client_oid: None
                            }).await;
                            level_orders.entry(key).or_insert((None, None)).1 = None;
                        }
                    }
                    
                    // Re-read after potential cancellation
                    let (bid_id, ask_id) = level_orders.get(&key).cloned().unwrap_or((None, None));
                    
                    // ═══ BID ORDER ═══
                    if bid_id.is_none() && !skip_bids && inv + bid_sz <= MAX_INV_SOL 
                        && bal.usdt >= bid_sz * bp && current_bids < MAX_ORDERS_PER_SIDE {
                        if let Ok(r) = ws.read().await.place_order(WsOrderRequest {
                            symbol: SYM.into(), side: "buy".into(),
                            price: format!("{:.2}", bp), size: format!("{:.2}", bid_sz),
                            client_oid: format!("b{}_{}", key, n),
                            order_type: "limit".into(), time_in_force: Some("GTC".into()),
                            post_only: Some(true)
                        }).await {
                            if r.success {
                                if let Some(ref oid) = r.order_id {
                                    level_orders.entry(key).or_insert((None, None)).0 = Some((oid.clone(), bp));
                                }
                            }
                        }
                    } else if bid_id.is_some() && (skip_bids || inv + bid_sz > MAX_INV_SOL) {
                        if let Some((ref id, _)) = bid_id {
                            let _ = ws.read().await.cancel_order(WsCancelRequest {
                                symbol: SYM.into(), order_id: Some(id.clone()), client_oid: None
                            }).await;
                            level_orders.entry(key).or_insert((None, None)).0 = None;
                        }
                    }
                    
                    // ═══ ASK ORDER ═══
                    if ask_id.is_none() && !skip_asks && inv - ask_sz >= -MAX_INV_SOL 
                        && bal.sol >= ask_sz && current_asks < MAX_ORDERS_PER_SIDE {
                        if let Ok(r) = ws.read().await.place_order(WsOrderRequest {
                            symbol: SYM.into(), side: "sell".into(),
                            price: format!("{:.2}", ap), size: format!("{:.2}", ask_sz),
                            client_oid: format!("a{}_{}", key, n),
                            order_type: "limit".into(), time_in_force: Some("GTC".into()),
                            post_only: Some(true)
                        }).await {
                            if r.success {
                                if let Some(ref oid) = r.order_id {
                                    level_orders.entry(key).or_insert((None, None)).1 = Some((oid.clone(), ap));
                                }
                            }
                        }
                    } else if ask_id.is_some() && inv - ask_sz < -MAX_INV_SOL {
                        if let Some((ref id, _)) = ask_id {
                            let _ = ws.read().await.cancel_order(WsCancelRequest {
                                symbol: SYM.into(), order_id: Some(id.clone()), client_oid: None
                            }).await;
                            level_orders.entry(key).or_insert((None, None)).1 = None;
                        }
                    }
                }
            }
            _ = log.tick() => {
                let md = data.read().await;
                let m = md.mid;
                let ofi = md.ofi;
                let sigma = md.sigma();
                let momentum = md.momentum();
                drop(md);
                
                let bal = balances.read().await.clone();
                let orders = active_orders.read().await.len();
                let inv = pnl.inv();
                let wr = if pnl.matched > 0 { (pnl.wins as f64 / pnl.matched as f64) * 100.0 } else { 0.0 };
                let skew = inv * GAMMA * sigma * sigma * 10000.0;
                
                info!("═══════════════════════════════════════════════════════════════");
                info!("{}s | B:{} S:{} | Matches:{} (W:{} L:{}) WR:{:.0}%", 
                    start.elapsed().as_secs(), pnl.buys, pnl.sells, pnl.matched, pnl.wins, pnl.losses, wr);
                info!("ORDERS:{} | Inv:{:.3} ${:.0} | OFI:{:.3} | σ:{:.3} | Mom:{:.2}% | Skew:{:.1}bps", 
                    orders, inv, inv * m, ofi, sigma, momentum * 100.0, skew);
                info!("BAL: {:.4} SOL, {:.2} USDT", bal.sol, bal.usdt);
                info!("SPREAD: ${:.4} | REBATE: ${:.4} | NET: ${:.4}", pnl.spread, pnl.reb, pnl.net());
                info!("═══════════════════════════════════════════════════════════════");
            }
        }
    }
}
