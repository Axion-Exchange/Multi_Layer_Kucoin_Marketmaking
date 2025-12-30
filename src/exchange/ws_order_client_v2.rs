//! WebSocket Order Client for KuCoin with Auto-Reconnection
//! Ultra-low latency order entry with automatic reconnection on disconnect

use anyhow::{Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, oneshot, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn, error, debug};

use super::KucoinAuth;

/// WebSocket Order Request
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WsOrderRequest {
    pub symbol: String,
    pub side: String,      // "buy" or "sell"
    pub price: String,
    pub size: String,
    pub client_oid: String,
    #[serde(rename = "type")]
    pub order_type: String, // "limit" or "market"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post_only: Option<bool>,
}

/// WebSocket Order Response
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct WsOrderResponse {
    pub order_id: Option<String>,
    pub client_oid: Option<String>,
    pub success: bool,
    pub code: Option<String>,
    pub msg: Option<String>,
}

/// Cancel Request
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WsCancelRequest {
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
}

/// Latency tracker
#[derive(Debug)]
pub struct LatencyStats {
    pub count: u64,
    pub total_us: u64,
    pub min_us: u64,
    pub max_us: u64,
    pub last_us: u64,
}

impl LatencyStats {
    pub fn new() -> Self {
        Self { count: 0, total_us: 0, min_us: u64::MAX, max_us: 0, last_us: 0 }
    }
    
    pub fn record(&mut self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.count += 1;
        self.total_us += us;
        self.min_us = self.min_us.min(us);
        self.max_us = self.max_us.max(us);
        self.last_us = us;
    }
    
    pub fn avg_us(&self) -> u64 {
        if self.count > 0 { self.total_us / self.count } else { 0 }
    }
    
    pub fn summary(&self) -> String {
        if self.count == 0 {
            return "No data".to_string();
        }
        format!("avg={:.2}ms min={:.2}ms max={:.2}ms last={:.2}ms n={}",
            self.avg_us() as f64 / 1000.0,
            self.min_us as f64 / 1000.0,
            self.max_us as f64 / 1000.0,
            self.last_us as f64 / 1000.0,
            self.count)
    }
}

/// Pending request awaiting response
struct PendingRequest {
    tx: oneshot::Sender<WsOrderResponse>,
    sent_at: Instant,
}

/// Reconnection stats
#[derive(Debug, Default)]
pub struct ReconnectStats {
    pub total_connects: u32,
    pub total_disconnects: u32,
    pub consecutive_failures: u32,
    pub last_connect: Option<Instant>,
    pub last_disconnect: Option<Instant>,
}

/// Internal connection state
struct ConnectionState {
    msg_tx: Option<mpsc::Sender<String>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

/// WebSocket Order Client with Auto-Reconnection
pub struct WsOrderClientV2 {
    auth: KucoinAuth,
    rest_url: String,
    ws_url: String,
    connected: Arc<AtomicBool>,
    reconnecting: Arc<AtomicBool>,
    request_counter: AtomicU64,
    pending: Arc<RwLock<HashMap<String, PendingRequest>>>,
    
    // Connection state (protected by mutex for reconnection)
    conn_state: Arc<Mutex<ConnectionState>>,
    
    // Reconnection control
    reconnect_stats: Arc<RwLock<ReconnectStats>>,
    should_reconnect: Arc<AtomicBool>,
    max_reconnect_attempts: u32,
    
    // Latency tracking
    place_latency: Arc<RwLock<LatencyStats>>,
    cancel_latency: Arc<RwLock<LatencyStats>>,
}

impl WsOrderClientV2 {
    /// Create new client with auto-reconnection enabled
    pub fn new(auth: KucoinAuth, rest_url: String, ws_url: String) -> Self {
        info!("[WS-ORDER] Configured with endpoint: {} (auto-reconnect enabled)", ws_url);
        Self {
            auth,
            rest_url,
            ws_url,
            connected: Arc::new(AtomicBool::new(false)),
            reconnecting: Arc::new(AtomicBool::new(false)),
            request_counter: AtomicU64::new(0),
            pending: Arc::new(RwLock::new(HashMap::new())),
            conn_state: Arc::new(Mutex::new(ConnectionState {
                msg_tx: None,
                handle: None,
            })),
            reconnect_stats: Arc::new(RwLock::new(ReconnectStats::default())),
            should_reconnect: Arc::new(AtomicBool::new(true)),
            max_reconnect_attempts: 10,
            place_latency: Arc::new(RwLock::new(LatencyStats::new())),
            cancel_latency: Arc::new(RwLock::new(LatencyStats::new())),
        }
    }
    
    /// Get private WS token from REST API
    async fn get_ws_token(&self) -> Result<(String, String)> {
        let endpoint = "/api/v1/bullet-private";
        
        // Use auth.sign() which returns (timestamp, signature, passphrase, version)
        let (timestamp, signature, passphrase, version) = self.auth.sign("POST", endpoint, "");
        
        let client = reqwest::Client::new();
        let resp = client.post(format!("{}{}", self.rest_url, endpoint))
            .header("KC-API-KEY", self.auth.api_key())
            .header("KC-API-SIGN", &signature)
            .header("KC-API-TIMESTAMP", &timestamp)
            .header("KC-API-PASSPHRASE", &passphrase)
            .header("KC-API-KEY-VERSION", &version)
            .header("Content-Type", "application/json")
            .send()
            .await?;
        
        let status = resp.status();
        let text = resp.text().await?;
        
        if !status.is_success() {
            return Err(anyhow!("Failed to get WS token: {} - {}", status, text));
        }
        
        #[derive(Deserialize)]
        struct ApiResp {
            code: String,
            data: Option<TokenData>,
            msg: Option<String>,
        }
        #[derive(Deserialize)]
        struct TokenData {
            token: String,
            #[serde(rename = "instanceServers")]
            instance_servers: Vec<InstanceServer>,
        }
        #[derive(Deserialize)]
        struct InstanceServer {
            endpoint: String,
        }
        
        let api_resp: ApiResp = serde_json::from_str(&text)
            .map_err(|e| anyhow!("Failed to parse token response: {} - {}", e, text))?;
        
        if api_resp.code != "200000" {
            return Err(anyhow!("API error: {} - {:?}", api_resp.code, api_resp.msg));
        }
        
        let data = api_resp.data.ok_or_else(|| anyhow!("No data in token response"))?;
        let endpoint = data.instance_servers.first()
            .map(|s| s.endpoint.clone())
            .unwrap_or_else(|| self.ws_url.clone());
        
        Ok((data.token, endpoint))
    }
    
    fn next_id(&self) -> String {
        let n = self.request_counter.fetch_add(1, Ordering::SeqCst);
        format!("ws_ord_{}", n)
    }
    
    /// Connect to WebSocket endpoint
    async fn connect_internal(&self) -> Result<()> {
        // Don't connect if already connected or reconnecting
        if self.connected.load(Ordering::SeqCst) {
            return Ok(());
        }
        
        self.reconnecting.store(true, Ordering::SeqCst);
        
        // Use URL-based authentication for Order Entry API
        // (Different from bullet-private token auth used for private WS channels)
        info!("[WS-ORDER] Signing for URL-based auth...");
        
        let (timestamp, signature, passphrase) = self.auth.sign_ws_url();
        
        // Add partner/partner_sign per tiagosiebler/kucoin-api
        let partner = "NODESDK";
        let partner_key = "d28f5b4a-179d-4fcb-9c00-c8319c0bb82c";
        // partner_sign = HMAC(timestamp + partner + apikey, partner_key)
        let partner_data = format!("{}{}{}", timestamp, partner, self.auth.api_key());
        let partner_sign = {
            use hmac::{Hmac, Mac};
            use sha2::Sha256;
            use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
            type HmacSha256 = Hmac<Sha256>;
            let mut mac = HmacSha256::new_from_slice(partner_key.as_bytes()).unwrap();
            mac.update(partner_data.as_bytes());
            BASE64.encode(mac.finalize().into_bytes())
        };
        
        let connect_url = format!(
            "{}?apikey={}&passphrase={}&sign={}&timestamp={}",
            self.ws_url,
            urlencoding::encode(self.auth.api_key()),
            urlencoding::encode(&passphrase),
            urlencoding::encode(&signature),
            &timestamp
        );
        
        info!("[WS-ORDER] Connecting with URL-based auth to: {}", self.ws_url);
        info!("[WS-ORDER] Full connect URL: {}", connect_url);
        
        let (ws_stream, _) = connect_async(&connect_url).await
            .map_err(|e| anyhow!("WS connect failed: {}", e))?;
        
        let (mut write, mut read) = ws_stream.split();
        
        // Create channel for outgoing messages
        let (tx, mut rx) = mpsc::channel::<String>(1000);
        
        // Store the sender
        {
            let mut state = self.conn_state.lock().await;
            state.msg_tx = Some(tx);
        }
        
        self.connected.store(true, Ordering::SeqCst);
        self.reconnecting.store(false, Ordering::SeqCst);
        
        // Update stats
        {
            let mut stats = self.reconnect_stats.write().await;
            stats.total_connects += 1;
            stats.consecutive_failures = 0;
            stats.last_connect = Some(Instant::now());
        }
        
        info!("[WS-ORDER] ✓ Connected to WS order endpoint");
        
        let connected = self.connected.clone();
        let pending = self.pending.clone();
        let auth_clone = self.auth.clone();
        let place_latency = self.place_latency.clone();
        let cancel_latency = self.cancel_latency.clone();
        let reconnect_stats = self.reconnect_stats.clone();
        
        let handle = tokio::spawn(async move {
            // Don't send initial ping - wait for welcome message first
            // The immediate ping was interfering with the auth flow
            info!("[WS-ORDER] Connected, waiting for auth response...");
            
            // Continue with 2s ping interval
            let mut ping_interval = tokio::time::interval(Duration::from_secs(2)); 
            ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ping_interval.tick().await; // Skip first immediate tick
            
            loop {
                tokio::select! {
                    // Send outgoing messages
                    Some(msg) = rx.recv() => {
                        debug!("[WS-ORDER] Sending: {}", msg);
                        if let Err(e) = write.send(Message::Text(msg)).await {
                            error!("[WS-ORDER] Send error: {}", e);
                            break;
                        }
                    }
                    
                    // Receive responses
                    Some(msg) = read.next() => {
                        match msg {
                            Ok(Message::Text(text)) => {
                                info!("[WS-ORDER] RECV_MSG: {}", text);
                                // Parse response and complete pending request
                                if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
                                    // Handle session verification - server sends sessionId after connect
                                    if let Some(session_id) = resp.get("sessionId").and_then(|v| v.as_str()) {
                                        // Check if this is the welcome message (has "data": "welcome")
                                        if let Some(data) = resp.get("data").and_then(|v| v.as_str()) {
                                            if data == "welcome" {
                                                info!("[WS-ORDER] Session authenticated! sessionId={}, pingInterval={:?}",
                                                    session_id, resp.get("pingInterval"));
                                                continue;
                                            }
                                        }
                                        // If just sessionId + timestamp (no data, no code), sign and send back
                                        if resp.get("data").is_none() && resp.get("code").is_none() {
                                            info!("[WS-ORDER] Received sessionId, signing response...");
                                            
                                            // CRITICAL: Per KuCoin docs, we must sign the raw session JSON
                                            // and send ONLY the signature string back
                                            let session_sig = {
                                                use hmac::{Hmac, Mac};
                                                use sha2::Sha256;
                                                use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
                                                type HmacSha256 = Hmac<Sha256>;
                                                let mut mac = HmacSha256::new_from_slice(auth_clone.api_secret().as_bytes()).unwrap();
                                                mac.update(text.as_bytes());
                                                BASE64.encode(mac.finalize().into_bytes())
                                            };
                                            
                                            info!("[WS-ORDER] Sending session signature...");
                                            if let Err(e) = write.send(Message::Text(session_sig)).await {
                                                error!("[WS-ORDER] Failed to send session signature: {}", e);
                                            }
                                            continue;
                                        }
                                        // If data is "welcome", session is authenticated
                                        if let Some(data) = resp.get("data").and_then(|v| v.as_str()) {
                                            if data == "welcome" {
                                                info!("[WS-ORDER] Session authenticated successfully! pingInterval={:?}",
                                                    resp.get("pingInterval"));
                                            }
                                        }
                                    }
                                    
                                    if let Some(id) = resp.get("id").and_then(|v| v.as_str()) {
                                        // Skip ping/pong responses
                                        if id == "ping" {
                                            continue;
                                        }
                                        
                                        let mut pending_guard = pending.write().await;
                                        if let Some(req) = pending_guard.remove(id) {
                                            let latency = req.sent_at.elapsed();
                                            
                                            // Track latency based on request type
                                            if id.contains("place") {
                                                let mut stats = place_latency.write().await;
                                                stats.record(latency);
                                            } else if id.contains("cancel") {
                                                let mut stats = cancel_latency.write().await;
                                                stats.record(latency);
                                            }
                                            
                                            let order_resp = WsOrderResponse {
                                                order_id: resp.get("data").and_then(|d| d.get("orderId")).and_then(|v| v.as_str()).map(String::from),
                                                client_oid: resp.get("data").and_then(|d| d.get("clientOid")).and_then(|v| v.as_str()).map(String::from),
                                                success: resp.get("code").and_then(|v| v.as_str()) == Some("200000"),
                                                code: resp.get("code").and_then(|v| v.as_str()).map(String::from),
                                                msg: resp.get("msg").and_then(|v| v.as_str()).map(String::from),
                                            };
                                            let _ = req.tx.send(order_resp);
                                            
                                            debug!("[WS-ORDER] Response in {:.2}ms", latency.as_secs_f64() * 1000.0);
                                        }
                                    }
                                }
                            }
                            Ok(Message::Ping(data)) => {
                                let _ = write.send(Message::Pong(data)).await;
                            }
                            Ok(Message::Pong(_)) => {
                                // Server responded to our ping
                            }
                            Ok(Message::Close(_)) => {
                                warn!("[WS-ORDER] Connection closed by server");
                                break;
                            }
                            Err(e) => {
                                error!("[WS-ORDER] Recv error: {}", e);
                                break;
                            }
                            _ => {}
                        }
                    }
                    
                    // Send ping to keep connection alive
                    _ = ping_interval.tick() => {
                        let ts = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis();
                                    let ping = json!({"id": "ping", "op": "ping", "timestamp": ts}).to_string();
                        if let Err(e) = write.send(Message::Text(ping)).await {
                            error!("[WS-ORDER] Ping error: {}", e);
                            break;
                        }
                        info!("[WS-ORDER] SENT_PING");
                    }
                }
            }
            
            // Mark disconnected and update stats
            connected.store(false, Ordering::SeqCst);
            {
                let mut stats = reconnect_stats.write().await;
                stats.total_disconnects += 1;
                stats.last_disconnect = Some(Instant::now());
            }
            warn!("[WS-ORDER] Connection loop ended");
        });
        
        // Store the handle
        {
            let mut state = self.conn_state.lock().await;
            state.handle = Some(handle);
        }
        
        Ok(())
    }
    
    /// Start the client with auto-reconnection
    pub async fn start(&self) -> Result<tokio::task::JoinHandle<()>> {
        // Initial connection
        self.connect_internal().await?;
        
        // Spawn reconnection monitor
        let connected = self.connected.clone();
        let reconnecting = self.reconnecting.clone();
        let should_reconnect = self.should_reconnect.clone();
        let reconnect_stats = self.reconnect_stats.clone();
        let max_attempts = self.max_reconnect_attempts;
        
        // Clone self for reconnection
        let auth = self.auth.clone();
        let rest_url = self.rest_url.clone();
        let ws_url = self.ws_url.clone();
        let pending = self.pending.clone();
        let auth_clone = self.auth.clone();
        let conn_state = self.conn_state.clone();
        let place_latency = self.place_latency.clone();
        let cancel_latency = self.cancel_latency.clone();
        
        let handle = tokio::spawn(async move {
            let mut check_interval = tokio::time::interval(Duration::from_secs(2));
            
            loop {
                check_interval.tick().await;
                
                // Check if we should stop
                if !should_reconnect.load(Ordering::SeqCst) {
                    info!("[WS-ORDER] Reconnection disabled, stopping monitor");
                    break;
                }
                
                // Check if disconnected and not already reconnecting
                if !connected.load(Ordering::SeqCst) && !reconnecting.load(Ordering::SeqCst) {
                    let stats = reconnect_stats.read().await;
                    let failures = stats.consecutive_failures;
                    drop(stats);
                    
                    if failures >= max_attempts {
                        error!("[WS-ORDER] Max reconnection attempts ({}) reached, giving up", max_attempts);
                        break;
                    }
                    
                    // Calculate backoff delay: min(1s * 2^failures, 30s)
                    let delay_secs = (1u64 << failures.min(5)).min(30);
                    info!("[WS-ORDER] Reconnecting in {}s (attempt {}/{})", delay_secs, failures + 1, max_attempts);
                    
                    tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                    
                    reconnecting.store(true, Ordering::SeqCst);
                    
                    // Use URL-based auth for reconnection (same as initial connect)
                    let (timestamp, signature, passphrase) = auth.sign_ws_url();
                    
                    let connect_url = format!(
                        "{}?version=3&apikey={}&sign={}&passphrase={}&timestamp={}",
                        ws_url,
                        urlencoding::encode(auth.api_key()),
                        urlencoding::encode(&signature),
                        urlencoding::encode(&passphrase),
                        &timestamp
                    );
                    
                    info!("[WS-ORDER] Reconnecting with URL-based auth...");
                    
                    match connect_async(&connect_url).await {
                                Ok((ws_stream, _)) => {
                                    let (mut write, mut read) = ws_stream.split();
                                    let (tx, mut rx) = mpsc::channel::<String>(1000);
                                    
                                    // Store new sender
                                    {
                                        let mut state = conn_state.lock().await;
                                        state.msg_tx = Some(tx);
                                    }
                                    
                                    connected.store(true, Ordering::SeqCst);
                                    reconnecting.store(false, Ordering::SeqCst);
                                    
                                    {
                                        let mut stats = reconnect_stats.write().await;
                                        stats.total_connects += 1;
                                        stats.consecutive_failures = 0;
                                        stats.last_connect = Some(Instant::now());
                                    }
                                    
                                    info!("[WS-ORDER] ✓ Reconnected successfully");
                                    
                                    // Spawn new connection handler
                                    let connected_inner = connected.clone();
                                    let pending_inner = pending.clone();
                                    let place_latency_inner = place_latency.clone();
                                    let cancel_latency_inner = cancel_latency.clone();
                                    let reconnect_stats_inner = reconnect_stats.clone();
                                    
                                    let handle = tokio::spawn(async move {
                                        // Send initial ping IMMEDIATELY to beat KuCoin's 3s timeout
                                        let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let initial_ping = json!({"id": "ping", "op": "ping", "timestamp": ts}).to_string();
                                        if let Err(e) = write.send(Message::Text(initial_ping)).await {
                                            error!("[WS-ORDER] Initial ping failed: {}", e);
                                        }
                                        
                                        // Continue with 2s ping interval
                                        let mut ping_interval = tokio::time::interval(Duration::from_secs(2)); 
                                        ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                                        ping_interval.tick().await; // Skip first immediate tick
                                        
                                        loop {
                                            tokio::select! {
                                                Some(msg) = rx.recv() => {
                                                    if let Err(e) = write.send(Message::Text(msg)).await {
                                                        error!("[WS-ORDER] Send error: {}", e);
                                                        break;
                                                    }
                                                }
                                                
                                                Some(msg) = read.next() => {
                                                    match msg {
                                                        Ok(Message::Text(text)) => {
                                                            if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
                                                                if let Some(id) = resp.get("id").and_then(|v| v.as_str()) {
                                                                    if id == "ping" { continue; }
                                                                    
                                                                    let mut pending_guard = pending_inner.write().await;
                                                                    if let Some(req) = pending_guard.remove(id) {
                                                                        let latency = req.sent_at.elapsed();
                                                                        
                                                                        if id.contains("place") {
                                                                            let mut stats = place_latency_inner.write().await;
                                                                            stats.record(latency);
                                                                        } else if id.contains("cancel") {
                                                                            let mut stats = cancel_latency_inner.write().await;
                                                                            stats.record(latency);
                                                                        }
                                                                        
                                                                        let order_resp = WsOrderResponse {
                                                                            order_id: resp.get("data").and_then(|d| d.get("orderId")).and_then(|v| v.as_str()).map(String::from),
                                                                            client_oid: resp.get("data").and_then(|d| d.get("clientOid")).and_then(|v| v.as_str()).map(String::from),
                                                                            success: resp.get("code").and_then(|v| v.as_str()) == Some("200000"),
                                                                            code: resp.get("code").and_then(|v| v.as_str()).map(String::from),
                                                                            msg: resp.get("msg").and_then(|v| v.as_str()).map(String::from),
                                                                        };
                                                                        let _ = req.tx.send(order_resp);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        Ok(Message::Ping(data)) => { let _ = write.send(Message::Pong(data)).await; }
                                                        Ok(Message::Close(_)) | Err(_) => break,
                                                        _ => {}
                                                    }
                                                }
                                                
                                                _ = ping_interval.tick() => {
                                                    let ts = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis();
                                    let ping = json!({"id": "ping", "op": "ping", "timestamp": ts}).to_string();
                                                    if write.send(Message::Text(ping)).await.is_err() { break; }
                                                }
                                            }
                                        }
                                        
                                        connected_inner.store(false, Ordering::SeqCst);
                                        {
                                            let mut stats = reconnect_stats_inner.write().await;
                                            stats.total_disconnects += 1;
                                            stats.last_disconnect = Some(Instant::now());
                                        }
                                        warn!("[WS-ORDER] Connection loop ended");
                                    });
                                    
                                    // Store handle
                                    {
                                        let mut state = conn_state.lock().await;
                                        state.handle = Some(handle);
                                    }
                                }
                                Err(e) => {
                                    error!("[WS-ORDER] Reconnect failed: {}", e);
                                    reconnecting.store(false, Ordering::SeqCst);
                                    let mut stats = reconnect_stats.write().await;
                                    stats.consecutive_failures += 1;
                                }
                    }
                }
            }
        });
        
        Ok(handle)
    }
    
    /// Legacy connect method - now calls start()
    pub async fn connect(&mut self) -> Result<tokio::task::JoinHandle<()>> {
        self.start().await
    }
    
    /// Stop auto-reconnection
    pub fn stop(&self) {
        self.should_reconnect.store(false, Ordering::SeqCst);
    }
    
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
    
    pub fn is_reconnecting(&self) -> bool {
        self.reconnecting.load(Ordering::SeqCst)
    }
    
    /// Get sender for orders
    async fn get_sender(&self) -> Result<mpsc::Sender<String>> {
        let state = self.conn_state.lock().await;
        state.msg_tx.clone().ok_or_else(|| anyhow!("Not connected"))
    }
    
    /// Place order via WebSocket
    pub async fn place_order(&self, req: WsOrderRequest) -> Result<WsOrderResponse> {
        let tx = self.get_sender().await?;
        
        let id = format!("place_{}", self.next_id());
        // KuCoin Pro API format for order placement (spot.order for Classic Account)
        let msg = json!({
            "id": id,
            "op": "spot.order",
            "args": {
                "symbol": req.symbol,
                "side": req.side,
                "price": req.price,
                "size": req.size,
                "clientOid": req.client_oid,
                "type": req.order_type,
                "timeInForce": req.time_in_force.unwrap_or_else(|| "GTC".to_string()),
                "postOnly": true
            }
        });
        
        let (resp_tx, resp_rx) = oneshot::channel();
        {
            let mut pending = self.pending.write().await;
            pending.insert(id.clone(), PendingRequest { tx: resp_tx, sent_at: Instant::now() });
        }
        
        tx.send(msg.to_string()).await?;
        
        // Wait for response with timeout
        match tokio::time::timeout(Duration::from_secs(5), resp_rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(anyhow!("Response channel closed")),
            Err(_) => {
                let mut pending = self.pending.write().await;
                pending.remove(&id);
                Err(anyhow!("Order timeout"))
            }
        }
    }
    
    /// Cancel order via WebSocket
    pub async fn cancel_order(&self, req: WsCancelRequest) -> Result<WsOrderResponse> {
        let tx = self.get_sender().await?;
        
        let id = format!("cancel_{}", self.next_id());
        
        // Build args object manually to avoid null values
        let mut args_obj = serde_json::Map::new();
        args_obj.insert("symbol".to_string(), serde_json::Value::String(req.symbol));
        if let Some(oid) = req.order_id {
            args_obj.insert("orderId".to_string(), serde_json::Value::String(oid));
        }
        if let Some(coid) = req.client_oid {
            args_obj.insert("clientOid".to_string(), serde_json::Value::String(coid));
        }
        
        let msg = json!({
            "id": id,
            "op": "spot.cancel",
            "args": serde_json::Value::Object(args_obj)
        });
        
        let (resp_tx, resp_rx) = oneshot::channel();
        {
            let mut pending = self.pending.write().await;
            pending.insert(id.clone(), PendingRequest { tx: resp_tx, sent_at: Instant::now() });
        }
        
        // DEBUG: Log the actual message being sent
        info!("[WS-ORDER] Sending cancel: {}", msg.to_string());
        
        tx.send(msg.to_string()).await?;
        
        match tokio::time::timeout(Duration::from_secs(5), resp_rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(anyhow!("Response channel closed")),
            Err(_) => {
                let mut pending = self.pending.write().await;
                pending.remove(&id);
                Err(anyhow!("Cancel timeout"))
            }
        }
    }
    
    /// Get reconnection statistics
    pub async fn get_reconnect_stats(&self) -> (u32, u32, u32) {
        let stats = self.reconnect_stats.read().await;
        (stats.total_connects, stats.total_disconnects, stats.consecutive_failures)
    }
    
    /// Get latency stats
    pub async fn get_latency_stats(&self) -> (String, String) {
        let place = self.place_latency.read().await;
        let cancel = self.cancel_latency.read().await;
        (place.summary(), cancel.summary())
    }
    
    /// Log latency summary
    pub async fn log_latency(&self) {
        let (place, cancel) = self.get_latency_stats().await;
        let (connects, disconnects, failures) = self.get_reconnect_stats().await;
        info!("[WS-ORDER] PLACE latency: {}", place);
        info!("[WS-ORDER] CANCEL latency: {}", cancel);
        info!("[WS-ORDER] Connections: {} connects, {} disconnects, {} failures", 
            connects, disconnects, failures);
    }
}
