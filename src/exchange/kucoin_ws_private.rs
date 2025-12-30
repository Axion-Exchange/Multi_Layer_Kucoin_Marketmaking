//! KuCoin Private WebSocket Feed
//!
//! Connects to KuCoin private WebSocket for real-time order updates:
//! - Order fills (match events)
//! - Order status changes (open, done, cancelled)
//!
//! Uses exponential backoff for reconnection.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::interval;
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn, error, debug};
use anyhow::Result;
use serde::{Deserialize, Serialize};

use super::auth::KucoinAuth;
use super::order_state::{SharedOrderManager, Fill, Side as OrderSide};

/// Token response from /api/v1/bullet-private
#[derive(Debug, Deserialize)]
struct TokenResponse {
    code: String,
    data: TokenData,
}

#[derive(Debug, Deserialize)]
struct TokenData {
    token: String,
    #[serde(rename = "instanceServers")]
    instance_servers: Vec<InstanceServer>,
}

#[derive(Debug, Deserialize)]
struct InstanceServer {
    endpoint: String,
    #[serde(rename = "pingInterval")]
    ping_interval: u64,
    #[serde(rename = "pingTimeout")]
    ping_timeout: u64,
}

/// WebSocket message from KuCoin
#[derive(Debug, Deserialize)]
struct WsMessage {
    #[serde(rename = "type")]
    msg_type: String,
    topic: Option<String>,
    subject: Option<String>,
    data: Option<serde_json::Value>,
    id: Option<String>,
}

/// Subscribe message
#[derive(Debug, Serialize)]
struct SubscribeMessage {
    id: String,
    #[serde(rename = "type")]
    msg_type: String,
    topic: String,
    #[serde(rename = "privateChannel")]
    private_channel: bool,
    response: bool,
}

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
}

/// Reconnection stats
pub struct ReconnectStats {
    pub attempts: u32,
    pub last_connect: Option<Instant>,
    pub last_disconnect: Option<Instant>,
    pub total_disconnects: u32,
}

/// KuCoin Private WebSocket feed
pub struct KucoinPrivateWs {
    auth: KucoinAuth,
    rest_url: String,
    ws_url: String,
    order_manager: SharedOrderManager,
    symbol: String,
    state: Arc<RwLock<ConnectionState>>,
    reconnect_stats: Arc<RwLock<ReconnectStats>>,
}

impl KucoinPrivateWs {
    pub fn new(
        auth: KucoinAuth,
        rest_url: String,
        ws_url: String,
        order_manager: SharedOrderManager,
        symbol: String,
    ) -> Self {
        Self {
            auth,
            rest_url,
            ws_url,
            order_manager,
            symbol,
            state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            reconnect_stats: Arc::new(RwLock::new(ReconnectStats {
                attempts: 0,
                last_connect: None,
                last_disconnect: None,
                total_disconnects: 0,
            })),
        }
    }

    /// Get connection state
    pub fn state(&self) -> Arc<RwLock<ConnectionState>> {
        self.state.clone()
    }

    /// Get private token from REST API
    async fn get_token(&self) -> Result<(String, String, u64)> {
        let client = reqwest::Client::new();
        let endpoint = "/api/v1/bullet-private";
        let _timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            .to_string();
        
        let (ts, sign, pass, _key) = self.auth.sign("POST", endpoint, "");
        
        let resp = client
            .post(format!("{}{}", self.rest_url, endpoint))
            .header("KC-API-KEY", self.auth.api_key())
            .header("KC-API-SIGN", sign)
            .header("KC-API-TIMESTAMP", ts)
            .header("KC-API-PASSPHRASE", pass)
            .header("KC-API-KEY-VERSION", "2")
            .header("Content-Type", "application/json")
            .send()
            .await?;
        
        let body = resp.text().await?;
        let token_resp: TokenResponse = serde_json::from_str(&body)?;
        
        if token_resp.code != "200000" {
            anyhow::bail!("Failed to get token: {}", body);
        }
        
        let server = &token_resp.data.instance_servers[0];
        Ok((
            token_resp.data.token,
            server.endpoint.clone(),
            server.ping_interval,
        ))
    }

    /// Start the WebSocket connection with auto-reconnect
    pub async fn start(&self) -> Result<tokio::task::JoinHandle<()>> {
        let auth = self.auth.clone();
        let rest_url = self.rest_url.clone();
        let ws_url_override = self.ws_url.clone();
        let order_manager = self.order_manager.clone();
        let symbol = self.symbol.clone();
        let state = self.state.clone();
        let reconnect_stats = self.reconnect_stats.clone();

        let handle = tokio::spawn(async move {
            let mut backoff_secs = 1u64;
            const MAX_BACKOFF: u64 = 30;

            loop {
                // Update state
                *state.write().await = ConnectionState::Connecting;
                
                info!("[KUCOIN-WS] Getting private token...");
                
                // Get token
                let token_result = Self::get_token_static(&auth, &rest_url).await;
                let (token, endpoint, ping_interval) = match token_result {
                    Ok(t) => t,
                    Err(e) => {
                        error!("[KUCOIN-WS] Failed to get token: {}", e);
                        *state.write().await = ConnectionState::Reconnecting;
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                
                // Use colo endpoint if configured, otherwise use dynamic endpoint
                let ws_endpoint = if false && ws_url_override.contains("kucoin.com") { // NEVER use config URL for private WS - always use token URL
                    ws_url_override.clone()
                } else {
                    endpoint
                };
                
                let connect_url = format!("{}?token={}", ws_endpoint, token);
                info!("[KUCOIN-WS] Connecting to {}", ws_endpoint);

                // Connect
                let ws_result = connect_async(&connect_url).await;
                let (mut ws_stream, _) = match ws_result {
                    Ok(s) => s,
                    Err(e) => {
                        error!("[KUCOIN-WS] Connection failed: {}", e);
                        *state.write().await = ConnectionState::Reconnecting;
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                // Update state and stats
                *state.write().await = ConnectionState::Connected;
                {
                    let mut stats = reconnect_stats.write().await;
                    stats.last_connect = Some(Instant::now());
                    stats.attempts = 0;
                }
                backoff_secs = 1; // Reset backoff

                info!("[KUCOIN-WS] Connected! Subscribing to order updates...");

                // Subscribe to private order changes
                let sub_msg = SubscribeMessage {
                    id: format!("sub_{}", std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis()),
                    msg_type: "subscribe".to_string(),
                    topic: format!("/spotMarket/tradeOrdersV2:{}", symbol),
                    private_channel: true,
                    response: true,
                };

                let sub_json = serde_json::to_string(&sub_msg).unwrap();
                if let Err(e) = ws_stream.send(Message::Text(sub_json)).await {
                    error!("[KUCOIN-WS] Subscribe failed: {}", e);
                    continue;
                }

                // Ping timer
                let mut ping_interval_timer = interval(Duration::from_millis(ping_interval));
                let mut last_pong = Instant::now();

                // Message loop
                loop {
                    tokio::select! {
                        _ = ping_interval_timer.tick() => {
                            // Send ping
                            let ping_msg = r#"{"id":"ping","type":"ping"}"#;
                            if let Err(e) = ws_stream.send(Message::Text(ping_msg.to_string())).await {
                                warn!("[KUCOIN-WS] Ping failed: {}", e);
                                break;
                            }
                            
                            // Check pong timeout
                            if last_pong.elapsed() > Duration::from_secs(ping_interval / 1000 * 3) {
                                warn!("[KUCOIN-WS] Pong timeout, reconnecting...");
                                break;
                            }
                        }
                        msg = ws_stream.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    // Parse message
                                    if let Ok(ws_msg) = serde_json::from_str::<WsMessage>(&text) {
                                        match ws_msg.msg_type.as_str() {
                                            "pong" => {
                                                last_pong = Instant::now();
                                            }
                                            "welcome" => {
                                                debug!("[KUCOIN-WS] Welcome received");
                                            }
                                            "ack" => {
                                                info!("[KUCOIN-WS] Subscribed successfully");
                                            }
                                            "message" => {
                                                // Process order update
                                                if let Some(data) = ws_msg.data {
                                                    Self::process_order_message(&order_manager, &data).await;
                                                }
                                            }
                                            _ => {
                                                debug!("[KUCOIN-WS] Unknown message type: {}", ws_msg.msg_type);
                                            }
                                        }
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    let _ = ws_stream.send(Message::Pong(data)).await;
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("[KUCOIN-WS] Server closed connection");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!("[KUCOIN-WS] Error: {}", e);
                                    break;
                                }
                                None => {
                                    warn!("[KUCOIN-WS] Stream ended");
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                // Disconnected - update stats
                {
                    let mut stats = reconnect_stats.write().await;
                    stats.last_disconnect = Some(Instant::now());
                    stats.total_disconnects += 1;
                    stats.attempts += 1;
                }
                *state.write().await = ConnectionState::Reconnecting;
                
                info!("[KUCOIN-WS] Reconnecting in {}s...", backoff_secs);
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF);
            }
        });

        Ok(handle)
    }

    /// Static version for use in spawn
    async fn get_token_static(auth: &KucoinAuth, rest_url: &str) -> Result<(String, String, u64)> {
        let client = reqwest::Client::new();
        let endpoint = "/api/v1/bullet-private";
        
        let (ts, sign, pass, _key) = auth.sign("POST", endpoint, "");
        
        let resp = client
            .post(format!("{}{}", rest_url, endpoint))
            .header("KC-API-KEY", auth.api_key())
            .header("KC-API-SIGN", sign)
            .header("KC-API-TIMESTAMP", ts)
            .header("KC-API-PASSPHRASE", pass)
            .header("KC-API-KEY-VERSION", "2")
            .header("Content-Type", "application/json")
            .send()
            .await?;
        
        let body = resp.text().await?;
        let token_resp: TokenResponse = serde_json::from_str(&body)?;
        
        if token_resp.code != "200000" {
            anyhow::bail!("Failed to get token: {}", body);
        }
        
        let server = &token_resp.data.instance_servers[0];
        Ok((
            token_resp.data.token,
            server.endpoint.clone(),
            server.ping_interval,
        ))
    }

    /// Process order update message
    async fn process_order_message(order_manager: &SharedOrderManager, data: &serde_json::Value) {
        // Parse order update
        let order_id = data.get("orderId").and_then(|v| v.as_str()).unwrap_or("");
        let msg_type = data.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let _status = data.get("status").and_then(|v| v.as_str()).unwrap_or("");
        
        match msg_type {
            "match" => {
                // Fill event
                let price = data.get("matchPrice")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let size = data.get("matchSize")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let side_str = data.get("side").and_then(|v| v.as_str()).unwrap_or("");
                let trade_id = data.get("tradeId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");

                if price > 0.0 && size > 0.0 {
                    let side = if side_str == "buy" { OrderSide::Buy } else { OrderSide::Sell };
                    
                    let fill = Fill {
                        order_id: order_id.to_string(),
                        trade_id: trade_id.to_string(),
                        side,
                        price,
                        size,
                        fee: 0.0, // Calculate from maker_fee
                        fee_currency: "USDT".to_string(),
                        timestamp: 0,
                    };

                    let mut mgr = order_manager.write().await;
                    mgr.on_fill(&fill);
                    
                    info!("[FILL] {} {} @ ${:.4} (order {})",
                        side_str.to_uppercase(), size, price, order_id);
                }
            }
            "canceled" | "done" => {
                // Order cancelled or completed
                let mut mgr = order_manager.write().await;
                mgr.on_cancel(order_id);
                debug!("[ORDER] {} - {}", order_id, msg_type);
            }
            "open" => {
                debug!("[ORDER] {} opened", order_id);
            }
            _ => {}
        }
    }
}
