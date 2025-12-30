//! KuCoin Public WebSocket Feed
//!
//! Receives real-time orderbook updates for market data.

use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn, error, debug};
use anyhow::Result;

use super::types::OrderBook;

/// KuCoin public WebSocket orderbook feed
pub struct KucoinPublicWs {
    ws_url: String,
    symbol: String,
    orderbook: Arc<RwLock<OrderBook>>,
}

impl KucoinPublicWs {
    pub fn new(ws_url: String, symbol: String) -> Self {
        Self {
            ws_url,
            symbol,
            orderbook: Arc::new(RwLock::new(OrderBook::default())),
        }
    }

    /// Get shared orderbook handle
    pub fn orderbook(&self) -> Arc<RwLock<OrderBook>> {
        self.orderbook.clone()
    }

    /// Start the WebSocket feed
    pub async fn start(&self, token: &str) -> Result<tokio::task::JoinHandle<()>> {
        let url = format!(
            "{}?token={}&connectId={}",
            self.ws_url,
            token,
            uuid::Uuid::new_v4()
        );
        
        let symbol = self.symbol.clone();
        let orderbook = self.orderbook.clone();

        info!("[KC-WS-PUB] Connecting to {} for {}", self.ws_url, symbol);

        let handle = tokio::spawn(async move {
            loop {
                match Self::run_connection(&url, &symbol, &orderbook).await {
                    Ok(_) => {
                        warn!("[KC-WS-PUB] Connection closed, reconnecting in 1s...");
                    }
                    Err(e) => {
                        error!("[KC-WS-PUB] Connection error: {:?}, reconnecting in 1s...", e);
                    }
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        Ok(handle)
    }

    async fn run_connection(
        url: &str,
        symbol: &str,
        orderbook: &Arc<RwLock<OrderBook>>,
    ) -> Result<()> {
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        info!("[KC-WS-PUB] Connected successfully");

        // Subscribe to level2 orderbook
        let sub_msg = serde_json::json!({
            "id": uuid::Uuid::new_v4().to_string(),
            "type": "subscribe",
            "topic": format!("/market/level2:{}", symbol),
            "privateChannel": false,
            "response": true
        });

        write.send(Message::Text(sub_msg.to_string())).await?;

        // Ping task
        let ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        tokio::pin!(ping_interval);

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    let ping = serde_json::json!({
                        "id": uuid::Uuid::new_v4().to_string(),
                        "type": "ping"
                    });
                    if write.send(Message::Text(ping.to_string())).await.is_err() {
                        break;
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(e) = Self::handle_message(&text, orderbook).await {
                                debug!("[KC-WS-PUB] Parse error: {:?}", e);
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            warn!("[KC-WS-PUB] Server closed connection");
                            break;
                        }
                        Some(Err(e)) => {
                            error!("[KC-WS-PUB] WebSocket error: {:?}", e);
                            break;
                        }
                        None => break,
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_message(text: &str, orderbook: &Arc<RwLock<OrderBook>>) -> Result<()> {
        let v: serde_json::Value = serde_json::from_str(text)?;
        
        // Check if it's a data message
        if v.get("type").and_then(|t| t.as_str()) != Some("message") {
            return Ok(());
        }

        let data = match v.get("data") {
            Some(d) => d,
            None => return Ok(()),
        };

        // Parse changes
        let changes = match data.get("changes") {
            Some(c) => c,
            None => return Ok(()),
        };

        let mut ob = orderbook.write().await;

        // Process bid changes
        if let Some(bids) = changes.get("bids").and_then(|b| b.as_array()) {
            for bid in bids {
                if let Some(arr) = bid.as_array() {
                    if let (Some(price), Some(size)) = (
                        arr.get(0).and_then(|p| p.as_str()?.parse::<f64>().ok()),
                        arr.get(1).and_then(|s| s.as_str()?.parse::<f64>().ok()),
                    ) {
                        // Update or remove from orderbook (size 0 = remove)
                        if size == 0.0 {
                            ob.bids.retain(|(p, _)| (*p - price).abs() > 0.00001);
                        } else {
                            // Update existing or insert
                            if let Some(pos) = ob.bids.iter().position(|(p, _)| (*p - price).abs() < 0.00001) {
                                ob.bids[pos].1 = size;
                            } else {
                                ob.bids.push((price, size));
                                ob.bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                            }
                        }
                    }
                }
            }
        }

        // Process ask changes
        if let Some(asks) = changes.get("asks").and_then(|a| a.as_array()) {
            for ask in asks {
                if let Some(arr) = ask.as_array() {
                    if let (Some(price), Some(size)) = (
                        arr.get(0).and_then(|p| p.as_str()?.parse::<f64>().ok()),
                        arr.get(1).and_then(|s| s.as_str()?.parse::<f64>().ok()),
                    ) {
                        if size == 0.0 {
                            ob.asks.retain(|(p, _)| (*p - price).abs() > 0.00001);
                        } else {
                            if let Some(pos) = ob.asks.iter().position(|(p, _)| (*p - price).abs() < 0.00001) {
                                ob.asks[pos].1 = size;
                            } else {
                                ob.asks.push((price, size));
                                ob.asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                            }
                        }
                    }
                }
            }
        }

        // Keep only top N levels
        ob.bids.truncate(20);
        ob.asks.truncate(20);

        Ok(())
    }
}
