//! KuCoin WebSocket Order Execution
//!
//! Ultra-low latency order placement via WebSocket.
//! This is the HF (High-Frequency) channel for market making.

use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn, error, debug};
use anyhow::Result;

use super::auth::KucoinAuth;
use super::types::{OrderRequest, Side};

/// Command to send to the order WebSocket
#[derive(Debug)]
pub enum OrderCmd {
    Place(OrderRequest),
    Cancel { order_id: String },
    CancelByClientOid { client_oid: String },
}

/// Response from order operations
#[derive(Debug, Clone)]
pub enum OrderEvent {
    Placed { order_id: String, client_oid: String },
    Cancelled { order_id: String },
    Filled { order_id: String, price: f64, size: f64, side: Side },
    Error { client_oid: String, message: String },
}

/// KuCoin WebSocket order channel for HF trading
pub struct KucoinOrderWs {
    ws_url: String,
    auth: KucoinAuth,
    cmd_tx: mpsc::Sender<OrderCmd>,
    cmd_rx: Arc<RwLock<Option<mpsc::Receiver<OrderCmd>>>>,
    event_tx: mpsc::Sender<OrderEvent>,
}

impl KucoinOrderWs {
    /// Create new order WebSocket
    pub fn new(
        ws_url: String,
        auth: KucoinAuth,
        event_tx: mpsc::Sender<OrderEvent>,
    ) -> (Self, mpsc::Sender<OrderCmd>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(1000);
        
        let ws = Self {
            ws_url,
            auth,
            cmd_tx: cmd_tx.clone(),
            cmd_rx: Arc::new(RwLock::new(Some(cmd_rx))),
            event_tx,
        };
        
        (ws, cmd_tx)
    }

    /// Get the command sender for placing orders
    pub fn cmd_sender(&self) -> mpsc::Sender<OrderCmd> {
        self.cmd_tx.clone()
    }

    /// Start the WebSocket connection
    pub async fn start(&self, token: &str) -> Result<tokio::task::JoinHandle<()>> {
        // Take the receiver out
        let cmd_rx = self.cmd_rx.write().await.take()
            .ok_or_else(|| anyhow::anyhow!("Receiver already taken"))?;

        let url = format!(
            "{}?token={}&connectId={}",
            self.ws_url,
            token,
            uuid::Uuid::new_v4()
        );
        
        let event_tx = self.event_tx.clone();
        let auth = self.auth.clone();
        let ws_url = self.ws_url.clone();

        info!("[KC-WS-ORD] Connecting to {} for HF orders", ws_url);

        let handle = tokio::spawn(async move {
            Self::run_loop(&url, cmd_rx, event_tx, auth).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        Ok(handle)
    }

    async fn run_loop(
        url: &str,
        mut cmd_rx: mpsc::Receiver<OrderCmd>,
        event_tx: mpsc::Sender<OrderEvent>,
        _auth: KucoinAuth,
    ) {
        loop {
            match Self::run_connection(url, &mut cmd_rx, &event_tx).await {
                Ok(_) => {
                    warn!("[KC-WS-ORD] Connection closed, reconnecting in 1s...");
                }
                Err(e) => {
                    error!("[KC-WS-ORD] Connection error: {:?}, reconnecting in 1s...", e);
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    async fn run_connection(
        url: &str,
        cmd_rx: &mut mpsc::Receiver<OrderCmd>,
        event_tx: &mpsc::Sender<OrderEvent>,
    ) -> Result<()> {
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        info!("[KC-WS-ORD] Connected successfully for HF orders");

        // Ping interval
        let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

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
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(OrderCmd::Place(order)) => {
                            let msg = Self::build_place_message(&order);
                            debug!("[KC-WS-ORD] Placing: {} {} @ {}", 
                                order.side.as_str(), order.size, order.price);
                            if write.send(Message::Text(msg)).await.is_err() {
                                break;
                            }
                        }
                        Some(OrderCmd::Cancel { order_id }) => {
                            let msg = Self::build_cancel_message(&order_id);
                            debug!("[KC-WS-ORD] Cancelling: {}", order_id);
                            if write.send(Message::Text(msg)).await.is_err() {
                                break;
                            }
                        }
                        Some(OrderCmd::CancelByClientOid { client_oid }) => {
                            let msg = Self::build_cancel_by_client_oid(&client_oid);
                            debug!("[KC-WS-ORD] Cancelling by clientOid: {}", client_oid);
                            if write.send(Message::Text(msg)).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(e) = Self::handle_message(&text, event_tx).await {
                                debug!("[KC-WS-ORD] Parse error: {:?}", e);
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            warn!("[KC-WS-ORD] Server closed connection");
                            break;
                        }
                        Some(Err(e)) => {
                            error!("[KC-WS-ORD] WebSocket error: {:?}", e);
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

    fn build_place_message(order: &OrderRequest) -> String {
        let msg = serde_json::json!({
            "id": uuid::Uuid::new_v4().to_string(),
            "type": "request",
            "topic": "/spotMarket/tradeOrders",
            "privateChannel": true,
            "data": {
                "type": "limit",
                "symbol": order.symbol,
                "side": order.side.as_str(),
                "price": order.price,
                "size": order.size,
                "clientOid": order.client_oid,
                "postOnly": order.post_only.unwrap_or(true)
            }
        });
        msg.to_string()
    }

    fn build_cancel_message(order_id: &str) -> String {
        let msg = serde_json::json!({
            "id": uuid::Uuid::new_v4().to_string(),
            "type": "request",
            "topic": "/spotMarket/cancelOrder",
            "privateChannel": true,
            "data": {
                "orderId": order_id
            }
        });
        msg.to_string()
    }

    fn build_cancel_by_client_oid(client_oid: &str) -> String {
        let msg = serde_json::json!({
            "id": uuid::Uuid::new_v4().to_string(),
            "type": "request",
            "topic": "/spotMarket/cancelOrderByClientOid",
            "privateChannel": true,
            "data": {
                "clientOid": client_oid
            }
        });
        msg.to_string()
    }

    async fn handle_message(
        text: &str,
        event_tx: &mpsc::Sender<OrderEvent>,
    ) -> Result<()> {
        let v: serde_json::Value = serde_json::from_str(text)?;
        
        // Check message type
        let msg_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
        
        match msg_type {
            "message" => {
                // Order update message
                let subject = v.get("subject").and_then(|s| s.as_str()).unwrap_or("");
                let data = v.get("data");

                match subject {
                    "orderOpen" => {
                        if let Some(data) = data {
                            let order_id = data.get("orderId").and_then(|o| o.as_str()).unwrap_or("");
                            let client_oid = data.get("clientOid").and_then(|c| c.as_str()).unwrap_or("");
                            
                            let _ = event_tx.send(OrderEvent::Placed {
                                order_id: order_id.to_string(),
                                client_oid: client_oid.to_string(),
                            }).await;
                        }
                    }
                    "orderClosed" | "orderCanceled" => {
                        if let Some(data) = data {
                            let order_id = data.get("orderId").and_then(|o| o.as_str()).unwrap_or("");
                            
                            let _ = event_tx.send(OrderEvent::Cancelled {
                                order_id: order_id.to_string(),
                            }).await;
                        }
                    }
                    "trade" => {
                        if let Some(data) = data {
                            let order_id = data.get("orderId").and_then(|o| o.as_str()).unwrap_or("");
                            let price = data.get("price").and_then(|p| p.as_str()?.parse::<f64>().ok()).unwrap_or(0.0);
                            let size = data.get("size").and_then(|s| s.as_str()?.parse::<f64>().ok()).unwrap_or(0.0);
                            let side_str = data.get("side").and_then(|s| s.as_str()).unwrap_or("buy");
                            let side = if side_str == "sell" { Side::Sell } else { Side::Buy };

                            info!("[KC-WS-ORD] FILL: {} {} @ {} ({})", 
                                side_str, size, price, order_id);
                            
                            let _ = event_tx.send(OrderEvent::Filled {
                                order_id: order_id.to_string(),
                                price,
                                size,
                                side,
                            }).await;
                        }
                    }
                    _ => {}
                }
            }
            "error" => {
                let message = v.get("data").and_then(|d| d.as_str()).unwrap_or("Unknown error");
                error!("[KC-WS-ORD] Error: {}", message);
                
                let _ = event_tx.send(OrderEvent::Error {
                    client_oid: String::new(),
                    message: message.to_string(),
                }).await;
            }
            _ => {}
        }

        Ok(())
    }
}
