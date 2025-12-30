//! KuCoin REST API Client

use anyhow::Result;
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::Deserialize;
use tracing::debug;
use std::time::Instant;

use super::auth::KucoinAuth;
use super::types::*;

// ==================== ORDER POLLING RESPONSE TYPES ====================

#[derive(Debug, Deserialize)]
pub struct OrderStatusResponse {
    pub code: String,
    pub data: Option<OrderInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderInfo {
    pub id: String,
    #[serde(rename = "clientOid")]
    pub client_oid: Option<String>,
    pub symbol: String,
    pub side: String,
    pub price: String,
    pub size: String,
    #[serde(rename = "dealSize")]
    pub deal_size: String,
    #[serde(rename = "dealFunds")]
    pub deal_funds: String,
    #[serde(rename = "isActive")]
    pub is_active: bool,
    #[serde(rename = "cancelExist")]
    pub cancel_exist: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FillInfo {
    pub symbol: String,
    #[serde(rename = "tradeId")]
    pub trade_id: String,
    #[serde(rename = "orderId")]
    pub order_id: String,
    pub side: String,
    pub price: String,
    pub size: String,
    pub fee: String,
    #[serde(rename = "feeCurrency")]
    pub fee_currency: String,
    #[serde(rename = "createdAt")]
    pub created_at: u64,
}

#[derive(Debug, Deserialize)]
pub struct FillsResponse {
    pub code: String,
    pub data: Option<FillsData>,
}

#[derive(Debug, Deserialize)]
pub struct FillsData {
    pub items: Vec<FillInfo>,
}

// ==================== REST CLIENT ====================

pub struct KucoinRestClient {
    client: Client,
    base_url: String,
    auth: KucoinAuth,
}

impl KucoinRestClient {
    pub fn new(endpoints: &KucoinEndpoints, auth: KucoinAuth) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .tcp_keepalive(std::time::Duration::from_secs(30))
            .pool_idle_timeout(std::time::Duration::from_secs(60))
            .pool_max_idle_per_host(10)
            .tcp_nodelay(true)  // Disable Nagle's algorithm for lower latency
            .build()?;
        Ok(Self { client, base_url: endpoints.rest_url.clone(), auth })
    }

    fn build_headers(&self, method: &str, endpoint: &str, body: &str) -> Result<HeaderMap> {
        let (timestamp, sign, passphrase, _) = self.auth.sign(method, endpoint, body);
        let mut headers = HeaderMap::new();
        headers.insert("KC-API-KEY", HeaderValue::from_str(self.auth.api_key())?);
        headers.insert("KC-API-SIGN", HeaderValue::from_str(&sign)?);
        headers.insert("KC-API-TIMESTAMP", HeaderValue::from_str(&timestamp)?);
        headers.insert("KC-API-PASSPHRASE", HeaderValue::from_str(&passphrase)?);
        headers.insert("KC-API-KEY-VERSION", HeaderValue::from_static("2"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(headers)
    }

    /// Place a new order
    pub async fn place_order(&self, order: &OrderRequest) -> Result<String> {
        let _start = Instant::now();
        let endpoint = "/api/v1/hf/orders";
        let body = serde_json::to_string(order)?;
        let headers = self.build_headers("POST", endpoint, &body)?;
        
        debug!("[REST] POST {} | {}", endpoint, body);
        
        let resp = self.client
            .post(&format!("{}{}", self.base_url, endpoint))
            .headers(headers)
            .body(body)
            .send()
            .await?;
        
        let status = resp.status();
        let body = resp.text().await?;
        
        debug!("[REST] Response: {} | {}", status, body);
        
        #[derive(Deserialize)]
        struct PlaceOrderResponse {
            code: String,
            data: Option<PlaceOrderData>,
            msg: Option<String>,
        }
        
        #[derive(Deserialize)]
        struct PlaceOrderData {
            #[serde(rename = "orderId")]
            order_id: String,
        }
        
        let parsed: PlaceOrderResponse = serde_json::from_str(&body)?;
        
        if parsed.code == "200000" {
            if let Some(data) = parsed.data {
                return Ok(data.order_id);
            }
        }
        
        anyhow::bail!("Order failed: {} - {}", parsed.code, parsed.msg.unwrap_or_default())
    }

    /// Cancel an order by ID
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let _start = Instant::now();
        let endpoint = format!("/api/v1/hf/orders/{}", order_id);
        let headers = self.build_headers("DELETE", &endpoint, "")?;
        
        let resp = self.client
            .delete(&format!("{}{}", self.base_url, endpoint))
            .headers(headers)
            .send()
            .await?;
        
        let _status = resp.status();
        let _body = resp.text().await?;
        
        Ok(())
    }

    /// Cancel an order by clientOid (different KuCoin endpoint)
    pub async fn cancel_by_client_oid(&self, symbol: &str, client_oid: &str) -> Result<()> {
        let endpoint = format!("/api/v1/hf/orders/client-order/{}?symbol={}", client_oid, symbol);
        let headers = self.build_headers("DELETE", &endpoint, "")?;
        
        let resp = self.client
            .delete(&format!("{}{}", self.base_url, endpoint))
            .headers(headers)
            .send()
            .await?;
        
        let status = resp.status();
        let body = resp.text().await?;
        debug!("[REST] Cancel by clientOid: {} {} -> {}", client_oid, status, body);
        
        Ok(())
    }

    /// Smart cancel - detects if ID is orderId or clientOid and uses correct endpoint
    pub async fn smart_cancel(&self, symbol: &str, id: &str) -> Result<()> {
        if id.starts_with("bid_") || id.starts_with("ask_") {
            // This is a clientOid
            self.cancel_by_client_oid(symbol, id).await
        } else {
            // This is an orderId
            self.cancel_order(id).await
        }
    }

    /// Get order status by order ID
    pub async fn get_order(&self, order_id: &str) -> Result<Option<OrderInfo>> {
        let endpoint = format!("/api/v1/hf/orders/{}", order_id);
        let headers = self.build_headers("GET", &endpoint, "")?;
        
        let resp = self.client
            .get(&format!("{}{}", self.base_url, endpoint))
            .headers(headers)
            .send()
            .await?;
        
        let body = resp.text().await?;
        let parsed: OrderStatusResponse = serde_json::from_str(&body)?;
        
        if parsed.code == "200000" {
            Ok(parsed.data)
        } else {
            Ok(None)
        }
    }

    /// Get recent fills for symbol
    pub async fn get_fills(&self, symbol: &str, limit: u32) -> Result<Vec<FillInfo>> {
        let endpoint = format!("/api/v1/fills?symbol={}&pageSize={}", symbol, limit);
        let headers = self.build_headers("GET", &endpoint, "")?;
        
        let resp = self.client
            .get(&format!("{}{}", self.base_url, endpoint))
            .headers(headers)
            .send()
            .await?;
        
        let body = resp.text().await?;
        let parsed: FillsResponse = serde_json::from_str(&body)?;
        
        if parsed.code == "200000" {
            Ok(parsed.data.map(|d| d.items).unwrap_or_default())
        } else {
            Ok(vec![])
        }
    }

    /// Cancel all orders for symbol
    pub async fn cancel_all_orders(&self, symbol: &str) -> Result<u32> {
        let endpoint = format!("/api/v1/hf/orders?symbol={}", symbol);
        let headers = self.build_headers("DELETE", &endpoint, "")?;
        
        let resp = self.client
            .delete(&format!("{}{}", self.base_url, endpoint))
            .headers(headers)
            .send()
            .await?;
        
        let body = resp.text().await?;
        
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body) {
            if let Some(cancelled) = parsed.get("data").and_then(|d| d.get("cancelledOrderIds")) {
                if let Some(arr) = cancelled.as_array() {
                    return Ok(arr.len() as u32);
                }
            }
        }
        Ok(0)
    }

    /// Get open orders for symbol
    pub async fn get_open_orders(&self, symbol: &str) -> Result<Vec<OrderInfo>> {
        let endpoint = format!("/api/v1/hf/orders?symbol={}&status=active", symbol);
        let headers = self.build_headers("GET", &endpoint, "")?;
        
        let resp = self.client
            .get(&format!("{}{}", self.base_url, endpoint))
            .headers(headers)
            .send()
            .await?;
        
        let body = resp.text().await?;
        
        #[derive(Deserialize)]
        struct OpenOrdersResponse {
            code: String,
            data: Option<OpenOrdersData>,
        }
        
        #[derive(Deserialize)]
        struct OpenOrdersData {
            items: Vec<OrderInfo>,
        }
        
        let parsed: OpenOrdersResponse = serde_json::from_str(&body)?;
        
        if parsed.code == "200000" {
            Ok(parsed.data.map(|d| d.items).unwrap_or_default())
        } else {
            Ok(vec![])
        }
    }
}
