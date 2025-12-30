#![allow(dead_code)]
#![allow(unused)]
//! KuCoin Exchange Layer
//!
//! Provides REST and WebSocket clients for KuCoin trading.

pub mod traits;
pub mod order_book;
pub mod order_template;
pub use order_template::OrderTemplate;
// traits module available as exchange::traits::*
pub mod auth;
pub mod rest;
pub mod types;
pub mod order_state;
pub mod order_state_machine;
pub mod kucoin_ws_private;
pub mod ws_order_client;

pub use auth::KucoinAuth;
pub use rest::KucoinRestClient;
pub use types::*;
pub use order_state::{Side as OrderSide, new_shared_order_manager};
pub use kucoin_ws_private::{KucoinPrivateWs, ConnectionState};

pub mod ws_order_client_v2;
pub use ws_order_client_v2::{WsOrderClientV2, WsOrderRequest, WsCancelRequest};
