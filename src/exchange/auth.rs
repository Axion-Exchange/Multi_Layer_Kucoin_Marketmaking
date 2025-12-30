//! KuCoin API Authentication
//!
//! Implements HMAC-SHA256 signing for REST and WebSocket API calls.
//! Supports both v1 (plain passphrase) and v2 (HMAC'd passphrase) auth.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// KuCoin authentication handler
#[derive(Clone)]
pub struct KucoinAuth {
    api_key: String,
    api_secret: String,
    passphrase: String,
    use_v2: bool,
}

impl KucoinAuth {
    pub fn new(api_key: String, api_secret: String, passphrase: String, use_v2: bool) -> Self {
        Self {
            api_key,
            api_secret,
            passphrase,
            use_v2,
        }
    }

    /// Get API key
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Get API secret
    pub fn api_secret(&self) -> &str {
        &self.api_secret
    }

    /// Sign a request and return headers
    ///
    /// Returns: (timestamp, signature, passphrase, key_version)
    pub fn sign(&self, method: &str, path: &str, body: &str) -> (String, String, String, String) {
        let timestamp = Self::timestamp_ms();
        
        // Create the string to sign: timestamp + method + path + body
        let str_to_sign = format!("{}{}{}{}", timestamp, method.to_uppercase(), path, body);
        
        // Sign with secret
        let signature = self.hmac_sign(&self.api_secret, &str_to_sign);

        // Get passphrase (v2 signs it, v1 uses plain)
        let passphrase = if self.use_v2 {
            self.hmac_sign(&self.api_secret, &self.passphrase)
        } else {
            self.passphrase.clone()
        };

        let version = if self.use_v2 { "2" } else { "1" };

        (timestamp, signature, passphrase, version.to_string())
    }

    /// Create HMAC-SHA256 signature with base64 encoding
    fn hmac_sign(&self, secret: &str, message: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .unwrap_or_else(|_| panic!("HMAC initialization failed - invalid key"));
        mac.update(message.as_bytes());
        let result = mac.finalize();
        BASE64.encode(result.into_bytes())
    }

    /// Get current timestamp in milliseconds
    fn timestamp_ms() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        duration.as_millis().to_string()
    }

    /// Sign for WebSocket connection
    ///
    /// Returns: (timestamp, signature, passphrase)
    pub fn sign_ws(&self) -> (String, String, String) {
        let timestamp = Self::timestamp_ms();
        
        // For WS, we sign: timestamp + "GET" + "/api/v1/bullet-private"
        let str_to_sign = format!("{}GET/api/v1/bullet-private", timestamp);
        let signature = self.hmac_sign(&self.api_secret, &str_to_sign);

        let passphrase = if self.use_v2 {
            self.hmac_sign(&self.api_secret, &self.passphrase)
        } else {
            self.passphrase.clone()
        };

        (timestamp, signature, passphrase)
    }

    /// Sign for WebSocket URL-based authentication
    /// For wss://wsapi.kucoin.com/v1/private?apikey=XXX&sign=XXX&passphrase=XXX&timestamp=XXX
    /// Returns: (timestamp, signature, passphrase)
    pub fn sign_ws_url(&self) -> (String, String, String) {
        let timestamp = Self::timestamp_ms();
        // Per tiagosiebler/kucoin-api (working implementation):
        // sign = HMAC-SHA256(apikey + timestamp)
        // passphrase = HMAC-SHA256(passphrase) ALWAYS
        let str_to_sign = format!("{}{}", self.api_key, timestamp);
        let signature = self.hmac_sign(&self.api_secret, &str_to_sign);
        // Note: for WS API, passphrase is ALWAYS signed (unlike REST which checks v2)
        let passphrase = self.hmac_sign(&self.api_secret, &self.passphrase);
        (timestamp, signature, passphrase)
    }
}

impl std::fmt::Debug for KucoinAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KucoinAuth")
            .field("api_key", &format!("{}...", &self.api_key[..8.min(self.api_key.len())]))
            .field("use_v2", &self.use_v2)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign() {
        let auth = KucoinAuth::new(
            "test_key".to_string(),
            "test_secret".to_string(),
            "test_pass".to_string(),
            true,
        );
        
        let (ts, sig, pass, ver) = auth.sign("POST", "/api/v1/orders", r#"{"symbol":"LTC-USDT"}"#);
        
        assert!(!ts.is_empty());
        assert!(!sig.is_empty());
        assert!(!pass.is_empty());
        assert_eq!(ver, "2");
    }
}
