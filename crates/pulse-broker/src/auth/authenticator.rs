use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::auth::permissions::Permissions;

/// A service's credentials and permissions.
#[derive(Debug, Clone)]
pub struct ServiceCredential {
    pub service_id: String,
    pub namespace: String,
    pub api_key: String,
    pub permissions: Permissions,
}

/// Authentication result.
#[derive(Debug)]
pub enum AuthResult {
    Ok {
        service_id: String,
        namespace: String,
        permissions: Permissions,
    },
    Rejected(String),
}

/// Authenticator that validates CONNECT frame credentials.
pub struct Authenticator {
    /// Map: namespace -> service_id -> credential
    credentials: ArcSwap<HashMap<String, HashMap<String, ServiceCredential>>>,
    /// If true, accept all connections without checking credentials (dev mode).
    allow_anonymous: bool,
}

impl Authenticator {
    /// Create an authenticator that accepts all connections (zero-config mode).
    pub fn anonymous() -> Self {
        Self {
            credentials: ArcSwap::new(Arc::new(HashMap::new())),
            allow_anonymous: true,
        }
    }

    /// Create an authenticator with loaded credentials.
    pub fn new(credentials: HashMap<String, HashMap<String, ServiceCredential>>) -> Self {
        Self {
            credentials: ArcSwap::new(Arc::new(credentials)),
            allow_anonymous: false,
        }
    }

    /// Authenticate a CONNECT request.
    pub fn authenticate(&self, service_id: &str, namespace: &str, api_key: &str) -> AuthResult {
        if self.allow_anonymous {
            return AuthResult::Ok {
                service_id: service_id.to_string(),
                namespace: namespace.to_string(),
                permissions: Permissions::allow_all(),
            };
        }

        let creds = self.credentials.load();
        let ns_creds = match creds.get(namespace) {
            Some(ns) => ns,
            None => {
                return AuthResult::Rejected(format!("namespace not found: {namespace}"));
            }
        };

        let svc_cred = match ns_creds.get(service_id) {
            Some(cred) => cred,
            None => {
                return AuthResult::Rejected(format!(
                    "service not found: {service_id} in namespace {namespace}"
                ));
            }
        };

        if svc_cred.api_key != api_key {
            return AuthResult::Rejected("invalid API key".into());
        }

        AuthResult::Ok {
            service_id: service_id.to_string(),
            namespace: namespace.to_string(),
            permissions: svc_cred.permissions.clone(),
        }
    }

    /// Hot-reload credentials (e.g., from services.yaml).
    pub fn reload(&self, credentials: HashMap<String, HashMap<String, ServiceCredential>>) {
        self.credentials.store(Arc::new(credentials));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_credentials() -> HashMap<String, HashMap<String, ServiceCredential>> {
        let mut ns = HashMap::new();
        let mut services = HashMap::new();
        services.insert(
            "order-svc".into(),
            ServiceCredential {
                service_id: "order-svc".into(),
                namespace: "ecommerce".into(),
                api_key: "psk_test_abc123".into(),
                permissions: Permissions {
                    publish_topics: vec!["order.*".into()],
                    subscribe_topics: vec!["payment.*".into()],
                },
            },
        );
        ns.insert("ecommerce".into(), services);
        ns
    }

    #[test]
    fn anonymous_mode_accepts_all() {
        let auth = Authenticator::anonymous();
        let result = auth.authenticate("any-service", "any-ns", "");
        assert!(matches!(result, AuthResult::Ok { .. }));
    }

    #[test]
    fn valid_credentials() {
        let auth = Authenticator::new(test_credentials());
        let result = auth.authenticate("order-svc", "ecommerce", "psk_test_abc123");
        assert!(matches!(result, AuthResult::Ok { .. }));
    }

    #[test]
    fn invalid_key_rejected() {
        let auth = Authenticator::new(test_credentials());
        let result = auth.authenticate("order-svc", "ecommerce", "wrong-key");
        assert!(matches!(result, AuthResult::Rejected(_)));
    }

    #[test]
    fn unknown_namespace_rejected() {
        let auth = Authenticator::new(test_credentials());
        let result = auth.authenticate("order-svc", "unknown", "psk_test_abc123");
        assert!(matches!(result, AuthResult::Rejected(_)));
    }

    #[test]
    fn unknown_service_rejected() {
        let auth = Authenticator::new(test_credentials());
        let result = auth.authenticate("unknown-svc", "ecommerce", "psk_test_abc123");
        assert!(matches!(result, AuthResult::Rejected(_)));
    }

    #[test]
    fn hot_reload_credentials() {
        let auth = Authenticator::new(HashMap::new());
        // Initially fails
        let r1 = auth.authenticate("order-svc", "ecommerce", "psk_test_abc123");
        assert!(matches!(r1, AuthResult::Rejected(_)));

        // Reload with credentials
        auth.reload(test_credentials());

        // Now succeeds
        let r2 = auth.authenticate("order-svc", "ecommerce", "psk_test_abc123");
        assert!(matches!(r2, AuthResult::Ok { .. }));
    }
}
