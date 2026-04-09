use dashmap::DashMap;

/// Metadata about a namespace.
#[derive(Debug, Clone)]
pub struct NamespaceInfo {
    pub name: String,
    pub service_count: usize,
    pub topic_count: usize,
    pub created_at: u64,
}

/// Registry for managing namespace isolation.
///
/// Namespaces are fully isolated: topics, services, routing rules, and quotas
/// in one namespace cannot interact with another.
pub struct NamespaceRegistry {
    namespaces: DashMap<String, NamespaceInfo>,
}

impl NamespaceRegistry {
    pub fn new() -> Self {
        Self {
            namespaces: DashMap::new(),
        }
    }

    /// Get or create a namespace. Returns true if newly created.
    pub fn ensure(&self, name: &str) -> bool {
        if self.namespaces.contains_key(name) {
            return false;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.namespaces.insert(
            name.to_string(),
            NamespaceInfo {
                name: name.to_string(),
                service_count: 0,
                topic_count: 0,
                created_at: now,
            },
        );
        true
    }

    /// Check if a namespace exists.
    pub fn exists(&self, name: &str) -> bool {
        self.namespaces.contains_key(name)
    }

    /// Get namespace info.
    pub fn get(&self, name: &str) -> Option<NamespaceInfo> {
        self.namespaces.get(name).map(|r| r.value().clone())
    }

    /// List all namespace names.
    pub fn list(&self) -> Vec<String> {
        self.namespaces.iter().map(|r| r.key().clone()).collect()
    }

    /// Increment service count for a namespace.
    pub fn add_service(&self, namespace: &str) {
        if let Some(mut ns) = self.namespaces.get_mut(namespace) {
            ns.service_count += 1;
        }
    }

    /// Decrement service count for a namespace.
    pub fn remove_service(&self, namespace: &str) {
        if let Some(mut ns) = self.namespaces.get_mut(namespace) {
            ns.service_count = ns.service_count.saturating_sub(1);
        }
    }

    /// Number of registered namespaces.
    pub fn count(&self) -> usize {
        self.namespaces.len()
    }
}

impl Default for NamespaceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_creates_namespace() {
        let reg = NamespaceRegistry::new();
        assert!(reg.ensure("ecommerce"));
        assert!(!reg.ensure("ecommerce")); // already exists
        assert_eq!(reg.count(), 1);
    }

    #[test]
    fn exists_check() {
        let reg = NamespaceRegistry::new();
        assert!(!reg.exists("test"));
        reg.ensure("test");
        assert!(reg.exists("test"));
    }

    #[test]
    fn service_counting() {
        let reg = NamespaceRegistry::new();
        reg.ensure("ns");
        reg.add_service("ns");
        reg.add_service("ns");
        assert_eq!(reg.get("ns").unwrap().service_count, 2);

        reg.remove_service("ns");
        assert_eq!(reg.get("ns").unwrap().service_count, 1);
    }

    #[test]
    fn list_namespaces() {
        let reg = NamespaceRegistry::new();
        reg.ensure("a");
        reg.ensure("b");
        reg.ensure("c");

        let mut names = reg.list();
        names.sort();
        assert_eq!(names, vec!["a", "b", "c"]);
    }
}
