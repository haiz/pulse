/// Topic-level ACL permissions for a service.
#[derive(Debug, Clone, Default)]
pub struct Permissions {
    pub publish_topics: Vec<String>,
    pub subscribe_topics: Vec<String>,
}

impl Permissions {
    /// Permissions that allow all publish/subscribe operations.
    pub fn allow_all() -> Self {
        Self {
            publish_topics: vec!["*".into(), ">".into()],
            subscribe_topics: vec!["*".into(), ">".into()],
        }
    }

    /// Check if the service can publish to a topic.
    pub fn can_publish(&self, topic: &str) -> bool {
        self.publish_topics
            .iter()
            .any(|p| pattern_matches(p, topic))
    }

    /// Check if the service can subscribe to a topic.
    pub fn can_subscribe(&self, topic: &str) -> bool {
        self.subscribe_topics
            .iter()
            .any(|p| pattern_matches(p, topic))
    }
}

/// Check if a pattern matches a topic.
/// Supports exact match, `*` (single segment), and `>` (multi-segment).
fn pattern_matches(pattern: &str, topic: &str) -> bool {
    if pattern == ">" || pattern == "*" {
        // Global wildcard
        return if pattern == ">" {
            true
        } else {
            !topic.contains('.')
        };
    }

    let pat_segments: Vec<&str> = pattern.split('.').collect();
    let top_segments: Vec<&str> = topic.split('.').collect();

    let mut pi = 0;
    let mut ti = 0;

    while pi < pat_segments.len() && ti < top_segments.len() {
        match pat_segments[pi] {
            ">" => return true, // matches everything remaining
            "*" => {
                // matches exactly one segment
                pi += 1;
                ti += 1;
            }
            exact => {
                if exact != top_segments[ti] {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
        }
    }

    pi == pat_segments.len() && ti == top_segments.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match() {
        assert!(pattern_matches("order.created", "order.created"));
        assert!(!pattern_matches("order.created", "order.updated"));
    }

    #[test]
    fn single_wildcard() {
        assert!(pattern_matches("order.*", "order.created"));
        assert!(pattern_matches("order.*", "order.updated"));
        assert!(!pattern_matches("order.*", "order.us.created"));
    }

    #[test]
    fn multi_wildcard() {
        assert!(pattern_matches("order.>", "order.created"));
        assert!(pattern_matches("order.>", "order.us.created"));
        assert!(!pattern_matches("order.>", "payment.created"));
    }

    #[test]
    fn global_wildcards() {
        assert!(pattern_matches(">", "anything"));
        assert!(pattern_matches(">", "any.thing.at.all"));
        assert!(pattern_matches("*", "single"));
        assert!(!pattern_matches("*", "two.segments"));
    }

    #[test]
    fn permissions_allow_all() {
        let perms = Permissions::allow_all();
        assert!(perms.can_publish("any.topic"));
        assert!(perms.can_subscribe("any.topic"));
    }

    #[test]
    fn permissions_restricted() {
        let perms = Permissions {
            publish_topics: vec!["order.*".into()],
            subscribe_topics: vec!["payment.*".into()],
        };
        assert!(perms.can_publish("order.created"));
        assert!(!perms.can_publish("payment.completed"));
        assert!(perms.can_subscribe("payment.completed"));
        assert!(!perms.can_subscribe("order.created"));
    }
}
