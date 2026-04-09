/// Route configuration loaded from routes.yaml.
/// Stub implementation for Phase 4. Full hot-reload support will be added later.
use serde::Deserialize;

/// Top-level routing configuration.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RoutingConfig {
    #[serde(default)]
    pub routes: Vec<RouteRule>,
}

/// A single route rule.
#[derive(Debug, Clone, Deserialize)]
pub struct RouteRule {
    pub name: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(rename = "match")]
    pub match_config: RouteMatch,
    #[serde(default)]
    pub deliver: Vec<DeliverTarget>,
}

fn default_true() -> bool {
    true
}

/// Match criteria for a route.
#[derive(Debug, Clone, Deserialize)]
pub struct RouteMatch {
    pub topic: String,
    #[serde(rename = "where")]
    pub filter: Option<String>,
}

/// A delivery target in a route rule.
#[derive(Debug, Clone, Deserialize)]
pub struct DeliverTarget {
    pub service: Option<String>,
    pub group: Option<String>,
}
