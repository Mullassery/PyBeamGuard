//! Externalized, YAML-loadable analyzer rule data.
//!
//! `HotKeyAnalyzer`/`CostAnalyzer`/`SparkJoinAnalyzer`/`StateAnalyzer`
//! previously had all of their rule data (risk-pattern keyword lists, cost
//! rates, size/cardinality thresholds) baked in as Rust `const`s -- any new
//! or custom rule required recompiling the whole tool. The rule-evaluation
//! *logic* was already cleanly decoupled from parsing/execution via the
//! `Analyzer` trait and a generic dispatcher; only the rule *data* needed
//! externalizing, which is what this module does.
//!
//! Each `*Rules` struct's `Default` impl reproduces the exact values that
//! used to be hardcoded `const`s, so analyzing without a rules file behaves
//! identically to before this change. A YAML file overrides only the
//! fields it sets -- see `RulesConfig::load_from_str`'s doc comment for the
//! merge behavior.

use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct HotKeyRules {
    /// Key-expression substrings that suggest a high-cardinality-variance
    /// key domain (customer/tenant/org ids, etc.) -- checked as a
    /// case-insensitive substring match.
    pub high_risk_patterns: Vec<String>,
    /// Key-expression substrings that suggest a time-based key domain
    /// (hour/day/timestamp), a weaker skew signal than `high_risk_patterns`.
    pub medium_risk_patterns: Vec<String>,
    /// A measured key cardinality strictly below this is escalated to
    /// `Critical` regardless of the key expression's name.
    pub low_cardinality_threshold: u64,
    /// A measured key cardinality at or above this downgrades a
    /// `high_risk_patterns` match from `High` to `Medium` (traffic is
    /// already spread over many keys).
    pub high_cardinality_downgrade_threshold: u64,
}

impl Default for HotKeyRules {
    fn default() -> Self {
        HotKeyRules {
            high_risk_patterns: [
                "customer",
                "tenant",
                "org",
                "account",
                "user",
                "organization",
                "client",
                "partner",
                "company",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            medium_risk_patterns: ["hour", "day", "date", "month", "timestamp"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            low_cardinality_threshold: 1_000,
            high_cardinality_downgrade_threshold: 100_000,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct CostRules {
    pub dataflow_shuffle_cost_per_gb: f64,
    pub worker_machine_cost_per_hour: f64,
    pub persistent_disk_cost_per_gb_month: f64,
}

impl Default for CostRules {
    fn default() -> Self {
        CostRules {
            dataflow_shuffle_cost_per_gb: 0.30,
            worker_machine_cost_per_hour: 0.35,
            persistent_disk_cost_per_gb_month: 0.04,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SparkJoinRules {
    /// Column-name substrings that suggest a high-cardinality join key,
    /// same matching semantics as `HotKeyRules::high_risk_patterns`.
    pub high_risk_patterns: Vec<String>,
    /// A `autoBroadcastJoinThreshold` above this (bytes) is flagged as
    /// excessive/risky.
    pub broadcast_threshold_high_risk_bytes: i64,
    /// A measured join-key cardinality strictly below this is escalated to
    /// `Critical` regardless of the key expression's name.
    pub low_cardinality_threshold: u64,
    /// A measured join-key cardinality at or above this downgrades a
    /// `high_risk_patterns` match from `High` to `Medium`.
    pub high_cardinality_downgrade_threshold: u64,
}

impl Default for SparkJoinRules {
    fn default() -> Self {
        SparkJoinRules {
            high_risk_patterns: [
                "customer",
                "tenant",
                "org",
                "account",
                "user",
                "organization",
                "client",
                "partner",
                "company",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            broadcast_threshold_high_risk_bytes: 1_073_741_824,
            low_cardinality_threshold: 1_000,
            high_cardinality_downgrade_threshold: 100_000,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct StateRules {
    /// A measured state size at or above this (GB) is flagged as large.
    pub large_measured_state_size_gb: f64,
}

impl Default for StateRules {
    fn default() -> Self {
        StateRules {
            large_measured_state_size_gb: 500.0,
        }
    }
}

/// The full set of externalizable rule data, one field per analyzer.
/// Load with [`RulesConfig::load_from_str`]/[`RulesConfig::load_from_file`],
/// or use [`RulesConfig::default`] for the built-in (pre-this-change)
/// behavior.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct RulesConfig {
    pub hotkey: HotKeyRules,
    pub cost: CostRules,
    pub spark_join: SparkJoinRules,
    pub state: StateRules,
}

impl RulesConfig {
    /// Parse a YAML rules document. Any section/field the document omits
    /// keeps its default value (via `#[serde(default)]` on every field in
    /// this module) -- a file only needs to set what it wants to override,
    /// e.g. just `hotkey.high_risk_patterns` to add a custom keyword
    /// without having to restate cost/spark_join/state.
    pub fn load_from_str(yaml: &str) -> anyhow::Result<Self> {
        let config: RulesConfig = serde_yaml::from_str(yaml)?;
        Ok(config)
    }

    pub fn load_from_file(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read rules file {}: {e}", path.display()))?;
        Self::load_from_str(&content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_matches_previously_hardcoded_values() {
        let rules = RulesConfig::default();
        assert!(rules
            .hotkey
            .high_risk_patterns
            .contains(&"customer".to_string()));
        assert_eq!(rules.hotkey.low_cardinality_threshold, 1_000);
        assert_eq!(rules.cost.dataflow_shuffle_cost_per_gb, 0.30);
        assert_eq!(
            rules.spark_join.broadcast_threshold_high_risk_bytes,
            1_073_741_824
        );
        assert_eq!(rules.state.large_measured_state_size_gb, 500.0);
    }

    #[test]
    fn test_partial_override_keeps_other_fields_at_default() {
        let yaml = r#"
hotkey:
  high_risk_patterns:
    - "custom_pattern"
"#;
        let rules = RulesConfig::load_from_str(yaml).unwrap();
        assert_eq!(
            rules.hotkey.high_risk_patterns,
            vec!["custom_pattern".to_string()]
        );
        // Untouched sections/fields keep their defaults.
        assert_eq!(rules.hotkey.low_cardinality_threshold, 1_000);
        assert_eq!(rules.cost, CostRules::default());
        assert_eq!(rules.state, StateRules::default());
    }

    #[test]
    fn test_full_override_all_sections() {
        let yaml = r#"
hotkey:
  high_risk_patterns: ["custom_id"]
  medium_risk_patterns: ["week"]
  low_cardinality_threshold: 50
  high_cardinality_downgrade_threshold: 200000
cost:
  dataflow_shuffle_cost_per_gb: 0.5
  worker_machine_cost_per_hour: 0.6
  persistent_disk_cost_per_gb_month: 0.1
spark_join:
  high_risk_patterns: ["shard_id"]
  broadcast_threshold_high_risk_bytes: 2000000000
state:
  large_measured_state_size_gb: 1000.0
"#;
        let rules = RulesConfig::load_from_str(yaml).unwrap();
        assert_eq!(
            rules.hotkey.high_risk_patterns,
            vec!["custom_id".to_string()]
        );
        assert_eq!(rules.hotkey.low_cardinality_threshold, 50);
        assert_eq!(rules.cost.dataflow_shuffle_cost_per_gb, 0.5);
        assert_eq!(
            rules.spark_join.broadcast_threshold_high_risk_bytes,
            2_000_000_000
        );
        assert_eq!(rules.state.large_measured_state_size_gb, 1000.0);
    }

    #[test]
    fn test_invalid_yaml_is_a_real_error() {
        let result = RulesConfig::load_from_str("not: valid: yaml: [");
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_yaml_is_all_defaults() {
        let rules = RulesConfig::load_from_str("").unwrap();
        assert_eq!(rules, RulesConfig::default());
    }
}
