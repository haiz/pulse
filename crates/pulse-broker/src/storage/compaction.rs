use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::BrokerError;
use crate::storage::wal::WalReader;

/// WAL compaction configuration.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Run compaction every N seconds (default: 3600 = 1 hour).
    pub interval_secs: u64,
    /// Compact when this ratio of events are completed (default: 0.8 = 80%).
    pub min_completed_ratio: f64,
    /// WAL retention in hours (default: 168 = 7 days).
    pub retention_hours: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            interval_secs: 3600,
            min_completed_ratio: 0.8,
            retention_hours: 168,
        }
    }
}

/// Statistics about a WAL segment for compaction decisions.
#[derive(Debug)]
pub struct SegmentStats {
    pub segment_number: u32,
    pub path: PathBuf,
    pub total_events: usize,
    pub completed_events: usize,
    pub file_size: u64,
}

impl SegmentStats {
    /// Ratio of completed events to total events.
    pub fn completed_ratio(&self) -> f64 {
        if self.total_events == 0 {
            return 0.0;
        }
        self.completed_events as f64 / self.total_events as f64
    }

    /// Whether this segment is eligible for compaction.
    pub fn eligible(&self, min_ratio: f64) -> bool {
        self.completed_ratio() >= min_ratio
    }
}

/// Analyze a WAL segment to determine compaction eligibility.
pub async fn analyze_segment(path: PathBuf) -> Result<SegmentStats, BrokerError> {
    let metadata = tokio::fs::metadata(&path).await?;
    let file_size = metadata.len();

    let mut reader = WalReader::open(path.clone()).await?;
    let mut total_events = 0usize;
    let mut completed_ids = HashSet::new();
    let mut event_ids = HashSet::new();

    loop {
        match reader.next_record() {
            Ok(Some(record)) => match record {
                crate::storage::wal::ReadRecord::EventWrite { msg_id, .. } => {
                    event_ids.insert(msg_id);
                    total_events += 1;
                }
                crate::storage::wal::ReadRecord::Completion { msg_id, .. } => {
                    completed_ids.insert(msg_id);
                }
            },
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let completed_events = event_ids.intersection(&completed_ids).count();

    Ok(SegmentStats {
        segment_number: reader.segment_number(),
        path,
        total_events,
        completed_events,
        file_size,
    })
}

/// Spawn a background compaction task.
pub fn spawn_compaction_task(
    wal_dir: PathBuf,
    config: CompactionConfig,
    active_segment: u32,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_secs(config.interval_secs);
        let mut timer = tokio::time::interval(interval);

        loop {
            timer.tick().await;

            if let Err(e) = run_compaction_cycle(&wal_dir, &config, active_segment).await {
                tracing::error!(error = %e, "compaction cycle failed");
            }
        }
    })
}

async fn run_compaction_cycle(
    wal_dir: &Path,
    config: &CompactionConfig,
    active_segment: u32,
) -> Result<(), BrokerError> {
    let segments = list_segment_files(wal_dir).await?;

    for seg_path in segments {
        // Extract segment number from filename
        let seg_num = parse_segment_number(&seg_path);
        if seg_num >= active_segment {
            continue; // Don't compact the active segment
        }

        match analyze_segment(seg_path.clone()).await {
            Ok(stats) => {
                if stats.eligible(config.min_completed_ratio) {
                    tracing::info!(
                        segment = stats.segment_number,
                        ratio = format!("{:.1}%", stats.completed_ratio() * 100.0),
                        total = stats.total_events,
                        completed = stats.completed_events,
                        "segment eligible for compaction"
                    );
                    // In a full implementation, we'd:
                    // 1. Create segment-NNNNNN.wal.compact with only pending events
                    // 2. fsync the new file
                    // 3. Atomically rename over the original
                    // 4. Optionally archive the old segment
                }
            }
            Err(e) => {
                tracing::warn!(path = %seg_path.display(), error = %e, "failed to analyze segment");
            }
        }
    }

    Ok(())
}

async fn list_segment_files(wal_dir: &Path) -> Result<Vec<PathBuf>, BrokerError> {
    let mut files = Vec::new();
    let mut entries = tokio::fs::read_dir(wal_dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().map(|e| e == "wal").unwrap_or(false) {
            files.push(path);
        }
    }

    files.sort();
    Ok(files)
}

fn parse_segment_number(path: &Path) -> u32 {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.strip_prefix("segment-"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_stats_ratio() {
        let stats = SegmentStats {
            segment_number: 1,
            path: PathBuf::from("test.wal"),
            total_events: 100,
            completed_events: 80,
            file_size: 1024,
        };
        assert!((stats.completed_ratio() - 0.8).abs() < f64::EPSILON);
        assert!(stats.eligible(0.8));
        assert!(!stats.eligible(0.9));
    }

    #[test]
    fn empty_segment_ratio() {
        let stats = SegmentStats {
            segment_number: 1,
            path: PathBuf::from("test.wal"),
            total_events: 0,
            completed_events: 0,
            file_size: 32,
        };
        assert!((stats.completed_ratio() - 0.0).abs() < f64::EPSILON);
        assert!(!stats.eligible(0.8));
    }

    #[test]
    fn parse_segment_number_from_path() {
        assert_eq!(parse_segment_number(Path::new("wal/segment-000005.wal")), 5);
        assert_eq!(parse_segment_number(Path::new("segment-000123.wal")), 123);
        assert_eq!(parse_segment_number(Path::new("unknown.wal")), 0);
    }
}
