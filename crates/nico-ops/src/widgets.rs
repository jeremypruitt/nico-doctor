//! Pure helpers for the trend widgets — per-scorecard sparkline and
//! header verdict breadcrumb. Both consume the ring buffer and stay free
//! of `ratatui` types so they can be unit-tested without a `TestBackend`.

use nico_common::output::Status;

use crate::ringbuffer::{RingBuffer, RunSnapshot};

/// Worst-case verdict over the layers in a single completed run, mirroring
/// `model::overall_verdict` (`Fail` > `Warn` > `Unknown` > `Ok`).
pub fn run_verdict(run: &RunSnapshot) -> Status {
    if run.layers.iter().any(|l| l.status == Status::Fail) {
        Status::Fail
    } else if run.layers.iter().any(|l| l.status == Status::Warn) {
        Status::Warn
    } else if run.layers.iter().any(|l| l.status == Status::Unknown) {
        Status::Unknown
    } else {
        Status::Ok
    }
}

/// Last `n` overall verdicts from the ring buffer, oldest → newest. When
/// the ring holds fewer than `n` runs, returns whatever is there. Empty
/// ring → empty vec (the renderer treats that as a no-op).
pub fn breadcrumb_verdicts(history: &RingBuffer, n: usize) -> Vec<Status> {
    if n == 0 || history.is_empty() {
        return Vec::new();
    }
    let total = history.len();
    let skip = total.saturating_sub(n);
    history.iter().skip(skip).map(run_verdict).collect()
}

/// Block-glyph sparkline for one Layer's finding-count history. Returns
/// the empty string when fewer than two snapshots exist (acceptance
/// criterion: blank with no jitter for `<2` entries) or when the layer is
/// absent from every snapshot. Width-bounded: only the most recent
/// `max_width` data points are rendered.
pub fn sparkline_for_layer(
    history: &RingBuffer,
    layer_name: &str,
    max_width: usize,
) -> String {
    if history.len() < 2 || max_width == 0 {
        return String::new();
    }
    let counts: Vec<usize> = history
        .iter()
        .filter_map(|run| {
            run.layers
                .iter()
                .find(|l| l.name == layer_name)
                .map(|l| l.finding_count)
        })
        .collect();
    if counts.len() < 2 {
        return String::new();
    }
    let skip = counts.len().saturating_sub(max_width);
    let window: &[usize] = &counts[skip..];
    let max = *window.iter().max().unwrap_or(&0);
    window.iter().map(|c| spark_glyph(*c, max)).collect()
}

/// Eight-level block-glyph ramp. `▁` is the lowest non-zero rung so a flat
/// run of zeros stays visibly distinct from an empty buffer (which renders
/// as the empty string).
const SPARK_GLYPHS: &[char] = &['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

fn spark_glyph(value: usize, max: usize) -> char {
    if max == 0 {
        return SPARK_GLYPHS[0];
    }
    let idx = (value * (SPARK_GLYPHS.len() - 1)) / max;
    SPARK_GLYPHS[idx.min(SPARK_GLYPHS.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ringbuffer::LayerStat;
    use chrono::Local;
    use std::time::Duration;

    fn run(layers: Vec<(&str, Status, usize)>) -> RunSnapshot {
        RunSnapshot {
            timestamp: Local::now(),
            total_duration: Duration::from_millis(0),
            layers: layers
                .into_iter()
                .map(|(n, s, fc)| LayerStat {
                    name: n.into(),
                    status: s,
                    finding_count: fc,
                    duration_ms: 0,
                })
                .collect(),
        }
    }

    // --- run_verdict ---

    #[test]
    fn run_verdict_empty_layers_is_ok() {
        let r = run(vec![]);
        assert_eq!(run_verdict(&r), Status::Ok);
    }

    #[test]
    fn run_verdict_fail_dominates() {
        let r = run(vec![
            ("a", Status::Warn, 0),
            ("b", Status::Fail, 0),
            ("c", Status::Ok, 0),
        ]);
        assert_eq!(run_verdict(&r), Status::Fail);
    }

    #[test]
    fn run_verdict_warn_over_unknown_and_ok() {
        let r = run(vec![
            ("a", Status::Unknown, 0),
            ("b", Status::Warn, 0),
            ("c", Status::Ok, 0),
        ]);
        assert_eq!(run_verdict(&r), Status::Warn);
    }

    #[test]
    fn run_verdict_all_ok_is_ok() {
        let r = run(vec![("a", Status::Ok, 0), ("b", Status::Ok, 0)]);
        assert_eq!(run_verdict(&r), Status::Ok);
    }

    // --- breadcrumb_verdicts ---

    #[test]
    fn breadcrumb_empty_ring_is_empty() {
        let rb = RingBuffer::new();
        assert!(breadcrumb_verdicts(&rb, 10).is_empty());
    }

    #[test]
    fn breadcrumb_returns_one_verdict_per_run() {
        let mut rb = RingBuffer::new();
        rb.push(run(vec![("a", Status::Ok, 0)]));
        rb.push(run(vec![("a", Status::Warn, 1)]));
        rb.push(run(vec![("a", Status::Fail, 2)]));
        let v = breadcrumb_verdicts(&rb, 10);
        assert_eq!(v, vec![Status::Ok, Status::Warn, Status::Fail]);
    }

    #[test]
    fn breadcrumb_caps_to_last_n_when_ring_is_larger() {
        let mut rb = RingBuffer::new();
        for s in [Status::Ok, Status::Warn, Status::Fail, Status::Ok, Status::Ok] {
            rb.push(run(vec![("a", s, 0)]));
        }
        let v = breadcrumb_verdicts(&rb, 3);
        assert_eq!(v, vec![Status::Fail, Status::Ok, Status::Ok]);
    }

    #[test]
    fn breadcrumb_zero_n_yields_empty() {
        let mut rb = RingBuffer::new();
        rb.push(run(vec![("a", Status::Ok, 0)]));
        assert!(breadcrumb_verdicts(&rb, 0).is_empty());
    }

    // --- sparkline_for_layer ---

    #[test]
    fn sparkline_blank_when_fewer_than_two_snapshots() {
        let rb = RingBuffer::new();
        assert_eq!(sparkline_for_layer(&rb, "logs", 10), "");
        let mut rb = RingBuffer::new();
        rb.push(run(vec![("logs", Status::Warn, 3)]));
        assert_eq!(sparkline_for_layer(&rb, "logs", 10), "");
    }

    #[test]
    fn sparkline_renders_one_glyph_per_snapshot() {
        let mut rb = RingBuffer::new();
        rb.push(run(vec![("logs", Status::Ok, 0)]));
        rb.push(run(vec![("logs", Status::Warn, 4)]));
        rb.push(run(vec![("logs", Status::Fail, 8)]));
        let sl = sparkline_for_layer(&rb, "logs", 10);
        assert_eq!(sl.chars().count(), 3);
        // Highest count maps to the top glyph.
        assert!(sl.ends_with('█'), "expected last rung to be full block: {sl:?}");
    }

    #[test]
    fn sparkline_skips_runs_missing_the_layer() {
        let mut rb = RingBuffer::new();
        rb.push(run(vec![("logs", Status::Ok, 1)]));
        rb.push(run(vec![("cluster", Status::Ok, 1)])); // logs absent
        rb.push(run(vec![("logs", Status::Ok, 2)]));
        let sl = sparkline_for_layer(&rb, "logs", 10);
        assert_eq!(sl.chars().count(), 2, "expected only runs that contain the layer: {sl:?}");
    }

    #[test]
    fn sparkline_caps_at_max_width() {
        let mut rb = RingBuffer::new();
        for i in 0..6usize {
            rb.push(run(vec![("logs", Status::Ok, i)]));
        }
        let sl = sparkline_for_layer(&rb, "logs", 3);
        assert_eq!(sl.chars().count(), 3);
    }

    #[test]
    fn sparkline_zero_width_is_blank() {
        let mut rb = RingBuffer::new();
        rb.push(run(vec![("logs", Status::Ok, 0)]));
        rb.push(run(vec![("logs", Status::Warn, 5)]));
        assert_eq!(sparkline_for_layer(&rb, "logs", 0), "");
    }

    #[test]
    fn sparkline_flat_zeros_renders_lowest_rung() {
        let mut rb = RingBuffer::new();
        rb.push(run(vec![("logs", Status::Ok, 0)]));
        rb.push(run(vec![("logs", Status::Ok, 0)]));
        let sl = sparkline_for_layer(&rb, "logs", 10);
        assert!(sl.chars().all(|c| c == '▁'), "flat zeros should be lowest rung: {sl:?}");
    }
}
