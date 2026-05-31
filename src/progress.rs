use std::collections::HashMap;
use std::time::Instant;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::sync::mpsc::Receiver;

use crate::tui::SendEvent;

// ── Formatting helpers ────────────────────────────────────────────────────────

#[must_use]
pub fn fmt_active_line(remote: &str, bytes_sent: u64, total_bytes: u64, speed_bps: f64, eta_secs: Option<f64>) -> String {
    let pct = (bytes_sent * 100).checked_div(total_bytes).map_or(0, |v| v.min(100));
    let eta = eta_secs.map_or_else(|| "ETA ?".to_owned(), |s| format!("ETA {}", fmt_duration(s)));
    format!(
        "{remote}: {} / {} ({pct}%) {}  {eta}",
        fmt_bytes(bytes_sent),
        fmt_bytes(total_bytes),
        fmt_speed(speed_bps),
    )
}

fn fmt_bytes(bytes: u64) -> String {
    const GB: u64 = 1_000_000_000;
    const MB: u64 = 1_000_000;
    const KB: u64 = 1_000;
    #[allow(clippy::cast_precision_loss)]
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.0} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.0} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn fmt_speed(bps: f64) -> String {
    if bps >= 1_000_000.0 {
        format!("{:.1} MB/s", bps / 1_000_000.0)
    } else if bps >= 1_000.0 {
        format!("{:.0} KB/s", bps / 1_000.0)
    } else {
        format!("{bps:.0} B/s")
    }
}

#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
fn fmt_duration(secs: f64) -> String {
    let s = secs.max(0.0) as u64;
    if s >= 60 {
        format!("{}m {:02}s", s / 60, s % 60)
    } else {
        format!("{s}s")
    }
}

// ── State machine ─────────────────────────────────────────────────────────────

enum RemoteEntryState {
    Waiting,
    Active {
        bytes_sent: u64,
        total_bytes: u64,
        speed_bps: f64,
        eta_secs: Option<f64>,
    },
    Terminal,
}

struct RemoteEntry {
    state: RemoteEntryState,
    last_progress: Option<(Instant, u64)>,
}

impl RemoteEntry {
    fn waiting() -> Self {
        Self { state: RemoteEntryState::Waiting, last_progress: None }
    }
}

pub struct ProgressTracker {
    remotes: HashMap<String, RemoteEntry>,
    done: bool,
}

impl ProgressTracker {
    #[must_use]
    pub fn new(remote_names: &[String]) -> Self {
        let remotes = remote_names
            .iter()
            .map(|n| (n.clone(), RemoteEntry::waiting()))
            .collect();
        Self { remotes, done: false }
    }

    /// Process one event. Returns a line to print immediately for terminal events.
    pub fn handle_event(&mut self, event: &SendEvent) -> Option<String> {
        match event {
            SendEvent::RemoteStarted { remote, total_bytes } => {
                let entry = self.remotes.entry(remote.clone()).or_insert_with(RemoteEntry::waiting);
                entry.state = RemoteEntryState::Active {
                    bytes_sent: 0,
                    total_bytes: *total_bytes,
                    speed_bps: 0.0,
                    eta_secs: None,
                };
                entry.last_progress = Some((Instant::now(), 0));
                None
            }
            SendEvent::RemoteProgress { remote, bytes_sent } => {
                if let Some(entry) = self.remotes.get_mut(remote) {
                    let now = Instant::now();
                    #[allow(clippy::cast_precision_loss)]
                    let (speed, eta) = match (entry.last_progress, &entry.state) {
                        (
                            Some((last_time, last_bytes)),
                            RemoteEntryState::Active { total_bytes, .. },
                        ) => {
                            let total = *total_bytes;
                            let dt = now.duration_since(last_time).as_secs_f64();
                            if dt > 0.0 {
                                let db = bytes_sent.saturating_sub(last_bytes) as f64;
                                let speed = db / dt;
                                let eta = if speed > 0.0 {
                                    let remaining = total.saturating_sub(*bytes_sent) as f64;
                                    Some(remaining / speed)
                                } else {
                                    None
                                };
                                (speed, eta)
                            } else {
                                match &entry.state {
                                    RemoteEntryState::Active { speed_bps, eta_secs, .. } => {
                                        (*speed_bps, *eta_secs)
                                    }
                                    _ => (0.0, None),
                                }
                            }
                        }
                        _ => (0.0, None),
                    };
                    if let RemoteEntryState::Active {
                        bytes_sent: ref mut bs,
                        ref mut speed_bps,
                        ref mut eta_secs,
                        ..
                    } = entry.state
                    {
                        *bs = *bytes_sent;
                        *speed_bps = speed;
                        *eta_secs = eta;
                    }
                    entry.last_progress = Some((now, *bytes_sent));
                }
                None
            }
            SendEvent::RemoteCompleted { remote, elapsed_secs, bytes } => {
                if let Some(entry) = self.remotes.get_mut(remote) {
                    entry.state = RemoteEntryState::Terminal;
                }
                Some(format!(
                    "{remote}: done — {} in {}",
                    fmt_bytes(*bytes),
                    fmt_duration(*elapsed_secs),
                ))
            }
            SendEvent::RemoteFailed { remote, error } => {
                if let Some(entry) = self.remotes.get_mut(remote) {
                    entry.state = RemoteEntryState::Terminal;
                }
                Some(format!("{remote}: failed — {error}"))
            }
            SendEvent::RemoteSkipped { remote } => {
                if let Some(entry) = self.remotes.get_mut(remote) {
                    entry.state = RemoteEntryState::Terminal;
                }
                Some(format!("{remote}: skipped"))
            }
            SendEvent::AllDone => {
                self.done = true;
                None
            }
            _ => None,
        }
    }

    /// Lines for all currently active remotes (for periodic ticks).
    #[must_use]
    pub fn tick_lines(&self) -> Vec<String> {
        let mut lines: Vec<String> = self
            .remotes
            .iter()
            .filter_map(|(name, entry)| {
                if let RemoteEntryState::Active { bytes_sent, total_bytes, speed_bps, eta_secs } =
                    &entry.state
                {
                    Some(fmt_active_line(name, *bytes_sent, *total_bytes, *speed_bps, *eta_secs))
                } else {
                    None
                }
            })
            .collect();
        lines.sort_unstable();
        lines
    }

    #[must_use]
    pub fn is_done(&self) -> bool {
        self.done
    }
}

// ── Plain (no-TTY) runner ─────────────────────────────────────────────────────

/// Consume events and write progress to stderr. Used when stdout is not a TTY.
///
/// Terminal events (completed/failed/skipped) are printed immediately.
/// Active remotes are printed every 10 seconds.
/// # Errors
/// Never returns `Err` in practice; signature matches `run_transfer` for uniform task handling.
pub async fn run_plain(mut rx: Receiver<SendEvent>) -> anyhow::Result<()> {
    let mut tracker = ProgressTracker::new(&[]);
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
    tick.tick().await; // consume first immediate tick

    loop {
        tokio::select! {
            maybe_event = rx.recv() => {
                let event = match maybe_event {
                    Some(e) => e,
                    None => SendEvent::AllDone,
                };
                if let Some(line) = tracker.handle_event(&event) {
                    eprintln!("{line}");
                }
                if tracker.is_done() {
                    return Ok(());
                }
            }
            _ = tick.tick() => {
                for line in tracker.tick_lines() {
                    eprintln!("{line}");
                }
            }
        }
    }
}

/// Consume events and render `indicatif` progress bars. Used when stdout is a TTY.
///
/// # Errors
/// Never returns `Err` in practice; signature matches `run_transfer` for uniform task handling.
pub async fn run_inline(mut rx: Receiver<SendEvent>, remote_names: Vec<String>) -> anyhow::Result<()> {
    let mp = MultiProgress::new();
    let style = ProgressStyle::with_template(
        "{prefix:.bold}  [{bar:16}] {pos:>3}%  {msg}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar())
    .progress_chars("\u{2588}\u{2591}\u{2591}");

    let bars: HashMap<String, ProgressBar> = remote_names
        .iter()
        .map(|name| {
            let pb = mp.add(ProgressBar::new(100));
            pb.set_style(style.clone());
            pb.set_prefix(name.clone());
            pb.set_message("waiting\u{2026}");
            (name.clone(), pb)
        })
        .collect();

    let mut tracker = ProgressTracker::new(&remote_names);

    loop {
        let event = match rx.recv().await {
            Some(e) => e,
            None => SendEvent::AllDone,
        };

        // Update indicatif bars based on event.
        match &event {
            SendEvent::RemoteStarted { remote, total_bytes } => {
                if let Some(pb) = bars.get(remote) {
                    pb.set_length(*total_bytes);
                    pb.set_position(0);
                    pb.set_message("starting\u{2026}");
                }
            }
            SendEvent::RemoteProgress { remote, bytes_sent } => {
                if let Some(pb) = bars.get(remote) {
                    pb.set_position(*bytes_sent);
                    // message updated below after tracker update
                }
            }
            SendEvent::RemoteCompleted { remote, elapsed_secs, bytes } => {
                if let Some(pb) = bars.get(remote) {
                    pb.finish_with_message(format!(
                        "\u{2713} {}  {}",
                        fmt_bytes(*bytes),
                        fmt_duration(*elapsed_secs),
                    ));
                }
            }
            SendEvent::RemoteFailed { remote, error } => {
                if let Some(pb) = bars.get(remote) {
                    pb.abandon_with_message(format!("\u{2717} {}", &error[..error.len().min(40)]));
                }
            }
            SendEvent::RemoteSkipped { remote } => {
                if let Some(pb) = bars.get(remote) {
                    pb.finish_with_message("skipped");
                }
            }
            SendEvent::AllDone => {
                for pb in bars.values() {
                    if !pb.is_finished() {
                        pb.abandon_with_message("\u{2717} not reached \u{2014} check logs");
                    }
                }
            }
            _ => {}
        }

        tracker.handle_event(&event);

        // Refresh speed/ETA message for active bars.
        for (name, entry) in &tracker.remotes {
            if let RemoteEntryState::Active { bytes_sent, total_bytes, speed_bps, eta_secs } =
                &entry.state
                && let Some(pb) = bars.get(name) {
                    pb.set_length(*total_bytes);
                    pb.set_position(*bytes_sent);
                    let eta = eta_secs
                        .map_or_else(|| "ETA ?".to_owned(), |s| format!("ETA {}", fmt_duration(s)));
                    pb.set_message(format!(
                        "{} / {}  {}  {eta}",
                        fmt_bytes(*bytes_sent),
                        fmt_bytes(*total_bytes),
                        fmt_speed(*speed_bps),
                    ));
            }
        }

        if tracker.is_done() {
            return Ok(());
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Cycle 1 — tracer bullet: progress line format
    #[test]
    fn fmt_active_line_formats_bytes_speed_eta() {
        let line = fmt_active_line(
            "tank/home \u{2192} primary",
            500_000_000,
            1_000_000_000,
            10_000_000.0,
            Some(50.0),
        );
        assert!(line.contains("tank/home → primary"), "should contain remote name");
        assert!(line.contains("500 MB"), "should contain bytes_sent");
        assert!(line.contains("1.0 GB"), "should contain total_bytes");
        assert!(line.contains("50%"), "should contain percentage");
        assert!(line.contains("10.0 MB/s"), "should contain speed");
        assert!(line.contains("ETA 50s"), "should contain ETA");
    }

    // Cycle 2 — tracker starts all remotes waiting
    #[test]
    fn tracker_starts_all_waiting() {
        let names = vec!["tank/home → primary".to_owned(), "tank/data → backup".to_owned()];
        let tracker = ProgressTracker::new(&names);
        assert!(!tracker.is_done());
        assert!(tracker.tick_lines().is_empty(), "no active remotes yet");
    }

    // Cycle 3 — RemoteStarted transitions to Active
    #[test]
    fn remote_started_appears_in_tick_lines() {
        let names = vec!["tank/home → primary".to_owned()];
        let mut tracker = ProgressTracker::new(&names);
        tracker.handle_event(&SendEvent::RemoteStarted {
            remote: "tank/home → primary".to_owned(),
            total_bytes: 1_000_000,
        });
        let lines = tracker.tick_lines();
        assert_eq!(lines.len(), 1, "one active remote");
        assert!(lines[0].contains("tank/home → primary"));
    }

    // Cycle 4 — RemoteProgress updates bytes_sent
    #[test]
    fn remote_progress_updates_bytes_in_tick_line() {
        let names = vec!["tank/home → primary".to_owned()];
        let mut tracker = ProgressTracker::new(&names);
        tracker.handle_event(&SendEvent::RemoteStarted {
            remote: "tank/home → primary".to_owned(),
            total_bytes: 1_000_000,
        });
        tracker.handle_event(&SendEvent::RemoteProgress {
            remote: "tank/home → primary".to_owned(),
            bytes_sent: 500_000,
        });
        let lines = tracker.tick_lines();
        assert!(lines[0].contains("500 KB"), "updated bytes_sent should appear");
    }

    // Cycle 5 — RemoteCompleted returns immediate line and removes from ticks
    #[test]
    fn completed_returns_immediate_line_and_leaves_tick() {
        let names = vec!["tank/home → primary".to_owned()];
        let mut tracker = ProgressTracker::new(&names);
        tracker.handle_event(&SendEvent::RemoteStarted {
            remote: "tank/home → primary".to_owned(),
            total_bytes: 100,
        });
        let line = tracker.handle_event(&SendEvent::RemoteCompleted {
            remote: "tank/home → primary".to_owned(),
            elapsed_secs: 2.5,
            bytes: 100,
        });
        assert!(line.is_some(), "should return immediate line");
        let text = line.unwrap();
        assert!(text.contains("tank/home → primary"), "line includes remote name");
        assert!(text.contains("done"), "line says done");
        assert!(tracker.tick_lines().is_empty(), "terminal remote excluded from ticks");
    }

    // Cycle 6 — RemoteFailed returns immediate line and removes from ticks
    #[test]
    fn failed_returns_immediate_line_and_leaves_tick() {
        let names = vec!["tank/home → primary".to_owned()];
        let mut tracker = ProgressTracker::new(&names);
        tracker.handle_event(&SendEvent::RemoteStarted {
            remote: "tank/home → primary".to_owned(),
            total_bytes: 100,
        });
        let line = tracker.handle_event(&SendEvent::RemoteFailed {
            remote: "tank/home → primary".to_owned(),
            error: "connection reset".to_owned(),
        });
        assert!(line.is_some());
        let text = line.unwrap();
        assert!(text.contains("failed"), "line says failed");
        assert!(text.contains("connection reset"), "line includes error");
        assert!(tracker.tick_lines().is_empty());
    }

    // Cycle 7 — RemoteSkipped returns immediate line and removes from ticks
    #[test]
    fn skipped_returns_immediate_line_and_leaves_tick() {
        let names = vec!["tank/home → primary".to_owned()];
        let mut tracker = ProgressTracker::new(&names);
        let line = tracker.handle_event(&SendEvent::RemoteSkipped {
            remote: "tank/home → primary".to_owned(),
        });
        assert!(line.is_some());
        assert!(line.unwrap().contains("skipped"));
        assert!(tracker.tick_lines().is_empty());
    }

    // Cycle 8 — AllDone marks tracker done
    #[test]
    fn all_done_marks_tracker_done() {
        let mut tracker = ProgressTracker::new(&[]);
        assert!(!tracker.is_done());
        tracker.handle_event(&SendEvent::AllDone);
        assert!(tracker.is_done());
    }

    // Cycle 9 — tick_lines excludes terminal remotes
    #[test]
    fn tick_lines_excludes_terminal_remotes() {
        let names = vec![
            "tank/home → primary".to_owned(),
            "tank/data → backup".to_owned(),
        ];
        let mut tracker = ProgressTracker::new(&names);
        tracker.handle_event(&SendEvent::RemoteStarted {
            remote: "tank/home → primary".to_owned(),
            total_bytes: 1_000,
        });
        tracker.handle_event(&SendEvent::RemoteStarted {
            remote: "tank/data → backup".to_owned(),
            total_bytes: 1_000,
        });
        tracker.handle_event(&SendEvent::RemoteCompleted {
            remote: "tank/home → primary".to_owned(),
            elapsed_secs: 1.0,
            bytes: 1_000,
        });
        let lines = tracker.tick_lines();
        assert_eq!(lines.len(), 1, "only one active remote remains");
        assert!(lines[0].contains("tank/data → backup"));
    }
}
