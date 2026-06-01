use std::io::{self, Stdout};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;


use anyhow::Context;
use crossterm::event::{Event, EventStream};
use crossterm::execute;
use futures_util::StreamExt as _;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::{Frame, Terminal};
use tokio::sync::mpsc::Receiver;

const SUMMARY_TIMEOUT_OK_SECS: u8 = 10;
const SUMMARY_TIMEOUT_FAIL_SECS: u8 = 30;

// ── Buffering logger ──────────────────────────────────────────────────────────

struct StoredRecord {
    level: log::Level,
    target: String,
    args: String,
}

struct TuiLogger {
    inner: env_logger::Logger,
    buffer: Mutex<Option<Vec<StoredRecord>>>,
}

impl log::Log for TuiLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &log::Record<'_>) {
        if !self.inner.enabled(record.metadata()) {
            return;
        }
        let mut guard = self.buffer.lock().unwrap();
        if let Some(buf) = &mut *guard {
            buf.push(StoredRecord {
                level: record.level(),
                target: record.target().to_owned(),
                args: record.args().to_string(),
            });
        } else {
            drop(guard);
            self.inner.log(record);
        }
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

static LOGGER: OnceLock<TuiLogger> = OnceLock::new();

/// Install the global logger. Call once at startup instead of `env_logger::init`.
///
/// While no TUI alternate screen is active, records pass straight through to
/// the inner `env_logger`. While an alternate screen is active (inside
/// [`run_countdown`] or [`run_transfer`]) records are buffered and replayed to
/// the restored terminal on exit.
///
/// # Panics
/// Panics if a global logger has already been installed by a prior call.
pub fn init_logger(level: &str) {
    let inner = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(level),
    )
    .build();
    let max_level = inner.filter();
    let logger = TuiLogger { inner, buffer: Mutex::new(None) };
    if LOGGER.set(logger).is_ok() {
        log::set_logger(LOGGER.get().unwrap()).expect("logger already set");
        log::set_max_level(max_level);
    }
}

fn start_buffering() {
    if let Some(logger) = LOGGER.get() {
        let mut buffer_lock = logger.buffer.lock().unwrap();
        if buffer_lock.is_none() {
            *buffer_lock = Some(Vec::new());
        }
    }
}

fn stop_and_replay() {
    let Some(logger) = LOGGER.get() else { return };
    let records = {
        let mut guard = logger.buffer.lock().unwrap();
        guard.take().unwrap_or_default()
    };
    // Lock released before replay so log:: calls can re-acquire it (they pass through when buffer is None).
    for r in records {
        log::log!(target: &r.target, r.level, "{}", r.args);
    }
}

// ── Events ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum SendEvent {
    CountdownTick { remaining_secs: u8 },
    CountdownAborted,
    RemoteStarted { remote: String, total_bytes: u64 },
    RemoteProgress { remote: String, bytes_sent: u64 },
    RemoteCompleted { remote: String, elapsed_secs: f64, bytes: u64 },
    RemoteFailed { remote: String, error: String },
    RemoteSkipped { remote: String },
    RemoteUpToDate { remote: String },
    AllDone,
    SummaryTick,
}

// ── Key enum ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuiKey {
    Any,
    Skip,
    Quit,
    Up,
    Down,
}

// ── Remote row ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum RemoteRowState {
    Waiting,
    Active {
        bytes_sent: u64,
        total_bytes: u64,
        speed_bps: f64,
        eta_secs: Option<f64>,
    },
    Done {
        bytes: u64,
        elapsed_secs: f64,
    },
    Failed {
        error: String,
    },
    Skipped,
    UpToDate,
}

#[derive(Debug, Clone)]
pub struct RemoteRow {
    pub name: String,
    pub state: RemoteRowState,
    last_progress: Option<(std::time::Instant, u64)>,
}

impl RemoteRow {
    fn new(name: String) -> Self {
        Self {
            name,
            state: RemoteRowState::Waiting,
            last_progress: None,
        }
    }
}

// ── State machine ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TuiState {
    Countdown { remaining: u8 },
    Aborted,
    Transferring,
    Summary,
    Exiting,
}

pub struct TuiApp {
    pub state: TuiState,
    pub remotes: Vec<RemoteRow>,
    pub focused: usize,
    pub summary_countdown: u8,
    cancellation_tokens: std::collections::HashMap<String, tokio_util::sync::CancellationToken>,
}

impl TuiApp {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: TuiState::Countdown { remaining: 5 },
            remotes: vec![],
            focused: 0,
            summary_countdown: SUMMARY_TIMEOUT_OK_SECS,
            cancellation_tokens: std::collections::HashMap::new(),
        }
    }

    #[must_use]
    pub fn for_transfer(remote_names: Vec<String>) -> Self {
        let cancellation_tokens = remote_names
            .iter()
            .map(|n| (n.clone(), tokio_util::sync::CancellationToken::new()))
            .collect();
        let remotes = remote_names.into_iter().map(RemoteRow::new).collect();
        Self {
            state: TuiState::Transferring,
            remotes,
            focused: 0,
            summary_countdown: SUMMARY_TIMEOUT_OK_SECS,
            cancellation_tokens,
        }
    }

    #[must_use]
    #[allow(clippy::implicit_hasher)]
    pub fn for_transfer_with_tokens(
        remote_names: Vec<String>,
        cancellation_tokens: std::collections::HashMap<String, tokio_util::sync::CancellationToken>,
    ) -> Self {
        let remotes = remote_names.into_iter().map(RemoteRow::new).collect();
        Self {
            state: TuiState::Transferring,
            remotes,
            focused: 0,
            summary_countdown: SUMMARY_TIMEOUT_OK_SECS,
            cancellation_tokens,
        }
    }

    /// Returns clones of all per-remote cancellation tokens.
    ///
    /// Cancelling any clone cancels the corresponding in-flight transfer.
    #[must_use]
    pub fn cancel_map(
        &self,
    ) -> std::collections::HashMap<String, tokio_util::sync::CancellationToken> {
        self.cancellation_tokens
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    #[allow(clippy::too_many_lines)]
    pub fn handle_send_event(&mut self, event: &SendEvent) {
        match event {
            SendEvent::CountdownTick { remaining_secs: 0 } => {
                if matches!(self.state, TuiState::Countdown { .. }) {
                    let _ = sd_notify::notify(&[sd_notify::NotifyState::Watchdog]);
                    self.state = TuiState::Transferring;
                }
            }
            SendEvent::CountdownTick { remaining_secs } => {
                if matches!(self.state, TuiState::Countdown { .. }) {
                    let _ = sd_notify::notify(&[sd_notify::NotifyState::Watchdog]);
                    self.state = TuiState::Countdown {
                        remaining: *remaining_secs,
                    };
                }
            }
            SendEvent::CountdownAborted => {
                if matches!(self.state, TuiState::Countdown { .. }) {
                    self.state = TuiState::Aborted;
                }
            }
            SendEvent::RemoteProgress { remote, bytes_sent } => {
                if let Some(row) = self.remotes.iter_mut().find(|r| r.name == *remote) {
                    let now = std::time::Instant::now();
                    #[allow(clippy::cast_precision_loss)]
                    let (speed, eta) = match (row.last_progress, &row.state) {
                        (
                            Some((last_time, last_bytes)),
                            RemoteRowState::Active { total_bytes, .. },
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
                                match &row.state {
                                    RemoteRowState::Active { speed_bps, eta_secs, .. } => {
                                        (*speed_bps, *eta_secs)
                                    }
                                    _ => (0.0, None),
                                }
                            }
                        }
                        _ => (0.0, None),
                    };
                    if let RemoteRowState::Active {
                        bytes_sent: ref mut bs,
                        ref mut speed_bps,
                        ref mut eta_secs,
                        ..
                    } = row.state
                    {
                        *bs = *bytes_sent;
                        *speed_bps = speed;
                        *eta_secs = eta;
                    }
                    row.last_progress = Some((now, *bytes_sent));
                    let _ = sd_notify::notify(&[sd_notify::NotifyState::Watchdog]);
                }
            }
            SendEvent::RemoteStarted { remote, total_bytes } => {
                if let Some(row) = self.remotes.iter_mut().find(|r| r.name == *remote) {
                    row.state = RemoteRowState::Active {
                        bytes_sent: 0,
                        total_bytes: *total_bytes,
                        speed_bps: 0.0,
                        eta_secs: None,
                    };
                    row.last_progress = Some((std::time::Instant::now(), 0));
                }
            }
            SendEvent::RemoteCompleted { remote, elapsed_secs, bytes } => {
                if let Some(row) = self.remotes.iter_mut().find(|r| r.name == *remote) {
                    row.state = RemoteRowState::Done {
                        bytes: *bytes,
                        elapsed_secs: *elapsed_secs,
                    };
                }
            }
            SendEvent::RemoteFailed { remote, error } => {
                if let Some(row) = self.remotes.iter_mut().find(|r| r.name == *remote) {
                    row.state = RemoteRowState::Failed { error: error.clone() };
                }
            }
            SendEvent::RemoteSkipped { remote } => {
                if let Some(row) = self.remotes.iter_mut().find(|r| r.name == *remote) {
                    row.state = RemoteRowState::Skipped;
                }
            }
            SendEvent::RemoteUpToDate { remote } => {
                if let Some(row) = self.remotes.iter_mut().find(|r| r.name == *remote) {
                    row.state = RemoteRowState::UpToDate;
                }
            }
            SendEvent::AllDone => {
                if matches!(self.state, TuiState::Transferring) {
                    let any_failed = self
                        .remotes
                        .iter()
                        .any(|r| matches!(r.state, RemoteRowState::Failed { .. }));
                    self.summary_countdown = if any_failed { SUMMARY_TIMEOUT_FAIL_SECS } else { SUMMARY_TIMEOUT_OK_SECS };
                    self.state = TuiState::Summary;
                }
            }
            SendEvent::SummaryTick => {
                if matches!(self.state, TuiState::Summary) {
                    let _ = sd_notify::notify(&[sd_notify::NotifyState::Watchdog]);
                    self.summary_countdown = self.summary_countdown.saturating_sub(1);
                    if self.summary_countdown == 0 {
                        self.state = TuiState::Exiting;
                    }
                }
            }
        }
    }

    pub fn handle_key(&mut self, key: TuiKey) {
        match &self.state {
            TuiState::Countdown { .. } => {
                self.state = TuiState::Aborted;
            }
            TuiState::Summary => {
                self.state = TuiState::Exiting;
            }
            TuiState::Transferring => match key {
                TuiKey::Skip => {
                    if self.remotes.len() == 1 {
                        if let Some(name) = self.remotes.first().map(|r| r.name.clone())
                            && let Some(tok) = self.cancellation_tokens.get(&name)
                        {
                            tok.cancel();
                        }
                        self.skip_all_active();
                        self.state = TuiState::Summary;
                    } else if let Some(row) = self.remotes.get_mut(self.focused)
                        && matches!(row.state, RemoteRowState::Active { .. } | RemoteRowState::Waiting)
                    {
                        let name = row.name.clone();
                        if let Some(tok) = self.cancellation_tokens.get(&name) {
                            tok.cancel();
                        }
                        row.state = RemoteRowState::Skipped;
                    }
                }
                TuiKey::Quit => {
                    for tok in self.cancellation_tokens.values() {
                        tok.cancel();
                    }
                    self.skip_all_active();
                    self.state = TuiState::Summary;
                }
                TuiKey::Up => {
                    self.focused = self.focused.saturating_sub(1);
                }
                TuiKey::Down => {
                    if self.focused + 1 < self.remotes.len() {
                        self.focused += 1;
                    }
                }
                TuiKey::Any => {}
            },
            _ => {}
        }
    }

    fn skip_all_active(&mut self) {
        for row in &mut self.remotes {
            if matches!(row.state, RemoteRowState::Active { .. } | RemoteRowState::Waiting) {
                row.state = RemoteRowState::Skipped;
            }
        }
    }
}

impl Default for TuiApp {
    fn default() -> Self {
        Self::new()
    }
}

// ── Countdown runner ──────────────────────────────────────────────────────────

/// Run the 5-second countdown screen.
///
/// Returns `true` if the countdown completed without a keypress (transfer
/// should proceed), or `false` if the user pressed a key (backup skipped).
///
/// # Errors
/// Returns `Err` on terminal I/O failure.
pub async fn run_countdown(datasets: &[(String, Vec<String>)]) -> anyhow::Result<bool> {
    enable_raw_mode().context("enable raw mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).context("enter alternate screen")?;
    start_buffering();

    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend).context("create terminal")?;

    let result = countdown_loop(&mut terminal, datasets).await;

    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen);
    terminal.show_cursor().ok();
    stop_and_replay();

    result
}

async fn countdown_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    datasets: &[(String, Vec<String>)],
) -> anyhow::Result<bool> {
    let mut app = TuiApp::new();
    let mut event_stream = EventStream::new();
    let mut tick = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    let mut remaining: u8 = 5;

    loop {
        terminal.draw(|f| render(f, &app, datasets))?;

        tokio::select! {
            _ = tick.tick() => {
                remaining = remaining.saturating_sub(1);
                app.handle_send_event(&SendEvent::CountdownTick { remaining_secs: remaining });
            }
            maybe_event = event_stream.next() => {
                if let Some(Ok(Event::Key(_))) = maybe_event {
                    app.handle_key(TuiKey::Any);
                }
            }
        }

        match app.state {
            TuiState::Aborted => {
                terminal.draw(|f| render(f, &app, datasets))?;
                tokio::time::sleep(Duration::from_secs(1)).await;
                return Ok(false);
            }
            TuiState::Transferring => return Ok(true),
            _ => {}
        }
    }
}

fn render(frame: &mut Frame<'_>, app: &TuiApp, datasets: &[(String, Vec<String>)]) {
    let area = frame.area();
    let box_area = centered_box(area, 62, dataset_box_height(datasets));

    let block = Block::default()
        .borders(Borders::ALL)
        .title(Span::styled(
            " ZRB ZFS Backup ",
            Style::default().add_modifier(Modifier::BOLD),
        ))
        .title_alignment(Alignment::Center);

    let inner = block.inner(box_area);
    frame.render_widget(block, box_area);

    let mut lines: Vec<Line<'_>> = vec![Line::default()];

    for (dataset, remotes) in datasets {
        let remotes_str = if remotes.is_empty() {
            "(no remotes)".to_owned()
        } else {
            remotes.join(", ")
        };
        lines.push(Line::from(format!("  {dataset}  →  {remotes_str}")));
    }
    lines.push(Line::default());

    match &app.state {
        TuiState::Countdown { remaining } => {
            lines.push(
                Line::from(Span::styled(
                    format!("  Starting in {remaining}s\u{2026}"),
                    Style::default().add_modifier(Modifier::BOLD),
                ))
                .alignment(Alignment::Center),
            );
            lines.push(Line::default());
            lines.push(
                Line::from("  Press any key to cancel").alignment(Alignment::Center),
            );
        }
        TuiState::Aborted => {
            lines.push(
                Line::from(Span::styled(
                    "  Backup skipped.",
                    Style::default().add_modifier(Modifier::BOLD),
                ))
                .alignment(Alignment::Center),
            );
        }
        _ => {}
    }

    frame.render_widget(Paragraph::new(lines), inner);
}

fn centered_box(area: Rect, desired_w: u16, desired_h: u16) -> Rect {
    let w = desired_w.min(area.width.saturating_sub(2));
    let h = desired_h.min(area.height.saturating_sub(2));
    let x = area.x + area.width.saturating_sub(w) / 2;
    let y = area.y + area.height.saturating_sub(h) / 2;
    Rect::new(x, y, w, h)
}

fn dataset_box_height(datasets: &[(String, Vec<String>)]) -> u16 {
    // borders (2) + top padding (1) + one row per dataset + padding (1) +
    // countdown/skip line (1) + empty line (1) + footer (1)
    let rows = u16::try_from(datasets.len()).unwrap_or(u16::MAX);
    2 + 1 + rows + 1 + 1 + 1 + 1
}

// ── Transfer screen I/O ───────────────────────────────────────────────────────

/// Build one `CancellationToken` per remote name.
///
/// Pass the returned map to [`run_transfer`] and to [`ops::send::send`] /
/// [`ops::send::send_resume`] so that `s`/`q` key presses abort the
/// corresponding in-flight `zfs send` stream.
#[must_use]
pub fn build_cancel_map(
    remote_names: &[String],
) -> std::collections::HashMap<String, tokio_util::sync::CancellationToken> {
    remote_names
        .iter()
        .map(|n| (n.clone(), tokio_util::sync::CancellationToken::new()))
        .collect()
}

/// Run the transfer screen.
///
/// Reads `SendEvent`s from `rx` and re-renders at ~60 fps.
/// Exits when `AllDone` transitions app to `Summary`, when `q` is pressed, or
/// when the channel is closed (sender dropped).
///
/// `cancel_tokens` should be the map returned by [`build_cancel_map`] for the
/// same set of remotes.  Pass a clone of the same map to `send`/`send_resume`
/// so key presses abort the corresponding in-flight transfers.
///
/// # Errors
/// Returns `Err` on terminal I/O failure.
#[allow(clippy::implicit_hasher)]
pub async fn run_transfer(
    rx: Receiver<SendEvent>,
    remote_names: Vec<String>,
    cancel_tokens: std::collections::HashMap<String, tokio_util::sync::CancellationToken>,
) -> anyhow::Result<()> {
    enable_raw_mode().context("enable raw mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).context("enter alternate screen")?;
    start_buffering();

    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend).context("create terminal")?;

    let result = transfer_loop(&mut terminal, rx, remote_names, cancel_tokens).await;

    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen);
    terminal.show_cursor().ok();
    stop_and_replay();

    result
}

#[allow(clippy::implicit_hasher)]
async fn transfer_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    mut rx: Receiver<SendEvent>,
    remote_names: Vec<String>,
    cancel_tokens: std::collections::HashMap<String, tokio_util::sync::CancellationToken>,
) -> anyhow::Result<()> {
    let mut app = TuiApp::for_transfer_with_tokens(remote_names, cancel_tokens);
    let mut event_stream = EventStream::new();
    let mut render_interval = tokio::time::interval(Duration::from_millis(16));
    let mut summary_interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = render_interval.tick() => {
                terminal.draw(|f| render_transfer(f, &app))?;
            }
            maybe_event = event_stream.next() => {
                if let Some(Ok(Event::Key(key))) = maybe_event {
                    use crossterm::event::KeyCode;
                    let tui_key = match key.code {
                        KeyCode::Char('s' | 'S') => TuiKey::Skip,
                        KeyCode::Char('q' | 'Q') => TuiKey::Quit,
                        KeyCode::Up => TuiKey::Up,
                        KeyCode::Down => TuiKey::Down,
                        _ => TuiKey::Any,
                    };
                    app.handle_key(tui_key);
                }
            }
            maybe_send_event = rx.recv() => {
                let send_event = match maybe_send_event {
                    Some(e) => e,
                    None => SendEvent::AllDone,
                };
                let was_transferring = matches!(app.state, TuiState::Transferring);
                app.handle_send_event(&send_event);
                if was_transferring && matches!(app.state, TuiState::Summary) {
                    summary_interval.reset();
                }
            }
            _ = summary_interval.tick(), if matches!(app.state, TuiState::Summary) => {
                app.handle_send_event(&SendEvent::SummaryTick);
            }
        }

        if matches!(app.state, TuiState::Exiting) {
            terminal.draw(|f| render_transfer(f, &app))?;
            return Ok(());
        }
    }
}

fn summary_title(app: &TuiApp) -> &'static str {
    let any_bad = app.remotes.iter().any(|r| {
        matches!(r.state, RemoteRowState::Failed { .. } | RemoteRowState::Skipped)
    });
    if any_bad { " Backup Finished " } else { " Backup Complete " }
}

fn render_transfer(frame: &mut Frame<'_>, app: &TuiApp) {
    let area = frame.area();

    let is_summary = matches!(app.state, TuiState::Summary | TuiState::Exiting);
    let any_bad = app.remotes.iter().any(|r| {
        matches!(r.state, RemoteRowState::Failed { .. } | RemoteRowState::Skipped)
    });

    let title = if is_summary { summary_title(app) } else { " ZRB ZFS Backup " };

    let border_color = if is_summary {
        if any_bad { Color::Red } else { Color::Green }
    } else {
        Color::Reset
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .title(Span::styled(
            title,
            Style::default().add_modifier(Modifier::BOLD).fg(border_color),
        ))
        .title_alignment(Alignment::Center);

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let name_w = app.remotes.iter().map(|r| r.name.len()).max().unwrap_or(8);

    let mut lines: Vec<Line<'_>> = vec![Line::default()];
    for (i, row) in app.remotes.iter().enumerate() {
        let focused = i == app.focused && matches!(app.state, TuiState::Transferring);
        lines.push(render_remote_row(row, name_w, focused));
    }

    lines.push(Line::default());

    match app.state {
        TuiState::Transferring => {
            lines.push(
                Line::from(Span::styled(
                    "[S] Skip remote   [Q] Quit",
                    Style::default().fg(Color::DarkGray),
                ))
                .alignment(Alignment::Center),
            );
        }
        TuiState::Summary => {
            lines.push(
                Line::from(Span::styled(
                    format!("Closing in {}s \u{2014} press any key to dismiss", app.summary_countdown),
                    Style::default().fg(Color::DarkGray),
                ))
                .alignment(Alignment::Center),
            );
        }
        TuiState::Exiting => {
            lines.push(
                Line::from(Span::styled(
                    "Closing\u{2026}",
                    Style::default().fg(Color::DarkGray),
                ))
                .alignment(Alignment::Center),
            );
        }
        _ => {}
    }

    frame.render_widget(Paragraph::new(lines), inner);
}

fn render_remote_row(row: &RemoteRow, name_w: usize, focused: bool) -> Line<'_> {
    let name_style = if focused {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
    };
    let name_span = Span::styled(format!("{:<width$}", row.name, width = name_w), name_style);

    match &row.state {
        RemoteRowState::Waiting => Line::from(vec![
            name_span,
            Span::raw("  "),
            Span::styled("waiting\u{2026}", Style::default().fg(Color::DarkGray)),
        ]),
        RemoteRowState::Active { bytes_sent, total_bytes, speed_bps, eta_secs } => {
            let pct: u8 = if *total_bytes > 0 {
                u8::try_from((bytes_sent * 100 / total_bytes).min(100)).unwrap_or(100)
            } else {
                0
            };
            let bar = make_bar(pct, 16);
            let speed_str = fmt_speed(*speed_bps);
            let eta_str = eta_secs
                .map_or_else(|| "ETA ?".to_owned(), |s| format!("ETA {}", fmt_duration(s)));
            Line::from(vec![
                name_span,
                Span::raw(format!(
                    "  [{bar}] {pct:>3}%  {} / {}  {speed_str}  {eta_str}",
                    fmt_bytes(*bytes_sent),
                    fmt_bytes(*total_bytes),
                )),
            ])
        }
        RemoteRowState::Done { bytes, elapsed_secs } => Line::from(vec![
            name_span,
            Span::styled(
                format!("  \u{2713} {}  {}", fmt_bytes(*bytes), fmt_duration(*elapsed_secs)),
                Style::default().add_modifier(Modifier::BOLD).fg(Color::Green),
            ),
        ]),
        RemoteRowState::Failed { error } => Line::from(vec![
            name_span,
            Span::styled(
                format!("  \u{2717} {}", truncate_str(error, 40)),
                Style::default().add_modifier(Modifier::REVERSED).fg(Color::Red),
            ),
        ]),
        RemoteRowState::Skipped => Line::from(vec![
            name_span,
            Span::styled("  skipped", Style::default().add_modifier(Modifier::DIM)),
        ]),
        RemoteRowState::UpToDate => Line::from(vec![
            name_span,
            Span::styled("  up to date", Style::default().add_modifier(Modifier::DIM)),
        ]),
    }
}

fn make_bar(pct: u8, width: usize) -> String {
    let filled = (pct as usize * width / 100).min(width);
    "\u{2588}".repeat(filled) + &"\u{2591}".repeat(width - filled)
}

#[allow(clippy::cast_precision_loss)]
fn fmt_bytes(bytes: u64) -> String {
    const GB: u64 = 1_000_000_000;
    const MB: u64 = 1_000_000;
    const KB: u64 = 1_000;
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

fn truncate_str(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_state_is_countdown_5() {
        let app = TuiApp::new();
        assert_eq!(app.state, TuiState::Countdown { remaining: 5 });
    }

    #[test]
    fn countdown_tick_updates_remaining() {
        let mut app = TuiApp::new();
        app.handle_send_event(&SendEvent::CountdownTick { remaining_secs: 3 });
        assert_eq!(app.state, TuiState::Countdown { remaining: 3 });
    }

    #[test]
    fn countdown_tick_zero_transitions_to_transferring() {
        let mut app = TuiApp::new();
        app.handle_send_event(&SendEvent::CountdownTick { remaining_secs: 0 });
        assert_eq!(app.state, TuiState::Transferring);
    }

    #[test]
    fn key_in_countdown_state_transitions_to_aborted() {
        let mut app = TuiApp::new();
        app.handle_key(TuiKey::Any);
        assert_eq!(app.state, TuiState::Aborted);
    }

    #[test]
    fn key_in_transferring_state_has_no_effect() {
        let mut app = TuiApp::new();
        app.handle_send_event(&SendEvent::CountdownTick { remaining_secs: 0 });
        assert_eq!(app.state, TuiState::Transferring);
        app.handle_key(TuiKey::Any);
        assert_eq!(app.state, TuiState::Transferring);
    }

    #[test]
    fn countdown_aborted_event_transitions_to_aborted() {
        let mut app = TuiApp::new();
        app.handle_send_event(&SendEvent::CountdownAborted);
        assert_eq!(app.state, TuiState::Aborted);
    }

    #[test]
    fn ticks_do_not_change_aborted_state() {
        let mut app = TuiApp::new();
        app.handle_key(TuiKey::Any);
        assert_eq!(app.state, TuiState::Aborted);
        app.handle_send_event(&SendEvent::CountdownTick { remaining_secs: 2 });
        assert_eq!(app.state, TuiState::Aborted, "tick must not revive aborted state");
    }

    // ── Issue 03: Transfer screen state machine ───────────────────────────────

    #[test]
    fn for_transfer_starts_in_transferring_with_waiting_rows() {
        let app = TuiApp::for_transfer(vec!["primary".into(), "secondary".into()]);
        assert_eq!(app.state, TuiState::Transferring);
        assert_eq!(app.remotes.len(), 2);
        assert_eq!(app.remotes[0].name, "primary");
        assert!(matches!(app.remotes[0].state, RemoteRowState::Waiting));
        assert_eq!(app.remotes[1].name, "secondary");
        assert!(matches!(app.remotes[1].state, RemoteRowState::Waiting));
    }

    #[test]
    fn remote_started_transitions_row_to_active() {
        let mut app = TuiApp::for_transfer(vec!["primary".into()]);
        app.handle_send_event(&SendEvent::RemoteStarted {
            remote: "primary".into(),
            total_bytes: 1024,
        });
        assert!(
            matches!(app.remotes[0].state, RemoteRowState::Active { total_bytes: 1024, bytes_sent: 0, .. }),
            "expected Active with total_bytes=1024, got {:?}", app.remotes[0].state
        );
    }

    #[test]
    fn remote_progress_updates_bytes_sent_in_active_row() {
        let mut app = TuiApp::for_transfer(vec!["primary".into()]);
        app.handle_send_event(&SendEvent::RemoteStarted {
            remote: "primary".into(),
            total_bytes: 1024,
        });
        app.handle_send_event(&SendEvent::RemoteProgress {
            remote: "primary".into(),
            bytes_sent: 512,
        });
        match &app.remotes[0].state {
            RemoteRowState::Active { bytes_sent, .. } => assert_eq!(*bytes_sent, 512),
            s => panic!("expected Active, got {s:?}"),
        }
    }

    #[test]
    fn remote_progress_on_unknown_remote_is_ignored() {
        let mut app = TuiApp::for_transfer(vec!["primary".into()]);
        app.handle_send_event(&SendEvent::RemoteProgress {
            remote: "ghost".into(),
            bytes_sent: 100,
        });
        assert!(matches!(app.remotes[0].state, RemoteRowState::Waiting));
    }

    #[test]
    fn remote_completed_transitions_row_to_done() {
        let mut app = TuiApp::for_transfer(vec!["primary".into()]);
        app.handle_send_event(&SendEvent::RemoteStarted { remote: "primary".into(), total_bytes: 100 });
        app.handle_send_event(&SendEvent::RemoteCompleted {
            remote: "primary".into(),
            elapsed_secs: 1.5,
            bytes: 100,
        });
        assert!(
            matches!(app.remotes[0].state, RemoteRowState::Done { bytes: 100, .. }),
            "expected Done, got {:?}", app.remotes[0].state
        );
    }

    #[test]
    fn remote_failed_transitions_row_to_failed() {
        let mut app = TuiApp::for_transfer(vec!["primary".into()]);
        app.handle_send_event(&SendEvent::RemoteStarted { remote: "primary".into(), total_bytes: 100 });
        app.handle_send_event(&SendEvent::RemoteFailed {
            remote: "primary".into(),
            error: "connection reset".into(),
        });
        match &app.remotes[0].state {
            RemoteRowState::Failed { error } => assert_eq!(error, "connection reset"),
            s => panic!("expected Failed, got {s:?}"),
        }
    }

    #[test]
    fn remote_skipped_event_transitions_row_to_skipped() {
        let mut app = TuiApp::for_transfer(vec!["primary".into()]);
        app.handle_send_event(&SendEvent::RemoteSkipped { remote: "primary".into() });
        assert!(matches!(app.remotes[0].state, RemoteRowState::Skipped));
    }

    #[test]
    fn remote_up_to_date_event_transitions_row_to_up_to_date() {
        let mut app = TuiApp::for_transfer(vec!["primary".into()]);
        app.handle_send_event(&SendEvent::RemoteUpToDate { remote: "primary".into() });
        assert!(matches!(app.remotes[0].state, RemoteRowState::UpToDate));
    }

    #[test]
    fn s_key_skips_focused_active_row() {
        let mut app = TuiApp::for_transfer(vec!["primary".into(), "secondary".into()]);
        app.handle_send_event(&SendEvent::RemoteStarted { remote: "primary".into(), total_bytes: 100 });
        app.handle_send_event(&SendEvent::RemoteStarted { remote: "secondary".into(), total_bytes: 100 });
        app.focused = 0;
        app.handle_key(TuiKey::Skip);
        assert!(matches!(app.remotes[0].state, RemoteRowState::Skipped), "focused row should be skipped");
        assert!(matches!(app.remotes[1].state, RemoteRowState::Active { .. }), "unfocused row should be untouched");
        assert_eq!(app.state, TuiState::Transferring, "state should stay Transferring");
    }

    #[test]
    fn s_key_with_single_remote_behaves_like_q() {
        let mut app = TuiApp::for_transfer(vec!["primary".into()]);
        app.handle_send_event(&SendEvent::RemoteStarted { remote: "primary".into(), total_bytes: 100 });
        app.handle_key(TuiKey::Skip);
        assert!(matches!(app.remotes[0].state, RemoteRowState::Skipped));
        assert_eq!(app.state, TuiState::Summary);
    }

    #[test]
    fn q_key_skips_all_active_and_transitions_to_summary() {
        let mut app = TuiApp::for_transfer(vec!["primary".into(), "secondary".into()]);
        app.handle_send_event(&SendEvent::RemoteStarted { remote: "primary".into(), total_bytes: 100 });
        app.handle_send_event(&SendEvent::RemoteStarted { remote: "secondary".into(), total_bytes: 100 });
        app.handle_key(TuiKey::Quit);
        assert!(matches!(app.remotes[0].state, RemoteRowState::Skipped));
        assert!(matches!(app.remotes[1].state, RemoteRowState::Skipped));
        assert_eq!(app.state, TuiState::Summary);
    }

    #[test]
    fn q_key_does_not_skip_done_rows() {
        let mut app = TuiApp::for_transfer(vec!["primary".into(), "secondary".into()]);
        app.handle_send_event(&SendEvent::RemoteCompleted {
            remote: "primary".into(), elapsed_secs: 1.0, bytes: 100,
        });
        app.handle_send_event(&SendEvent::RemoteStarted { remote: "secondary".into(), total_bytes: 100 });
        app.handle_key(TuiKey::Quit);
        assert!(matches!(app.remotes[0].state, RemoteRowState::Done { .. }), "Done row should not be changed");
        assert!(matches!(app.remotes[1].state, RemoteRowState::Skipped));
        assert_eq!(app.state, TuiState::Summary);
    }

    #[test]
    fn arrow_keys_move_focus() {
        let mut app = TuiApp::for_transfer(vec!["a".into(), "b".into(), "c".into()]);
        assert_eq!(app.focused, 0);
        app.handle_key(TuiKey::Down);
        assert_eq!(app.focused, 1);
        app.handle_key(TuiKey::Down);
        assert_eq!(app.focused, 2);
        app.handle_key(TuiKey::Down); // clamps at end
        assert_eq!(app.focused, 2);
        app.handle_key(TuiKey::Up);
        assert_eq!(app.focused, 1);
        app.handle_key(TuiKey::Up);
        assert_eq!(app.focused, 0);
        app.handle_key(TuiKey::Up); // clamps at start
        assert_eq!(app.focused, 0);
    }

    #[test]
    fn any_key_in_summary_transitions_to_exiting() {
        let mut app = TuiApp::for_transfer(vec!["r".into()]);
        app.handle_send_event(&SendEvent::AllDone);
        assert_eq!(app.state, TuiState::Summary);
        app.handle_key(TuiKey::Any);
        assert_eq!(app.state, TuiState::Exiting);
    }

    #[test]
    fn all_done_in_transferring_transitions_to_summary() {
        let mut app = TuiApp::new();
        app.handle_send_event(&SendEvent::CountdownTick { remaining_secs: 0 });
        assert_eq!(app.state, TuiState::Transferring);
        app.handle_send_event(&SendEvent::AllDone);
        assert_eq!(app.state, TuiState::Summary);
    }

    // ── Issue 04: Summary screen state machine ────────────────────────────────

    #[test]
    fn summary_countdown_starts_at_ok_timeout_when_no_failures() {
        let mut app = TuiApp::for_transfer(vec!["r".into()]);
        app.handle_send_event(&SendEvent::AllDone);
        assert_eq!(app.state, TuiState::Summary);
        assert_eq!(app.summary_countdown, SUMMARY_TIMEOUT_OK_SECS);
    }

    #[test]
    fn summary_countdown_starts_at_fail_timeout_when_remote_failed() {
        let mut app = TuiApp::for_transfer(vec!["r".into()]);
        app.handle_send_event(&SendEvent::RemoteFailed {
            remote: "r".into(),
            error: "connection reset".into(),
        });
        app.handle_send_event(&SendEvent::AllDone);
        assert_eq!(app.state, TuiState::Summary);
        assert_eq!(app.summary_countdown, SUMMARY_TIMEOUT_FAIL_SECS);
    }

    #[test]
    fn summary_countdown_is_ok_timeout_when_only_skipped() {
        let mut app = TuiApp::for_transfer(vec!["r".into()]);
        app.handle_send_event(&SendEvent::RemoteSkipped { remote: "r".into() });
        app.handle_send_event(&SendEvent::AllDone);
        assert_eq!(app.state, TuiState::Summary);
        assert_eq!(app.summary_countdown, SUMMARY_TIMEOUT_OK_SECS);
    }

    #[test]
    fn one_summary_tick_decrements_countdown() {
        let mut app = TuiApp::for_transfer(vec!["r".into()]);
        app.handle_send_event(&SendEvent::AllDone);
        app.handle_send_event(&SendEvent::SummaryTick);
        assert_eq!(app.state, TuiState::Summary, "still Summary after one tick");
        assert_eq!(app.summary_countdown, SUMMARY_TIMEOUT_OK_SECS - 1);
    }

    #[test]
    fn enough_summary_ticks_transition_to_exiting() {
        let mut app = TuiApp::for_transfer(vec!["r".into()]);
        app.handle_send_event(&SendEvent::AllDone);
        for _ in 0..SUMMARY_TIMEOUT_OK_SECS {
            app.handle_send_event(&SendEvent::SummaryTick);
        }
        assert_eq!(app.state, TuiState::Exiting);
    }

    #[test]
    fn summary_tick_outside_summary_is_ignored() {
        let mut app = TuiApp::for_transfer(vec!["r".into()]);
        assert_eq!(app.state, TuiState::Transferring);
        app.handle_send_event(&SendEvent::SummaryTick);
        assert_eq!(app.state, TuiState::Transferring);
    }

    // ── Issue 05: Cancellation tokens ─────────────────────────────────────────

    #[test]
    fn skip_key_cancels_focused_remote_token() {
        let mut app = TuiApp::for_transfer(vec!["primary".into(), "secondary".into()]);
        app.handle_send_event(&SendEvent::RemoteStarted { remote: "primary".into(), total_bytes: 100 });
        app.handle_send_event(&SendEvent::RemoteStarted { remote: "secondary".into(), total_bytes: 100 });
        let tokens = app.cancel_map();
        app.focused = 0;
        app.handle_key(TuiKey::Skip);
        assert!(tokens["primary"].is_cancelled(), "primary token cancelled after Skip on primary");
        assert!(!tokens["secondary"].is_cancelled(), "secondary token untouched");
    }

    #[test]
    fn quit_key_cancels_all_tokens() {
        let mut app = TuiApp::for_transfer(vec!["primary".into(), "secondary".into()]);
        app.handle_send_event(&SendEvent::RemoteStarted { remote: "primary".into(), total_bytes: 100 });
        app.handle_send_event(&SendEvent::RemoteStarted { remote: "secondary".into(), total_bytes: 100 });
        let tokens = app.cancel_map();
        app.handle_key(TuiKey::Quit);
        assert!(tokens["primary"].is_cancelled(), "primary token cancelled after Quit");
        assert!(tokens["secondary"].is_cancelled(), "secondary token cancelled after Quit");
    }

    #[test]
    fn skip_with_single_remote_cancels_its_token() {
        let mut app = TuiApp::for_transfer(vec!["primary".into()]);
        app.handle_send_event(&SendEvent::RemoteStarted { remote: "primary".into(), total_bytes: 100 });
        let tokens = app.cancel_map();
        app.handle_key(TuiKey::Skip);
        assert!(tokens["primary"].is_cancelled(), "single-remote Skip cancels its token");
    }

    #[test]
    fn summary_countdown_is_ok_timeout_when_only_up_to_date() {
        let mut app = TuiApp::for_transfer(vec!["r".into()]);
        app.handle_send_event(&SendEvent::RemoteUpToDate { remote: "r".into() });
        app.handle_send_event(&SendEvent::AllDone);
        assert_eq!(app.state, TuiState::Summary);
        assert_eq!(app.summary_countdown, SUMMARY_TIMEOUT_OK_SECS);
    }

    #[test]
    fn summary_title_is_complete_when_all_up_to_date() {
        let mut app = TuiApp::for_transfer(vec!["r".into()]);
        app.handle_send_event(&SendEvent::RemoteUpToDate { remote: "r".into() });
        app.handle_send_event(&SendEvent::AllDone);
        assert_eq!(summary_title(&app), " Backup Complete ");
    }

    #[test]
    fn summary_title_is_finished_when_skipped_not_up_to_date() {
        let mut app = TuiApp::for_transfer(vec!["r".into()]);
        app.handle_send_event(&SendEvent::RemoteSkipped { remote: "r".into() });
        app.handle_send_event(&SendEvent::AllDone);
        assert_eq!(summary_title(&app), " Backup Finished ");
    }
}
