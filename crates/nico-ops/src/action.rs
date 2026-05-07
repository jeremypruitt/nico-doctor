use std::time::Instant;

use crate::model::LayerSnapshot;

/// Direction for focus navigation across the scorecard grid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    Left,
    Right,
    Up,
    Down,
}

/// All state mutations flow through `App::handle(Action)`. There is no
/// other mutator. (See ADR-010, ADR-012.)
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// `R` — kick off a refresh round.
    Refresh,
    /// Move the focused scorecard.
    Focus(Dir),
    /// `Enter` — open the detail overlay for the focused scorecard.
    OpenDetail,
    /// `?` — open the keybinds overlay.
    OpenHelp,
    /// `Esc`, `Enter` (when overlay is open), or repeat-toggle of the
    /// overlay key — dismiss any open overlay.
    CloseOverlay,
    /// Terminal resized — repaint.
    Resize,
    /// Snapshots from a completed (or in-progress) refresh round.
    Snapshots(Vec<LayerSnapshot>),
    /// `space` — pause/resume the auto-refresh timer. Manual `R` always
    /// works regardless of pause state.
    TogglePause,
    /// Periodic clock tick from the host loop. The reducer compares
    /// `now` against the next-refresh deadline and may emit
    /// `Effect::StartRefresh`. Throbber animation is also driven by
    /// the timestamp on this action.
    Tick(Instant),
    /// `m` — toggle between Layout A (6-up scorecard) and Layout B
    /// (Mission Control 2×3 grid). Issue #155.
    ToggleLayout,
    /// `Enter` while in Layout B — zoom the focused quadrant
    /// full-screen. (In Layout A, `Enter` opens the detail overlay
    /// instead — see [`Action::OpenDetail`].)
    ZoomQuadrant,
    /// New namespace-scoped events for Layout B's Activity quadrant.
    /// Sourced from `nico_correlate::recent_namespace_events`.
    NamespaceEvents(Vec<nico_correlate::Event>),
    /// `q` / `Ctrl-C` — exit cleanly.
    Quit,
}
