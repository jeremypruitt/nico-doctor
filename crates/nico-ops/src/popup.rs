//! Popup/overlay primitive — the shared render contract for every modal
//! overlay in `nico ops`.
//!
//! Per PRD-006 Slice 3 (issue #369): a single chrome contract for every
//! popup so we never reinvent centered-Clear-Block plumbing. Each overlay
//! supplies a [`PopupSpec`] describing its title, pre-rendered body lines,
//! footer lines (keymap hints), and a [`PopupSize`]; the primitive handles
//! centering, the [`Clear`] wipe, the bordered [`Block`], the body
//! `Paragraph` (with vertical scroll), and the footer band.
//!
//! Modal-stack semantics (only one popup open at a time, underlying view
//! does not receive keys while a popup is up) live in `events::translate`
//! and the `App` reducer's guards on `Overlay`. The primitive itself is a
//! pure render helper.
//!
//! See PRD-006 §"Popup/overlay primitive" and the audit table in
//! the slice's PR for the three ported overlays (Help, Detail,
//! Correlate).

use nico_common::theme::Theme;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::Style,
    text::Line,
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};

/// Sizing band for a popup. Maps to a `(width%, height%)` of the available
/// viewport. Three bands cover every overlay in the dashboard today:
/// help-style (small, info-dense), correlate-style (medium, timeline),
/// detail-style (large, scrollable findings list).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopupSize {
    /// ~60% × 50% — help/keybinds overlay.
    Small,
    /// ~80% × 70% — correlate popover.
    Medium,
    /// ~80% × 80% — detail overlay.
    Large,
}

impl PopupSize {
    /// `(width%, height%)` percentages of the available viewport.
    pub fn percentages(self) -> (u16, u16) {
        match self {
            PopupSize::Small => (60, 50),
            PopupSize::Medium => (80, 70),
            PopupSize::Large => (80, 80),
        }
    }
}

/// One popup's render spec. Body and footer come in as pre-built `Line`s
/// so callers can use whatever styling they want; the primitive only
/// arranges the chrome.
pub struct PopupSpec<'a> {
    pub title: String,
    pub body: Vec<Line<'a>>,
    /// Footer lines — typically one line of keymap hints. Empty footer
    /// means no footer band is reserved.
    pub footer: Vec<Line<'a>>,
    pub size: PopupSize,
    /// Body vertical scroll offset (rows). Caller-owned state.
    pub scroll: u16,
}

/// Render `spec` centered in `area`. Wipes the popup rect with [`Clear`]
/// so the underlying view doesn't bleed through, paints the bordered
/// title block, then lays out body + footer inside.
pub fn render_popup(spec: &PopupSpec<'_>, theme: &Theme, frame: &mut Frame, area: Rect) {
    let (pct_x, pct_y) = spec.size.percentages();
    let popup_area = centered(area, pct_x, pct_y);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(spec.title.clone())
        .style(Style::default().bg(theme.overlay_bg).fg(theme.overlay_fg));
    let inner = block.inner(popup_area);
    frame.render_widget(Clear, popup_area);
    frame.render_widget(block, popup_area);

    let footer_height = if spec.footer.is_empty() {
        0
    } else {
        spec.footer.len() as u16
    };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(footer_height)])
        .split(inner);

    let body_area = chunks[0].inner(Margin {
        horizontal: 1,
        vertical: 0,
    });
    frame.render_widget(
        Paragraph::new(spec.body.clone())
            .wrap(Wrap { trim: false })
            .scroll((spec.scroll, 0)),
        body_area,
    );

    if footer_height > 0 {
        let footer_area = chunks[1].inner(Margin {
            horizontal: 1,
            vertical: 0,
        });
        frame.render_widget(Paragraph::new(spec.footer.clone()), footer_area);
    }
}

fn centered(area: Rect, pct_x: u16, pct_y: u16) -> Rect {
    let h = (area.width * pct_x) / 100;
    let v = (area.height * pct_y) / 100;
    let x = area.x + (area.width.saturating_sub(h)) / 2;
    let y = area.y + (area.height.saturating_sub(v)) / 2;
    Rect {
        x,
        y,
        width: h,
        height: v,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nico_common::theme::DEFAULT;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::text::Span;

    fn render_to_string(spec: &PopupSpec<'_>, w: u16, h: u16) -> String {
        let backend = TestBackend::new(w, h);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| render_popup(spec, &DEFAULT, f, f.area()))
            .unwrap();
        let buf = terminal.backend().buffer().clone();
        let mut out = String::new();
        for y in 0..buf.area.height {
            for x in 0..buf.area.width {
                out.push_str(buf.cell((x, y)).unwrap().symbol());
            }
            out.push('\n');
        }
        out
    }

    fn spec_with(
        title: &str,
        body: Vec<Line<'static>>,
        footer: Vec<Line<'static>>,
    ) -> PopupSpec<'static> {
        PopupSpec {
            title: title.to_string(),
            body,
            footer,
            size: PopupSize::Medium,
            scroll: 0,
        }
    }

    #[test]
    fn renders_title_in_top_border() {
        let spec = spec_with(" hello ", vec![], vec![]);
        let s = render_to_string(&spec, 60, 20);
        assert!(s.contains("hello"), "title missing:\n{s}");
    }

    #[test]
    fn renders_body_lines_inside_block() {
        let spec = spec_with(
            " t ",
            vec![Line::from("FIRST"), Line::from("SECOND")],
            vec![],
        );
        let s = render_to_string(&spec, 60, 20);
        assert!(s.contains("FIRST"), "body line 1 missing:\n{s}");
        assert!(s.contains("SECOND"), "body line 2 missing:\n{s}");
    }

    #[test]
    fn renders_footer_lines_at_bottom() {
        let spec = spec_with(
            " t ",
            vec![Line::from("body")],
            vec![Line::from("[esc] close")],
        );
        let s = render_to_string(&spec, 60, 20);
        // Footer must be lower in the buffer than the body line.
        let body_row = s.lines().position(|l| l.contains("body")).expect("body");
        let footer_row = s
            .lines()
            .position(|l| l.contains("[esc] close"))
            .expect("footer");
        assert!(
            footer_row > body_row,
            "footer should sit below body — body@{body_row} footer@{footer_row}:\n{s}"
        );
    }

    #[test]
    fn body_scroll_offsets_visible_rows() {
        let body: Vec<Line<'static>> = (0..30).map(|i| Line::from(format!("row{i:02}"))).collect();
        let mut spec = spec_with(" t ", body, vec![]);
        spec.scroll = 10;
        let s = render_to_string(&spec, 60, 20);
        assert!(
            !s.contains("row00"),
            "scrolled-past row should be hidden:\n{s}"
        );
        assert!(s.contains("row10"), "scroll target row missing:\n{s}");
    }

    #[test]
    fn small_popup_is_narrower_than_large() {
        // Compare the rendered border position for Small vs Large at the
        // same viewport. Small (60% × 50%) must occupy fewer columns than
        // Large (80% × 80%).
        let body = vec![Line::from(Span::raw("x"))];
        let small = PopupSpec {
            title: " s ".into(),
            body: body.clone(),
            footer: vec![],
            size: PopupSize::Small,
            scroll: 0,
        };
        let large = PopupSpec {
            title: " l ".into(),
            body,
            footer: vec![],
            size: PopupSize::Large,
            scroll: 0,
        };
        let s_small = render_to_string(&small, 120, 40);
        let s_large = render_to_string(&large, 120, 40);
        // Count the row that contains the most border chars (`─`).
        let small_border_width = s_small
            .lines()
            .map(|l| l.chars().filter(|c| *c == '─').count())
            .max()
            .unwrap_or(0);
        let large_border_width = s_large
            .lines()
            .map(|l| l.chars().filter(|c| *c == '─').count())
            .max()
            .unwrap_or(0);
        assert!(
            small_border_width < large_border_width,
            "Small popup ({small_border_width} cols of border) should be \
             narrower than Large ({large_border_width})"
        );
    }

    #[test]
    fn percentages_scale_with_size() {
        assert!(PopupSize::Small.percentages().0 < PopupSize::Medium.percentages().0);
        assert!(PopupSize::Medium.percentages().1 < PopupSize::Large.percentages().1);
    }
}
