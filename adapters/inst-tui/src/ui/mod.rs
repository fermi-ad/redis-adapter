mod overview;
mod drilldown;

use crate::app::{App, Screen};
use crate::redis_client::SharedState;
use ratatui::prelude::*;
use ratatui::widgets::*;

pub fn render(f: &mut Frame, app: &mut App) {
    let snap = app.snapshot();
    app.clamp_selection(&snap);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // title bar
            Constraint::Min(0),    // main content
            Constraint::Length(1), // status bar
        ])
        .split(f.area());

    render_title_bar(f, chunks[0], app);

    match app.screen {
        Screen::Overview => overview::render(f, chunks[1], app, &snap),
        Screen::DrillDown => drilldown::render(f, chunks[1], app, &snap),
    }

    render_status_bar(f, chunks[2], app, &snap);
}

fn render_title_bar(f: &mut Frame, area: Rect, app: &App) {
    let tabs = match app.screen {
        Screen::Overview => {
            let titles: Vec<Line> = crate::app::OverviewTab::all()
                .iter()
                .map(|t| Line::from(t.label()))
                .collect();
            let selected = crate::app::OverviewTab::all()
                .iter()
                .position(|t| *t == app.overview_tab)
                .unwrap_or(0);
            Tabs::new(titles)
                .select(selected)
                .highlight_style(Style::default().fg(Color::Yellow).bold())
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(" inst-tui "),
                )
        }
        Screen::DrillDown => {
            let snap = app.snapshot();
            let dev_name = snap
                .devices
                .get(app.selected_device)
                .map(|d| d.name.as_str())
                .unwrap_or("?");

            let titles: Vec<Line> = crate::app::DrillDownTab::all()
                .iter()
                .map(|t| Line::from(t.label()))
                .collect();
            let selected = crate::app::DrillDownTab::all()
                .iter()
                .position(|t| *t == app.drilldown_tab)
                .unwrap_or(0);
            Tabs::new(titles)
                .select(selected)
                .highlight_style(Style::default().fg(Color::Cyan).bold())
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(format!(" {} ", dev_name)),
                )
        }
    };

    f.render_widget(tabs, area);
}

fn render_status_bar(f: &mut Frame, area: Rect, app: &App, snap: &SharedState) {
    let device_count = snap.devices.len();
    let bpm_count = snap
        .devices
        .iter()
        .filter(|d| d.device_type == crate::data::DeviceType::Bpm)
        .count();
    let blm_count = snap
        .devices
        .iter()
        .filter(|d| d.device_type == crate::data::DeviceType::Blm)
        .count();
    let bcm_count = snap
        .devices
        .iter()
        .filter(|d| d.device_type == crate::data::DeviceType::Bcm)
        .count();

    let status = match app.screen {
        Screen::Overview => format!(
            " {} devices ({} BPM, {} BLM, {} BCM) | Tab: switch | Enter: drill down | q: quit",
            device_count, bpm_count, blm_count, bcm_count
        ),
        Screen::DrillDown => {
            " Esc: back | Tab: switch tab | Left/Right: channel | 1/2/3: tab | q: quit"
                .to_string()
        }
    };

    let bar = Paragraph::new(status).style(Style::default().fg(Color::White).bg(Color::DarkGray));
    f.render_widget(bar, area);
}
