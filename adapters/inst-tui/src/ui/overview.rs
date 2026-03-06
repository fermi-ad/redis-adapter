use crate::app::{App, OverviewTab};
use crate::data::DeviceType;
use crate::redis_client::SharedState;
use ratatui::prelude::*;
use ratatui::widgets::*;

pub fn render(f: &mut Frame, area: Rect, app: &App, snap: &SharedState) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(0), Constraint::Length(24)])
        .split(area);

    match app.overview_tab {
        OverviewTab::BpmOrbit => render_bpm_orbit(f, chunks[0], app, snap),
        OverviewTab::BlmLosses => render_blm_losses(f, chunks[0], app, snap),
        OverviewTab::BcmCurrents => render_bcm_currents(f, chunks[0], app, snap),
    }

    render_device_list(f, chunks[1], app, snap);
}

fn render_device_list(f: &mut Frame, area: Rect, app: &App, snap: &SharedState) {
    let dtype = match app.overview_tab {
        OverviewTab::BpmOrbit => DeviceType::Bpm,
        OverviewTab::BlmLosses => DeviceType::Blm,
        OverviewTab::BcmCurrents => DeviceType::Bcm,
    };

    let filtered = app.filtered_devices(snap);
    let items: Vec<ListItem> = filtered
        .iter()
        .enumerate()
        .map(|(i, (_, d))| {
            let style = if i == app.device_scroll {
                Style::default().fg(Color::Yellow).bold()
            } else {
                Style::default()
            };
            ListItem::new(d.name.clone()).style(style)
        })
        .collect();

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" {} Devices ({}) ", dtype, filtered.len())),
        )
        .highlight_style(Style::default().fg(Color::Yellow));

    f.render_widget(list, area);
}

fn render_bpm_orbit(f: &mut Frame, area: Rect, app: &App, snap: &SharedState) {
    let orbit = app.bpm_orbit_data(snap);

    if orbit.is_empty() {
        let msg = Paragraph::new("No BPM data available. Waiting for devices...")
            .block(Block::default().borders(Borders::ALL).title(" BPM Orbit "))
            .alignment(Alignment::Center);
        f.render_widget(msg, area);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(33),
            Constraint::Percentage(33),
            Constraint::Percentage(34),
        ])
        .split(area);

    // Horizontal position vs index
    let h_data: Vec<(f64, f64)> = orbit
        .iter()
        .enumerate()
        .map(|(i, (_, h, _, _, _))| (i as f64, *h))
        .collect();
    render_line_chart(f, chunks[0], &h_data, " H Position (mm) ", Color::Green);

    // Vertical position vs index
    let v_data: Vec<(f64, f64)> = orbit
        .iter()
        .enumerate()
        .map(|(i, (_, _, v, _, _))| (i as f64, *v))
        .collect();
    render_line_chart(f, chunks[1], &v_data, " V Position (mm) ", Color::Cyan);

    // Intensity vs index
    let int_data: Vec<(f64, f64)> = orbit
        .iter()
        .enumerate()
        .map(|(i, (_, _, _, hi, vi))| (i as f64, (hi + vi) / 2.0))
        .collect();
    render_line_chart(f, chunks[2], &int_data, " Intensity ", Color::Yellow);
}

fn render_blm_losses(f: &mut Frame, area: Rect, app: &App, snap: &SharedState) {
    let losses = app.blm_loss_data(snap);

    if losses.is_empty() {
        let msg = Paragraph::new("No BLM data available. Waiting for devices...")
            .block(Block::default().borders(Borders::ALL).title(" BLM Losses "))
            .alignment(Alignment::Center);
        f.render_widget(msg, area);
        return;
    }

    // Bar chart showing loss per BLM
    let max_loss = losses
        .iter()
        .map(|(_, v)| *v)
        .fold(0.0_f64, f64::max)
        .max(1e-10);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(3)])
        .split(area);

    // Render losses as horizontal bars using a custom approach
    let bar_values: Vec<u64> = losses
        .iter()
        .map(|(_, v)| (v.abs() / max_loss * 100.0) as u64)
        .collect();

    let sparkline = Sparkline::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" BLM Losses ({} monitors) ", losses.len())),
        )
        .data(&bar_values)
        .style(Style::default().fg(Color::Red))
        .bar_set(symbols::bar::NINE_LEVELS);
    f.render_widget(sparkline, chunks[0]);

    // Legend showing scale
    let legend = Paragraph::new(format!(
        " Max loss: {:.4e} | {} BLMs total",
        max_loss,
        losses.len()
    ))
    .style(Style::default().fg(Color::DarkGray));
    f.render_widget(legend, chunks[1]);
}

fn render_bcm_currents(f: &mut Frame, area: Rect, app: &App, snap: &SharedState) {
    let currents = app.bcm_current_data(snap);

    if currents.is_empty() {
        let msg = Paragraph::new("No BCM data available. Waiting for devices...")
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" BCM Currents "),
            )
            .alignment(Alignment::Center);
        f.render_widget(msg, area);
        return;
    }

    // Show each BCM's gated currents
    let n_bcm = currents.len();
    let constraints: Vec<Constraint> = (0..n_bcm)
        .map(|_| Constraint::Ratio(1, n_bcm as u32))
        .collect();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);

    for (i, (name, ch_vals)) in currents.iter().enumerate() {
        if i >= chunks.len() {
            break;
        }

        let data: Vec<(f64, f64)> = ch_vals
            .iter()
            .enumerate()
            .map(|(j, (_, v))| (j as f64, *v))
            .collect();

        render_line_chart(
            f,
            chunks[i],
            &data,
            &format!(" {} Gated Current ", name),
            Color::Magenta,
        );
    }
}

fn render_line_chart(f: &mut Frame, area: Rect, data: &[(f64, f64)], title: &str, color: Color) {
    if data.is_empty() {
        let msg = Paragraph::new("No data")
            .block(Block::default().borders(Borders::ALL).title(title))
            .alignment(Alignment::Center);
        f.render_widget(msg, area);
        return;
    }

    let x_min = data.iter().map(|&(x, _)| x).fold(f64::INFINITY, f64::min);
    let x_max = data
        .iter()
        .map(|&(x, _)| x)
        .fold(f64::NEG_INFINITY, f64::max);
    let y_min = data.iter().map(|&(_, y)| y).fold(f64::INFINITY, f64::min);
    let y_max = data
        .iter()
        .map(|&(_, y)| y)
        .fold(f64::NEG_INFINITY, f64::max);

    let y_margin = (y_max - y_min).abs() * 0.1 + 1e-10;

    let dataset = Dataset::default()
        .marker(symbols::Marker::Braille)
        .graph_type(GraphType::Line)
        .style(Style::default().fg(color))
        .data(data);

    let chart = Chart::new(vec![dataset])
        .block(Block::default().borders(Borders::ALL).title(title))
        .x_axis(
            Axis::default()
                .style(Style::default().fg(Color::Gray))
                .bounds([x_min, x_max.max(x_min + 1.0)])
                .labels::<Vec<Line>>(vec![
                    format!("{:.0}", x_min).into(),
                    format!("{:.0}", x_max).into(),
                ]),
        )
        .y_axis(
            Axis::default()
                .style(Style::default().fg(Color::Gray))
                .bounds([y_min - y_margin, y_max + y_margin])
                .labels::<Vec<Line>>(vec![
                    format!("{:.3}", y_min).into(),
                    format!("{:.3}", y_max).into(),
                ]),
        );

    f.render_widget(chart, area);
}
