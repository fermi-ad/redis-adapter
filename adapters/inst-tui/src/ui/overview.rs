use crate::app::{App, OverviewTab};
use crate::data::DeviceType;
use crate::redis_client::SharedState;
use ratatui::prelude::*;
use ratatui::widgets::*;

pub fn render(f: &mut Frame, area: Rect, app: &App, snap: &SharedState) {
    match app.overview_tab {
        OverviewTab::BlmLosses => {
            // BLM fills the full area (no device list sidebar)
            render_blm_losses(f, area, app, snap);
        }
        _ => {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Min(0), Constraint::Length(24)])
                .split(area);

            match app.overview_tab {
                OverviewTab::BpmOrbit => render_bpm_orbit(f, chunks[0], app, snap),
                OverviewTab::BcmCurrents => render_bcm_currents(f, chunks[0], app, snap),
                _ => unreachable!(),
            }

            render_device_list(f, chunks[1], app, snap);
        }
    }
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

/// Compute bar_width and bar_gap for n items in inner_w columns
fn compute_bar_layout(n: u16, inner_w: u16) -> (u16, u16) {
    if n == 0 {
        return (1, 0);
    }
    let bar_width = (inner_w / n).max(1);
    let bar_gap = if bar_width > 2 { 1 } else { 0 };
    let bar_width = if bar_gap > 0 {
        ((inner_w + bar_gap) / n).saturating_sub(bar_gap).max(1)
    } else {
        bar_width
    };
    (bar_width, bar_gap)
}

/// Get the x center pixel for bar i given layout
fn bar_center_x(i: u16, bar_width: u16, bar_gap: u16, offset_x: u16) -> u16 {
    offset_x + i * (bar_width + bar_gap) + bar_width / 2
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
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ])
        .split(area);

    let bpm_labels: Vec<String> = orbit
        .iter()
        .map(|(name, _, _, _, _)| {
            name.trim_start_matches(|c: char| !c.is_ascii_digit())
                .trim_start_matches('0')
                .to_string()
        })
        .collect();

    let h_vals: Vec<f64> = orbit.iter().map(|(_, h, _, _, _)| *h).collect();
    let v_vals: Vec<f64> = orbit.iter().map(|(_, _, v, _, _)| *v).collect();
    let hi_vals: Vec<f64> = orbit.iter().map(|(_, _, _, hi, _)| *hi).collect();
    let vi_vals: Vec<f64> = orbit.iter().map(|(_, _, _, _, vi)| *vi).collect();

    // H Position line plot aligned to bars
    render_aligned_line_plot(f, chunks[0], &h_vals, &bpm_labels, " H Position (mm) ", Color::Green);

    // H Intensity bar chart
    let h_int_bars: Vec<Bar> = hi_vals
        .iter()
        .enumerate()
        .map(|(i, v)| {
            Bar::default()
                .value((v.abs() * 1e4).min(10000.0) as u64)
                .label(Line::from(bpm_labels[i].clone()))
                .style(Style::default().fg(Color::Green))
        })
        .collect();
    render_bar_chart_dynamic(f, chunks[1], &h_int_bars, " H Intensity ", Color::Green);

    // V Position line plot aligned to bars
    render_aligned_line_plot(f, chunks[2], &v_vals, &bpm_labels, " V Position (mm) ", Color::Cyan);

    // V Intensity bar chart
    let v_int_bars: Vec<Bar> = vi_vals
        .iter()
        .enumerate()
        .map(|(i, v)| {
            Bar::default()
                .value((v.abs() * 1e4).min(10000.0) as u64)
                .label(Line::from(bpm_labels[i].clone()))
                .style(Style::default().fg(Color::Cyan))
        })
        .collect();
    render_bar_chart_dynamic(f, chunks[3], &v_int_bars, " V Intensity ", Color::Cyan);
}

/// Render a line plot with points aligned to bar chart x positions
fn render_aligned_line_plot(
    f: &mut Frame,
    area: Rect,
    values: &[f64],
    labels: &[String],
    title: &str,
    color: Color,
) {
    if values.is_empty() {
        let msg = Paragraph::new("No data")
            .block(Block::default().borders(Borders::ALL).title(title))
            .alignment(Alignment::Center);
        f.render_widget(msg, area);
        return;
    }

    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.width < 4 || inner.height < 3 {
        return;
    }

    let n = values.len() as u16;
    // Label row at bottom
    let chart_h = inner.height.saturating_sub(1);
    let label_y = inner.y + chart_h;

    // Use same bar layout as bar chart
    let (bar_width, bar_gap) = compute_bar_layout(n, inner.width);

    let y_min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let y_max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let y_margin = (y_max - y_min).abs() * 0.1 + 1e-10;
    let y_lo = y_min - y_margin;
    let y_hi = y_max + y_margin;

    let buf = f.buffer_mut();

    // Compute pixel positions for each point
    let mut points: Vec<(u16, u16)> = Vec::with_capacity(values.len());
    for (i, &val) in values.iter().enumerate() {
        let cx = bar_center_x(i as u16, bar_width, bar_gap, inner.x);
        let py_frac = (val - y_lo) / (y_hi - y_lo);
        let py = inner.y + chart_h.saturating_sub(1)
            - ((py_frac * (chart_h.saturating_sub(1)) as f64).round() as u16)
                .min(chart_h.saturating_sub(1));
        points.push((cx, py));
    }

    // Draw connecting lines between consecutive points
    for w in points.windows(2) {
        let (x0, y0) = w[0];
        let (x1, y1) = w[1];
        draw_line(buf, x0, y0, x1, y1, color, inner);
    }

    // Draw points on top
    for &(px, py) in &points {
        if px < inner.x + inner.width && py >= inner.y && py < inner.y + chart_h {
            buf[(px, py)].set_char('●').set_fg(color);
        }
    }

    // Y-axis labels (left side, 2 labels)
    let y_max_str = format!("{:.2}", y_max);
    let y_min_str = format!("{:.2}", y_min);
    for (ci, ch) in y_max_str.chars().take(6).enumerate() {
        let x = inner.x + ci as u16;
        if x < inner.x + inner.width {
            buf[(x, inner.y)].set_char(ch).set_fg(Color::DarkGray);
        }
    }
    if chart_h > 1 {
        for (ci, ch) in y_min_str.chars().take(6).enumerate() {
            let x = inner.x + ci as u16;
            if x < inner.x + inner.width {
                buf[(x, inner.y + chart_h - 1)].set_char(ch).set_fg(Color::DarkGray);
            }
        }
    }

    // X labels at bottom, aligned to bar centers
    for (i, label) in labels.iter().enumerate() {
        let cx = bar_center_x(i as u16, bar_width, bar_gap, inner.x);
        // Center label under bar
        let lw = label.len() as u16;
        let lx = cx.saturating_sub(lw / 2);
        for (ci, ch) in label.chars().enumerate() {
            let x = lx + ci as u16;
            if x < inner.x + inner.width && label_y < inner.y + inner.height {
                buf[(x, label_y)].set_char(ch).set_fg(Color::Gray);
            }
        }
    }
}

/// Bresenham line drawing
fn draw_line(buf: &mut ratatui::buffer::Buffer, x0: u16, y0: u16, x1: u16, y1: u16, color: Color, bounds: Rect) {
    let dx = (x1 as i32 - x0 as i32).abs();
    let dy = -(y1 as i32 - y0 as i32).abs();
    let sx: i32 = if x0 < x1 { 1 } else { -1 };
    let sy: i32 = if y0 < y1 { 1 } else { -1 };
    let mut err = dx + dy;
    let mut cx = x0 as i32;
    let mut cy = y0 as i32;

    loop {
        if cx >= bounds.x as i32
            && cx < (bounds.x + bounds.width) as i32
            && cy >= bounds.y as i32
            && cy < (bounds.y + bounds.height.saturating_sub(1)) as i32
        {
            let cell = &mut buf[(cx as u16, cy as u16)];
            if cell.symbol() == " " {
                cell.set_char('·').set_fg(color);
            }
        }
        if cx == x1 as i32 && cy == y1 as i32 {
            break;
        }
        let e2 = 2 * err;
        if e2 >= dy {
            err += dy;
            cx += sx;
        }
        if e2 <= dx {
            err += dx;
            cy += sy;
        }
    }
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

    let max_loss = losses
        .iter()
        .map(|(_, v)| v.abs())
        .fold(0.0_f64, f64::max)
        .max(1e-10);

    // Also consider baseline values for max scale
    let max_val = if let Some(baseline) = &app.blm_baseline {
        let max_base = baseline.values().map(|v| v.abs()).fold(0.0_f64, f64::max);
        max_loss.max(max_base)
    } else {
        max_loss
    };

    let has_baseline = app.blm_baseline.is_some();

    let title = if has_baseline {
        format!(
            " BLM Losses ({}) | max: {:.4e} | baseline active [b] ",
            losses.len(),
            max_loss
        )
    } else {
        format!(
            " BLM Losses ({}) | max: {:.4e} | [b] save baseline ",
            losses.len(),
            max_loss
        )
    };

    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.width == 0 || inner.height < 2 {
        return;
    }

    let n = losses.len() as u16;
    // Reserve 1 row for labels, 1 row for selected info
    let chart_height = inner.height.saturating_sub(2);
    let bar_gap: u16 = 1;
    let bar_width = if n > 0 {
        ((inner.width + bar_gap) / n).saturating_sub(bar_gap).max(1)
    } else {
        1
    };

    let buf = f.buffer_mut();

    for (i, (name, val)) in losses.iter().enumerate() {
        let x_start = inner.x + (i as u16) * (bar_width + bar_gap);
        if x_start >= inner.x + inner.width {
            break;
        }

        let cur_height = ((val.abs() / max_val) * chart_height as f64) as u16;
        let cur_height = cur_height.min(chart_height);

        let (base_height, has_base) = if let Some(baseline) = &app.blm_baseline {
            if let Some(&base_val) = baseline.get(name) {
                let bh = ((base_val.abs() / max_val) * chart_height as f64) as u16;
                (bh.min(chart_height), true)
            } else {
                (0, false)
            }
        } else {
            (0, false)
        };

        // Draw the bar column by column
        for dx in 0..bar_width {
            let x = x_start + dx;
            if x >= inner.x + inner.width {
                break;
            }

            for row in 0..chart_height {
                let y = inner.y + chart_height - 1 - row;
                if row < cur_height {
                    if has_base {
                        if cur_height > base_height && row >= base_height {
                            // Worse: red tip above baseline
                            buf[(x, y)].set_char('█').set_fg(Color::Red);
                        } else if cur_height < base_height && row >= cur_height {
                            // This won't render (row < cur_height check above)
                            // but the yellow portion below is shorter = green improvement
                            buf[(x, y)].set_char('█').set_fg(Color::Yellow);
                        } else {
                            buf[(x, y)].set_char('█').set_fg(Color::Yellow);
                        }
                    } else {
                        buf[(x, y)].set_char('█').set_fg(Color::Yellow);
                    }
                }
                // If baseline was higher, paint the gap green (improvement)
                if has_base && cur_height < base_height && row >= cur_height && row < base_height {
                    buf[(x, y)].set_char('▒').set_fg(Color::Green);
                }
            }
        }

        // Label row (strip leading zeros)
        let label = name
            .trim_start_matches(|c: char| !c.is_ascii_digit())
            .trim_start_matches('0');
        let label = if label.is_empty() { "0" } else { label };
        let label_y = inner.y + chart_height;
        let is_selected = i == app.device_scroll;
        let label_style = if is_selected {
            Style::default().fg(Color::Yellow).bold()
        } else {
            Style::default().fg(Color::Gray)
        };
        for (ci, ch) in label.chars().enumerate() {
            let x = x_start + ci as u16;
            if x < inner.x + inner.width && ci < bar_width as usize {
                buf[(x, label_y)].set_char(ch).set_style(label_style);
            }
        }
    }

    // Selected BLM info at the bottom
    let info_y = inner.y + chart_height + 1;
    if info_y < inner.y + inner.height {
        if let Some((name, val)) = losses.get(app.device_scroll) {
            let info = if let Some(baseline) = &app.blm_baseline {
                if let Some(&base_val) = baseline.get(name) {
                    let diff = val - base_val;
                    let sign = if diff >= 0.0 { "+" } else { "" };
                    format!(" > {} : {:.4e} (base: {:.4e}, {}{:.4e}) ", name, val, base_val, sign, diff)
                } else {
                    format!(" > {} : {:.4e} ", name, val)
                }
            } else {
                format!(" > {} : {:.4e} ", name, val)
            };
            let style = Style::default().fg(Color::Yellow).bold();
            for (ci, ch) in info.chars().enumerate() {
                let x = inner.x + ci as u16;
                if x < inner.x + inner.width {
                    buf[(x, info_y)].set_char(ch).set_style(style);
                }
            }
        }
    }
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

    // Single overview bar chart: one bar per BCM showing sum of gated currents
    let bars: Vec<Bar> = currents
        .iter()
        .map(|(name, ch_vals)| {
            let total: f64 = ch_vals.iter().map(|(_, v)| v.abs()).sum();
            let label = name
                .strip_prefix("BCM-")
                .unwrap_or(name);
            Bar::default()
                .value((total * 1e4) as u64) // scale for visibility
                .label(Line::from(label.to_string()))
                .style(Style::default().fg(Color::Magenta))
        })
        .collect();

    let max_current: f64 = currents
        .iter()
        .map(|(_, ch_vals)| ch_vals.iter().map(|(_, v)| v.abs()).sum::<f64>())
        .fold(0.0_f64, f64::max)
        .max(1e-10);

    let n = bars.len() as u16;
    let inner_w = area.width.saturating_sub(2);
    let bar_width = if n > 0 { ((inner_w + 1) / n).saturating_sub(1).max(1) } else { 1 };
    let bar_gap = if bar_width > 1 { 1 } else { 0 };

    let bar_chart = BarChart::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(
                    " BCM Currents ({} monitors) | max: {:.4e} ",
                    currents.len(),
                    max_current
                )),
        )
        .data(BarGroup::default().bars(&bars))
        .bar_width(bar_width)
        .bar_gap(bar_gap)
        .value_style(Style::default().fg(Color::Magenta).bold())
        .label_style(Style::default().fg(Color::Gray));

    f.render_widget(bar_chart, area);
}

fn render_bar_chart_dynamic(f: &mut Frame, area: Rect, bars: &[Bar], title: &str, color: Color) {
    let n = bars.len() as u16;
    let inner_w = area.width.saturating_sub(2);
    let (bar_width, bar_gap) = compute_bar_layout(n, inner_w);

    let bar_chart = BarChart::default()
        .block(Block::default().borders(Borders::ALL).title(title))
        .data(BarGroup::default().bars(bars))
        .bar_width(bar_width)
        .bar_gap(bar_gap)
        .value_style(Style::default().fg(color).bold())
        .label_style(Style::default().fg(Color::Gray));

    f.render_widget(bar_chart, area);
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
