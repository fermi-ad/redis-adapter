use crate::app::{App, DrillDownTab};
use crate::data::DataKind;
use crate::redis_client::SharedState;
use ratatui::prelude::*;
use ratatui::widgets::*;

pub fn render(f: &mut Frame, area: Rect, app: &mut App, snap: &SharedState) {
    if snap.devices.get(app.selected_device).is_none() {
        let msg = Paragraph::new("No device selected")
            .block(Block::default().borders(Borders::ALL))
            .alignment(Alignment::Center);
        f.render_widget(msg, area);
        return;
    }

    match app.drilldown_tab {
        DrillDownTab::Waveforms => render_waveforms(f, area, app, snap),
        DrillDownTab::Fft => render_fft(f, area, app, snap),
        DrillDownTab::Settings => render_settings(f, area, app, snap),
    }
}

fn render_waveforms(f: &mut Frame, area: Rect, app: &App, snap: &SharedState) {
    let dev = match snap.devices.get(app.selected_device) {
        Some(d) => d,
        None => return,
    };
    let data = snap.device_data.get(&dev.name);

    let wf_channels: Vec<_> = dev
        .channels
        .iter()
        .filter(|c| c.kind == DataKind::Waveform)
        .collect();

    if wf_channels.is_empty() {
        let msg = Paragraph::new("No waveform channels")
            .block(Block::default().borders(Borders::ALL).title(" Waveforms "))
            .alignment(Alignment::Center);
        f.render_widget(msg, area);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(22), Constraint::Min(0)])
        .split(area);

    // Channel list
    let items: Vec<ListItem> = wf_channels
        .iter()
        .enumerate()
        .map(|(i, ch)| {
            let style = if i == app.selected_channel {
                Style::default().fg(Color::Yellow).bold()
            } else {
                Style::default()
            };
            ListItem::new(format!(" {} ", ch.label)).style(style)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Channels "),
    );
    f.render_widget(list, chunks[0]);

    // Waveform plot
    let selected_ch = wf_channels.get(app.selected_channel);
    if let Some(ch) = selected_ch {
        if let Some(dev_data) = data {
            if let Some(wf) = dev_data.waveforms.get(&ch.key) {
                let plot_data = downsample(wf, area.width as usize * 2);
                let data_points: Vec<(f64, f64)> = plot_data
                    .iter()
                    .enumerate()
                    .map(|(i, &v)| (i as f64, v))
                    .collect();

                render_chart(
                    f,
                    chunks[1],
                    &data_points,
                    &format!(" {} [{}] ({} samples) ", ch.label, ch.key, wf.len()),
                    &ch.unit,
                    Color::Green,
                );
            } else {
                let msg = Paragraph::new("No data")
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title(format!(" {} ", ch.label)),
                    )
                    .alignment(Alignment::Center);
                f.render_widget(msg, chunks[1]);
            }
        } else {
            let msg = Paragraph::new("Waiting for data...")
                .block(Block::default().borders(Borders::ALL).title(" Waveform "))
                .alignment(Alignment::Center);
            f.render_widget(msg, chunks[1]);
        }
    }
}

fn render_fft(f: &mut Frame, area: Rect, app: &mut App, snap: &SharedState) {
    let dev = match snap.devices.get(app.selected_device) {
        Some(d) => d,
        None => return,
    };
    let data = snap.device_data.get(&dev.name);

    let wf_channels: Vec<_> = dev
        .channels
        .iter()
        .filter(|c| c.kind == DataKind::Waveform)
        .collect();

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(22), Constraint::Min(0)])
        .split(area);

    // Channel list
    let items: Vec<ListItem> = wf_channels
        .iter()
        .enumerate()
        .map(|(i, ch)| {
            let style = if i == app.selected_channel {
                Style::default().fg(Color::Cyan).bold()
            } else {
                Style::default()
            };
            ListItem::new(format!(" {} ", ch.label)).style(style)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Channels "),
    );
    f.render_widget(list, chunks[0]);

    // FFT plot
    let selected_ch = wf_channels.get(app.selected_channel);
    if let Some(ch) = selected_ch {
        if let Some(dev_data) = data {
            if let Some(wf) = dev_data.waveforms.get(&ch.key) {
                // Check if we need to recompute FFT
                let fft_key = format!("{}:{}", dev.name, ch.key);
                if app.fft_source_key != fft_key || app.fft_result.is_empty() {
                    app.fft_source_key = fft_key;
                    app.compute_fft(wf);
                }

                if !app.fft_result.is_empty() {
                    // Skip DC component, show first half
                    let fft_data = &app.fft_result[1..];
                    let plot_data = downsample(fft_data, area.width as usize * 2);
                    let data_points: Vec<(f64, f64)> = plot_data
                        .iter()
                        .enumerate()
                        .map(|(i, &v)| (i as f64, v))
                        .collect();

                    render_chart(
                        f,
                        chunks[1],
                        &data_points,
                        &format!(
                            " FFT: {} ({} bins) ",
                            ch.label,
                            app.fft_result.len()
                        ),
                        "magnitude",
                        Color::Cyan,
                    );
                }
            } else {
                let msg = Paragraph::new("No waveform data for FFT")
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title(" FFT Analysis "),
                    )
                    .alignment(Alignment::Center);
                f.render_widget(msg, chunks[1]);
            }
        }
    }
}

fn render_settings(f: &mut Frame, area: Rect, app: &App, snap: &SharedState) {
    let dev = match snap.devices.get(app.selected_device) {
        Some(d) => d,
        None => return,
    };
    let data = snap.device_data.get(&dev.name);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(8), Constraint::Length(10), Constraint::Min(0)])
        .split(area);

    // Device info
    let info_text = vec![
        Line::from(vec![
            Span::styled("Device: ", Style::default().fg(Color::Gray)),
            Span::styled(&dev.name, Style::default().fg(Color::White).bold()),
        ]),
        Line::from(vec![
            Span::styled("Type: ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{}", dev.device_type),
                Style::default().fg(Color::Yellow),
            ),
        ]),
        Line::from(vec![
            Span::styled("Redis: ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{}:{}", dev.redis_host, dev.redis_port),
                Style::default().fg(Color::Cyan),
            ),
        ]),
        Line::from(vec![
            Span::styled("Data type: ", Style::default().fg(Color::Gray)),
            Span::styled(&dev.data_type, Style::default().fg(Color::White)),
        ]),
        Line::from(vec![
            Span::styled("Channels: ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{}", dev.channels.len()),
                Style::default().fg(Color::White),
            ),
        ]),
    ];

    let info = Paragraph::new(info_text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Device Info "),
    );
    f.render_widget(info, chunks[0]);

    // Controls
    if dev.controls.is_empty() {
        let msg = Paragraph::new("No runtime controls available")
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Controls "),
            )
            .alignment(Alignment::Center);
        f.render_widget(msg, chunks[1]);
    } else {
        let ctrl_items: Vec<ListItem> = dev
            .controls
            .iter()
            .enumerate()
            .map(|(i, ctrl)| {
                let editing = app.editing_control == Some(i);
                let style = if editing {
                    Style::default().fg(Color::Yellow).bold()
                } else {
                    Style::default()
                };
                let val_str = if editing {
                    format!("{}: {}_", ctrl.label, app.edit_buffer)
                } else {
                    format!("{}: {:.4} (Enter to edit)", ctrl.label, ctrl.default_value)
                };
                ListItem::new(val_str).style(style)
            })
            .collect();

        let list = List::new(ctrl_items).block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Controls (Enter to edit, Esc to cancel) "),
        );
        f.render_widget(list, chunks[1]);
    }

    // Channel summary table
    if let Some(dev_data) = data {
        let rows: Vec<Row> = dev
            .channels
            .iter()
            .map(|ch| {
                let kind_str = match ch.kind {
                    DataKind::Waveform => "waveform",
                    DataKind::Scalar => "scalar",
                };
                let value_str = match ch.kind {
                    DataKind::Scalar => match dev_data.scalars.get(&ch.key) {
                        Some(v) => format!("{:.6}", v),
                        None => "---".to_string(),
                    },
                    DataKind::Waveform => match dev_data.waveforms.get(&ch.key) {
                        Some(wf) => {
                            let mean = wf.iter().sum::<f64>() / wf.len() as f64;
                            format!("mean={:.4} ({} pts)", mean, wf.len())
                        }
                        None => "---".to_string(),
                    },
                };

                Row::new(vec![
                    Cell::from(ch.key.clone()),
                    Cell::from(kind_str),
                    Cell::from(ch.label.clone()),
                    Cell::from(value_str),
                ])
            })
            .collect();

        let table = Table::new(
            rows,
            [
                Constraint::Length(20),
                Constraint::Length(10),
                Constraint::Length(20),
                Constraint::Min(20),
            ],
        )
        .header(
            Row::new(vec!["Key", "Type", "Label", "Value"])
                .style(Style::default().fg(Color::Yellow).bold()),
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Channel Data "),
        );

        f.render_widget(table, chunks[2]);
    }
}

fn render_chart(
    f: &mut Frame,
    area: Rect,
    data: &[(f64, f64)],
    title: &str,
    y_label: &str,
    color: Color,
) {
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
                    format!("{:.0}", (x_max + x_min) / 2.0).into(),
                    format!("{:.0}", x_max).into(),
                ]),
        )
        .y_axis(
            Axis::default()
                .title(y_label)
                .style(Style::default().fg(Color::Gray))
                .bounds([y_min - y_margin, y_max + y_margin])
                .labels::<Vec<Line>>(vec![
                    format!("{:.4}", y_min).into(),
                    format!("{:.4}", (y_max + y_min) / 2.0).into(),
                    format!("{:.4}", y_max).into(),
                ]),
        );

    f.render_widget(chart, area);
}

/// Downsample a waveform to at most max_points for plotting
fn downsample(data: &[f64], max_points: usize) -> Vec<f64> {
    if data.len() <= max_points || max_points == 0 {
        return data.to_vec();
    }

    let step = data.len() as f64 / max_points as f64;
    let mut result = Vec::with_capacity(max_points);

    for i in 0..max_points {
        let start = (i as f64 * step) as usize;
        let end = ((i + 1) as f64 * step) as usize;
        let end = end.min(data.len());

        if start < end {
            // Use min-max decimation for better visual fidelity
            let mut min_val = f64::INFINITY;
            let mut max_val = f64::NEG_INFINITY;
            for &v in &data[start..end] {
                min_val = min_val.min(v);
                max_val = max_val.max(v);
            }
            // Alternate min/max to preserve peaks
            if i % 2 == 0 {
                result.push(min_val);
            } else {
                result.push(max_val);
            }
        }
    }

    result
}
