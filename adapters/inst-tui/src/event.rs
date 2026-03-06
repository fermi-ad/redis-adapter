use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use std::time::Duration;

pub enum AppEvent {
    Key(KeyEvent),
    Tick,
}

pub fn poll_event(tick_rate: Duration) -> Option<AppEvent> {
    if event::poll(tick_rate).unwrap_or(false) {
        if let Ok(Event::Key(key)) = event::read() {
            return Some(AppEvent::Key(key));
        }
    }
    Some(AppEvent::Tick)
}

use crate::app::{App, DrillDownTab, OverviewTab, Screen};
use crate::data::DataKind;
use crate::redis_client::RedisPool;

pub fn handle_key(app: &mut App, key: KeyEvent) {
    // Quit on q or Ctrl+C
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        app.running = false;
        return;
    }

    // If editing a control value, handle input mode
    if app.editing_control.is_some() {
        handle_edit_key(app, key);
        return;
    }

    match key.code {
        KeyCode::Char('q') => app.running = false,

        // Tab navigation
        KeyCode::Tab => {
            match app.screen {
                Screen::Overview => {
                    app.overview_tab = app.overview_tab.next();
                    app.device_scroll = 0;
                }
                Screen::DrillDown => app.drilldown_tab = app.drilldown_tab.next(),
            }
        }
        KeyCode::BackTab => {
            match app.screen {
                Screen::Overview => {
                    app.overview_tab = app.overview_tab.prev();
                    app.device_scroll = 0;
                }
                Screen::DrillDown => app.drilldown_tab = app.drilldown_tab.prev(),
            }
        }

        // Direct tab selection
        KeyCode::Char('1') => match app.screen {
            Screen::Overview => {
                app.overview_tab = OverviewTab::BpmOrbit;
                app.device_scroll = 0;
            }
            Screen::DrillDown => app.drilldown_tab = DrillDownTab::Waveforms,
        },
        KeyCode::Char('2') => match app.screen {
            Screen::Overview => {
                app.overview_tab = OverviewTab::BlmLosses;
                app.device_scroll = 0;
            }
            Screen::DrillDown => app.drilldown_tab = DrillDownTab::Fft,
        },
        KeyCode::Char('3') => match app.screen {
            Screen::Overview => {
                app.overview_tab = OverviewTab::BcmCurrents;
                app.device_scroll = 0;
            }
            Screen::DrillDown => app.drilldown_tab = DrillDownTab::Settings,
        },

        // Device list navigation (uses device_scroll for filtered list position)
        KeyCode::Up | KeyCode::Char('k') => {
            if app.screen == Screen::DrillDown
                && app.drilldown_tab == DrillDownTab::Waveforms
            {
                app.selected_channel = app.selected_channel.saturating_sub(1);
            } else if app.screen == Screen::DrillDown
                && app.drilldown_tab == DrillDownTab::Settings
            {
                // Navigate controls list
            } else {
                app.device_scroll = app.device_scroll.saturating_sub(1);
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            let snap = app.snapshot();
            if app.screen == Screen::DrillDown
                && app.drilldown_tab == DrillDownTab::Waveforms
            {
                if let Some(dev) = snap.devices.get(app.selected_device) {
                    let wf_count = dev
                        .channels
                        .iter()
                        .filter(|c| c.kind == DataKind::Waveform)
                        .count();
                    if app.selected_channel + 1 < wf_count {
                        app.selected_channel += 1;
                    }
                }
            } else {
                let filtered = app.filtered_devices(&snap);
                if !filtered.is_empty() && app.device_scroll + 1 < filtered.len() {
                    app.device_scroll += 1;
                }
            }
        }

        // Enter drilldown
        KeyCode::Enter => {
            if app.screen == Screen::Overview {
                let snap = app.snapshot();
                let filtered = app.filtered_devices(&snap);
                if let Some(&(global_idx, _)) = filtered.get(app.device_scroll) {
                    app.selected_device = global_idx;
                    app.screen = Screen::DrillDown;
                    app.drilldown_tab = DrillDownTab::Waveforms;
                    app.selected_channel = 0;
                }
            } else if app.screen == Screen::DrillDown
                && app.drilldown_tab == DrillDownTab::Settings
            {
                let snap = app.snapshot();
                if let Some(dev) = snap.devices.get(app.selected_device) {
                    if !dev.controls.is_empty() {
                        app.editing_control = Some(0);
                        app.edit_buffer.clear();
                    }
                }
            }
        }

        // Back to overview
        KeyCode::Esc => {
            if app.screen == Screen::DrillDown {
                app.screen = Screen::Overview;
            }
        }

        // Channel selection in waveform/FFT views
        KeyCode::Left => {
            if app.screen == Screen::DrillDown {
                app.selected_channel = app.selected_channel.saturating_sub(1);
            }
        }
        KeyCode::Right => {
            if app.screen == Screen::DrillDown {
                let snap = app.snapshot();
                if let Some(dev) = snap.devices.get(app.selected_device) {
                    let wf_count = dev
                        .channels
                        .iter()
                        .filter(|c| c.kind == DataKind::Waveform)
                        .count();
                    if app.selected_channel + 1 < wf_count {
                        app.selected_channel += 1;
                    }
                }
            }
        }

        _ => {}
    }
}

fn handle_edit_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.editing_control = None;
            app.edit_buffer.clear();
        }
        KeyCode::Enter => {
            if let Ok(val) = app.edit_buffer.parse::<f64>() {
                let snap = app.snapshot();
                if let Some(dev) = snap.devices.get(app.selected_device) {
                    if let Some(ctrl_idx) = app.editing_control {
                        if let Some(ctrl) = dev.controls.get(ctrl_idx) {
                            let full_key = dev.stream_key(&ctrl.key);
                            let mut pool =
                                RedisPool::new(vec![(dev.redis_host.clone(), dev.redis_port)]);
                            pool.write_control(
                                &dev.redis_host,
                                dev.redis_port,
                                &full_key,
                                val,
                            );
                        }
                    }
                }
            }
            app.editing_control = None;
            app.edit_buffer.clear();
        }
        KeyCode::Backspace => {
            app.edit_buffer.pop();
        }
        KeyCode::Char(c) if c.is_ascii_digit() || c == '.' || c == '-' => {
            app.edit_buffer.push(c);
        }
        _ => {}
    }
}
