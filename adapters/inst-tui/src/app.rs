use crate::data::*;
use crate::redis_client::SharedState;
use num_complex::Complex;
use rustfft::FftPlanner;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverviewTab {
    BpmOrbit,
    BlmLosses,
    BcmCurrents,
}

impl OverviewTab {
    pub fn label(&self) -> &str {
        match self {
            OverviewTab::BpmOrbit => "BPM Orbit",
            OverviewTab::BlmLosses => "BLM Losses",
            OverviewTab::BcmCurrents => "BCM Currents",
        }
    }

    pub fn all() -> &'static [OverviewTab] {
        &[
            OverviewTab::BpmOrbit,
            OverviewTab::BlmLosses,
            OverviewTab::BcmCurrents,
        ]
    }

    pub fn next(&self) -> Self {
        match self {
            OverviewTab::BpmOrbit => OverviewTab::BlmLosses,
            OverviewTab::BlmLosses => OverviewTab::BcmCurrents,
            OverviewTab::BcmCurrents => OverviewTab::BpmOrbit,
        }
    }

    pub fn prev(&self) -> Self {
        match self {
            OverviewTab::BpmOrbit => OverviewTab::BcmCurrents,
            OverviewTab::BlmLosses => OverviewTab::BpmOrbit,
            OverviewTab::BcmCurrents => OverviewTab::BlmLosses,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrillDownTab {
    Waveforms,
    Fft,
    Settings,
}

impl DrillDownTab {
    pub fn label(&self) -> &str {
        match self {
            DrillDownTab::Waveforms => "Waveforms",
            DrillDownTab::Fft => "FFT",
            DrillDownTab::Settings => "Settings",
        }
    }

    pub fn all() -> &'static [DrillDownTab] {
        &[DrillDownTab::Waveforms, DrillDownTab::Fft, DrillDownTab::Settings]
    }

    pub fn next(&self) -> Self {
        match self {
            DrillDownTab::Waveforms => DrillDownTab::Fft,
            DrillDownTab::Fft => DrillDownTab::Settings,
            DrillDownTab::Settings => DrillDownTab::Waveforms,
        }
    }

    pub fn prev(&self) -> Self {
        match self {
            DrillDownTab::Waveforms => DrillDownTab::Settings,
            DrillDownTab::Fft => DrillDownTab::Waveforms,
            DrillDownTab::Settings => DrillDownTab::Fft,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Overview,
    DrillDown,
}

pub struct App {
    pub state: Arc<Mutex<SharedState>>,
    pub screen: Screen,
    pub overview_tab: OverviewTab,
    pub drilldown_tab: DrillDownTab,
    pub device_scroll: usize,
    pub selected_device: usize,
    pub selected_channel: usize,
    pub running: bool,
    // Control editing state
    pub editing_control: Option<usize>,
    pub edit_buffer: String,
    // FFT cache
    pub fft_result: Vec<f64>,
    pub fft_source_key: String,
    // Hosts for write-back
    pub hosts: Vec<(String, u16)>,
}

impl App {
    pub fn new(state: Arc<Mutex<SharedState>>, hosts: Vec<(String, u16)>) -> Self {
        Self {
            state,
            screen: Screen::Overview,
            overview_tab: OverviewTab::BpmOrbit,
            drilldown_tab: DrillDownTab::Waveforms,
            device_scroll: 0,
            selected_device: 0,
            selected_channel: 0,
            running: true,
            editing_control: None,
            edit_buffer: String::new(),
            fft_result: Vec::new(),
            fft_source_key: String::new(),
            hosts: hosts.clone(),
        }
    }

    /// Get snapshot of shared state
    pub fn snapshot(&self) -> SharedState {
        self.state.lock().unwrap().clone()
    }

    /// Filter devices by type for current overview tab
    pub fn filtered_devices<'a>(&self, snap: &'a SharedState) -> Vec<(usize, &'a DeviceInfo)> {
        let dtype = match self.overview_tab {
            OverviewTab::BpmOrbit => DeviceType::Bpm,
            OverviewTab::BlmLosses => DeviceType::Blm,
            OverviewTab::BcmCurrents => DeviceType::Bcm,
        };
        snap.devices
            .iter()
            .enumerate()
            .filter(|(_, d)| d.device_type == dtype)
            .collect()
    }

    /// Get BPM orbit data: Vec<(device_name, h_pos_mean, v_pos_mean, h_intensity_mean, v_intensity_mean)>
    pub fn bpm_orbit_data(&self, snap: &SharedState) -> Vec<(String, f64, f64, f64, f64)> {
        let mut result = Vec::new();
        for dev in &snap.devices {
            if dev.device_type != DeviceType::Bpm {
                continue;
            }
            let data = match snap.device_data.get(&dev.name) {
                Some(d) => d,
                None => continue,
            };

            let h_pos = waveform_mean(data.waveforms.get("POS_H"));
            let v_pos = waveform_mean(data.waveforms.get("POS_V"));
            let h_int = waveform_mean(data.waveforms.get("INT_H"));
            let v_int = waveform_mean(data.waveforms.get("INT_V"));

            result.push((dev.name.clone(), h_pos, v_pos, h_int, v_int));
        }
        result
    }

    /// Get BLM loss data: Vec<(device_name, total_loss)>
    pub fn blm_loss_data(&self, snap: &SharedState) -> Vec<(String, f64)> {
        let mut result = Vec::new();
        for dev in &snap.devices {
            if dev.device_type != DeviceType::Blm {
                continue;
            }
            let data = match snap.device_data.get(&dev.name) {
                Some(d) => d,
                None => continue,
            };

            // Sum INTEG_50MS_CH* scalars for total loss
            let mut total = 0.0;
            for i in 0..8 {
                let key = format!("INTEG_50MS_CH{}", i);
                if let Some(&val) = data.scalars.get(&key) {
                    total += val;
                }
            }
            result.push((dev.name.clone(), total));
        }
        result
    }

    /// Get BCM current data: Vec<(device_name, Vec<channel_current>)>
    pub fn bcm_current_data(&self, snap: &SharedState) -> Vec<(String, Vec<f64>)> {
        let mut result = Vec::new();
        for dev in &snap.devices {
            if dev.device_type != DeviceType::Bcm {
                continue;
            }
            let data = match snap.device_data.get(&dev.name) {
                Some(d) => d,
                None => continue,
            };

            let mut currents = Vec::new();
            for i in 0..4 {
                let key = format!("INTEG_GATED_CH{}", i);
                currents.push(*data.scalars.get(&key).unwrap_or(&0.0));
            }
            result.push((dev.name.clone(), currents));
        }
        result
    }

    /// Get the currently selected device
    pub fn current_device<'a>(&self, snap: &'a SharedState) -> Option<&'a DeviceInfo> {
        snap.devices.get(self.selected_device)
    }

    /// Compute FFT of a waveform
    pub fn compute_fft(&mut self, waveform: &[f64]) {
        if waveform.is_empty() {
            self.fft_result.clear();
            return;
        }

        let n = waveform.len().next_power_of_two();
        let mut planner = FftPlanner::new();
        let fft = planner.plan_fft_forward(n);

        let mut buffer: Vec<Complex<f64>> = waveform
            .iter()
            .map(|&x| Complex::new(x, 0.0))
            .collect();
        buffer.resize(n, Complex::new(0.0, 0.0));

        fft.process(&mut buffer);

        // Take magnitude of first half (positive frequencies)
        let n_bins = n / 2 + 1;
        self.fft_result = buffer[..n_bins]
            .iter()
            .map(|c| c.norm() / (n as f64).sqrt())
            .collect();
    }

    /// Handle device selection ensuring bounds
    pub fn clamp_selection(&mut self, snap: &SharedState) {
        if !snap.devices.is_empty() {
            let filtered = self.filtered_devices(snap);
            if !filtered.is_empty() {
                // Map selected_device to the filtered list range
                let max_idx = snap.devices.len().saturating_sub(1);
                if self.selected_device > max_idx {
                    self.selected_device = max_idx;
                }
            }
        }

        if let Some(dev) = snap.devices.get(self.selected_device) {
            let waveform_channels: Vec<_> = dev
                .channels
                .iter()
                .filter(|c| c.kind == DataKind::Waveform)
                .collect();
            if !waveform_channels.is_empty()
                && self.selected_channel >= waveform_channels.len()
            {
                self.selected_channel = 0;
            }
        }
    }
}

fn waveform_mean(wf: Option<&Vec<f64>>) -> f64 {
    match wf {
        Some(data) if !data.is_empty() => data.iter().sum::<f64>() / data.len() as f64,
        _ => 0.0,
    }
}
