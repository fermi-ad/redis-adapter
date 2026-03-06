use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeviceType {
    Bpm,
    Blm,
    Bcm,
}

impl std::fmt::Display for DeviceType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DeviceType::Bpm => write!(f, "BPM"),
            DeviceType::Blm => write!(f, "BLM"),
            DeviceType::Bcm => write!(f, "BCM"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataKind {
    Waveform,
    Scalar,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelMeta {
    pub key: String,
    pub kind: DataKind,
    pub label: String,
    #[serde(default)]
    pub unit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlMeta {
    pub key: String,
    pub label: String,
    #[serde(default)]
    pub default_value: f64,
}

/// Metadata published by each adapter to {DeviceName}:META
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceMetadata {
    #[serde(rename = "type")]
    pub device_type: DeviceType,
    pub device: String,
    pub data_type: String,
    pub channels: Vec<ChannelMeta>,
    #[serde(default)]
    pub controls: Vec<ControlMeta>,
}

/// Runtime device info combining metadata with connection details
#[derive(Debug, Clone)]
pub struct DeviceInfo {
    pub name: String,
    pub device_type: DeviceType,
    pub redis_host: String,
    pub redis_port: u16,
    pub data_type: String,
    pub channels: Vec<ChannelMeta>,
    pub controls: Vec<ControlMeta>,
}

impl DeviceInfo {
    /// Full stream key: {DeviceName}:{SubKey}
    pub fn stream_key(&self, sub_key: &str) -> String {
        format!("{}:{}", self.name, sub_key)
    }
}

/// Cached data for a device
#[derive(Debug, Clone, Default)]
pub struct DeviceData {
    pub scalars: HashMap<String, f64>,
    pub waveforms: HashMap<String, Vec<f64>>,
    pub timestamps: HashMap<String, String>,
}

/// Deserialize a blob from Redis stream field "_" into Vec<f64>
/// Data is stored as raw bytes via memcpy in the C++ adapters
pub fn deserialize_waveform(blob: &[u8], data_type: &str) -> Vec<f64> {
    match data_type {
        "float32" => {
            let n = blob.len() / 4;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let bytes: [u8; 4] = [
                    blob[i * 4],
                    blob[i * 4 + 1],
                    blob[i * 4 + 2],
                    blob[i * 4 + 3],
                ];
                out.push(f32::from_ne_bytes(bytes) as f64);
            }
            out
        }
        "float64" => {
            let n = blob.len() / 8;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let bytes: [u8; 8] = [
                    blob[i * 8],
                    blob[i * 8 + 1],
                    blob[i * 8 + 2],
                    blob[i * 8 + 3],
                    blob[i * 8 + 4],
                    blob[i * 8 + 5],
                    blob[i * 8 + 6],
                    blob[i * 8 + 7],
                ];
                out.push(f64::from_ne_bytes(bytes));
            }
            out
        }
        "int32" => {
            let n = blob.len() / 4;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let bytes: [u8; 4] = [
                    blob[i * 4],
                    blob[i * 4 + 1],
                    blob[i * 4 + 2],
                    blob[i * 4 + 3],
                ];
                out.push(i32::from_ne_bytes(bytes) as f64);
            }
            out
        }
        "uint16" => {
            let n = blob.len() / 2;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let bytes: [u8; 2] = [blob[i * 2], blob[i * 2 + 1]];
                out.push(u16::from_ne_bytes(bytes) as f64);
            }
            out
        }
        _ => Vec::new(),
    }
}

/// Deserialize a scalar double from Redis stream field "_"
pub fn deserialize_scalar(blob: &[u8]) -> Option<f64> {
    if blob.len() != 8 {
        return None;
    }
    let bytes: [u8; 8] = blob.try_into().ok()?;
    Some(f64::from_ne_bytes(bytes))
}
