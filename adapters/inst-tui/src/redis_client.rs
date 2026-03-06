use crate::data::*;
use redis::{Connection, RedisResult};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Manages connections to multiple Redis hosts and fetches device data
pub struct RedisPool {
    hosts: Vec<(String, u16)>,
    connections: HashMap<String, Connection>,
}

impl RedisPool {
    pub fn new(hosts: Vec<(String, u16)>) -> Self {
        Self {
            hosts,
            connections: HashMap::new(),
        }
    }

    fn host_key(host: &str, port: u16) -> String {
        format!("{}:{}", host, port)
    }

    fn get_or_connect(&mut self, host: &str, port: u16) -> Option<&mut Connection> {
        let key = Self::host_key(host, port);
        if !self.connections.contains_key(&key) {
            let url = format!("redis://{}:{}/", host, port);
            match redis::Client::open(url.as_str()) {
                Ok(client) => match client.get_connection_with_timeout(Duration::from_secs(2)) {
                    Ok(conn) => {
                        self.connections.insert(key.clone(), conn);
                    }
                    Err(e) => {
                        eprintln!("Failed to connect to {}: {}", key, e);
                        return None;
                    }
                },
                Err(e) => {
                    eprintln!("Invalid Redis URL {}: {}", url, e);
                    return None;
                }
            }
        }
        self.connections.get_mut(&key)
    }

    /// Scan all hosts for META keys and return discovered devices
    pub fn discover_devices(&mut self) -> Vec<DeviceInfo> {
        let mut devices = Vec::new();

        for (host, port) in self.hosts.clone() {
            let conn = match self.get_or_connect(&host, port) {
                Some(c) => c,
                None => continue,
            };

            // SCAN for keys matching *:META
            let keys: Vec<String> = match scan_keys(conn, "*:META") {
                Ok(k) => k,
                Err(_) => continue,
            };

            for key in &keys {
                // Read latest entry from the META stream
                let raw: RedisResult<Vec<(String, Vec<(String, redis::Value)>)>> =
                    redis::cmd("XREVRANGE")
                        .arg(key)
                        .arg("+")
                        .arg("-")
                        .arg("COUNT")
                        .arg(1)
                        .query(conn);

                if let Ok(entries) = raw {
                    for (_id, fields) in entries {
                        for (field, value) in fields {
                            if field == "_" {
                                if let redis::Value::BulkString(bytes) = value {
                                    let json_str = String::from_utf8_lossy(&bytes);
                                    if let Ok(meta) =
                                        serde_json::from_str::<DeviceMetadata>(&json_str)
                                    {
                                        devices.push(DeviceInfo {
                                            name: meta.device.clone(),
                                            device_type: meta.device_type,
                                            redis_host: host.clone(),
                                            redis_port: port,
                                            data_type: meta.data_type,
                                            channels: meta.channels,
                                            controls: meta.controls,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Sort: BPMs first, then BLMs, then BCMs, by name within type
        devices.sort_by(|a, b| {
            a.device_type
                .cmp(&b.device_type)
                .then(a.name.cmp(&b.name))
        });
        devices
    }

    /// Fetch latest scalar value from a stream key
    pub fn fetch_scalar(&mut self, host: &str, port: u16, key: &str) -> Option<f64> {
        let conn = self.get_or_connect(host, port)?;

        let entries: RedisResult<Vec<(String, Vec<(String, redis::Value)>)>> =
            redis::cmd("XREVRANGE")
                .arg(key)
                .arg("+")
                .arg("-")
                .arg("COUNT")
                .arg(1)
                .query(conn);

        if let Ok(entries) = entries {
            for (_id, fields) in entries {
                for (field, value) in fields {
                    if field == "_" {
                        if let redis::Value::BulkString(bytes) = value {
                            return deserialize_scalar(&bytes);
                        }
                    }
                }
            }
        }
        None
    }

    /// Fetch latest waveform from a stream key
    pub fn fetch_waveform(
        &mut self,
        host: &str,
        port: u16,
        key: &str,
        data_type: &str,
    ) -> Option<Vec<f64>> {
        let conn = self.get_or_connect(host, port)?;

        let entries: RedisResult<Vec<(String, Vec<(String, redis::Value)>)>> =
            redis::cmd("XREVRANGE")
                .arg(key)
                .arg("+")
                .arg("-")
                .arg("COUNT")
                .arg(1)
                .query(conn);

        if let Ok(entries) = entries {
            for (_id, fields) in entries {
                for (field, value) in fields {
                    if field == "_" {
                        if let redis::Value::BulkString(bytes) = value {
                            let wf = deserialize_waveform(&bytes, data_type);
                            if !wf.is_empty() {
                                return Some(wf);
                            }
                        }
                    }
                }
            }
        }
        None
    }

    /// Write a control value to a device's setting stream
    pub fn write_control(
        &mut self,
        host: &str,
        port: u16,
        key: &str,
        value: f64,
    ) -> bool {
        let conn = match self.get_or_connect(host, port) {
            Some(c) => c,
            None => return false,
        };

        // Write as string value to match twin's expected format:
        // XADD {key} * v {value}
        let result: RedisResult<String> = redis::cmd("XADD")
            .arg(key)
            .arg("*")
            .arg("v")
            .arg(value.to_string())
            .query(conn);

        result.is_ok()
    }
}

impl DeviceType {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let ord = |d: &DeviceType| -> u8 {
            match d {
                DeviceType::Bpm => 0,
                DeviceType::Blm => 1,
                DeviceType::Bcm => 2,
            }
        };
        ord(self).cmp(&ord(other))
    }
}

fn scan_keys(conn: &mut Connection, pattern: &str) -> RedisResult<Vec<String>> {
    let keys: Vec<String> = redis::cmd("KEYS").arg(pattern).query(conn)?;
    Ok(keys)
}

/// Shared state between background fetcher and UI
#[derive(Debug, Clone)]
pub struct SharedState {
    pub devices: Vec<DeviceInfo>,
    pub device_data: HashMap<String, DeviceData>,
    pub connected: bool,
    pub last_error: Option<String>,
}

impl Default for SharedState {
    fn default() -> Self {
        Self {
            devices: Vec::new(),
            device_data: HashMap::new(),
            connected: false,
            last_error: None,
        }
    }
}

/// Start background data fetcher thread
pub fn start_fetcher(
    hosts: Vec<(String, u16)>,
    state: Arc<Mutex<SharedState>>,
    refresh_ms: u64,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut pool = RedisPool::new(hosts);

        // Initial discovery
        let devices = pool.discover_devices();
        {
            let mut s = state.lock().unwrap();
            s.devices = devices;
            s.connected = true;
        }

        loop {
            thread::sleep(Duration::from_millis(refresh_ms));

            // Re-discover periodically (every 30 refreshes)
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let count = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let should_rediscover = count % 30 == 0;

            if should_rediscover {
                let devices = pool.discover_devices();
                let mut s = state.lock().unwrap();
                s.devices = devices;
            }

            // Fetch data for all known devices
            let devices = {
                let s = state.lock().unwrap();
                s.devices.clone()
            };

            let mut all_data: HashMap<String, DeviceData> = HashMap::new();

            for dev in &devices {
                let mut data = DeviceData::default();

                for ch in &dev.channels {
                    let full_key = dev.stream_key(&ch.key);

                    match ch.kind {
                        DataKind::Scalar => {
                            if let Some(val) =
                                pool.fetch_scalar(&dev.redis_host, dev.redis_port, &full_key)
                            {
                                data.scalars.insert(ch.key.clone(), val);
                            }
                        }
                        DataKind::Waveform => {
                            if let Some(wf) = pool.fetch_waveform(
                                &dev.redis_host,
                                dev.redis_port,
                                &full_key,
                                &dev.data_type,
                            ) {
                                data.waveforms.insert(ch.key.clone(), wf);
                            }
                        }
                    }
                }

                all_data.insert(dev.name.clone(), data);
            }

            {
                let mut s = state.lock().unwrap();
                s.device_data = all_data;
            }
        }
    })
}
