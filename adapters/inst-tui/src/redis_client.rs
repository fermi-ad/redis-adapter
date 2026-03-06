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

    /// Scan a single host for META keys and return discovered devices
    fn discover_on_host(&mut self, host: &str, port: u16) -> Vec<DeviceInfo> {
        let mut devices = Vec::new();
        let conn = match self.get_or_connect(host, port) {
            Some(c) => c,
            None => return devices,
        };

        let keys: Vec<String> = match scan_keys(conn, "*:META") {
            Ok(k) => k,
            Err(_) => return devices,
        };

        for key in &keys {
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
                                        redis_host: host.to_string(),
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

        devices
    }

    /// Scan all hosts for META keys and return discovered devices
    pub fn discover_devices(&mut self) -> Vec<DeviceInfo> {
        let mut devices = Vec::new();

        for (host, port) in self.hosts.clone() {
            devices.extend(self.discover_on_host(&host, port));
        }

        devices.sort_by(|a, b| {
            a.device_type
                .cmp(&b.device_type)
                .then(a.name.cmp(&b.name))
        });
        devices
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

/// Parse XREAD response from raw redis::Value
/// Returns Vec<(stream_key, Vec<(entry_id, Vec<(field, value_bytes)>)>)>
fn parse_xread_response(
    val: &redis::Value,
) -> Vec<(String, Vec<(String, Vec<(String, Vec<u8>)>)>)> {
    let mut result = Vec::new();

    // XREAD returns: Array of [stream_key, entries] pairs
    let streams = match val {
        redis::Value::Array(arr) => arr,
        redis::Value::Nil => return result,
        _ => return result,
    };

    for stream in streams {
        let pair = match stream {
            redis::Value::Array(arr) if arr.len() == 2 => arr,
            _ => continue,
        };

        let stream_key = match &pair[0] {
            redis::Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
            _ => continue,
        };

        let entries_arr = match &pair[1] {
            redis::Value::Array(arr) => arr,
            _ => continue,
        };

        let mut entries = Vec::new();
        for entry in entries_arr {
            let entry_pair = match entry {
                redis::Value::Array(arr) if arr.len() == 2 => arr,
                _ => continue,
            };

            let entry_id = match &entry_pair[0] {
                redis::Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                _ => continue,
            };

            let fields_arr = match &entry_pair[1] {
                redis::Value::Array(arr) => arr,
                _ => continue,
            };

            let mut fields = Vec::new();
            let mut i = 0;
            while i + 1 < fields_arr.len() {
                let field_name = match &fields_arr[i] {
                    redis::Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                    _ => {
                        i += 2;
                        continue;
                    }
                };
                let field_value = match &fields_arr[i + 1] {
                    redis::Value::BulkString(b) => b.clone(),
                    _ => Vec::new(),
                };
                fields.push((field_name, field_value));
                i += 2;
            }

            entries.push((entry_id, fields));
        }

        result.push((stream_key, entries));
    }

    result
}

/// Seed initial data for all devices on a host using XREVRANGE
fn seed_initial_data(
    conn: &mut Connection,
    devices: &[DeviceInfo],
) -> (HashMap<String, DeviceData>, HashMap<String, String>) {
    let mut all_data: HashMap<String, DeviceData> = HashMap::new();
    let mut last_ids: HashMap<String, String> = HashMap::new();

    for dev in devices {
        let mut data = DeviceData::default();

        for ch in &dev.channels {
            let full_key = dev.stream_key(&ch.key);

            let raw: RedisResult<Vec<(String, Vec<(String, redis::Value)>)>> =
                redis::cmd("XREVRANGE")
                    .arg(&full_key)
                    .arg("+")
                    .arg("-")
                    .arg("COUNT")
                    .arg(1)
                    .query(conn);

            if let Ok(entries) = raw {
                for (id, fields) in entries {
                    last_ids.insert(full_key.clone(), id);
                    for (field, value) in fields {
                        if field == "_" {
                            if let redis::Value::BulkString(bytes) = value {
                                match ch.kind {
                                    DataKind::Scalar => {
                                        if let Some(val) = deserialize_scalar(&bytes) {
                                            data.scalars.insert(ch.key.clone(), val);
                                        }
                                    }
                                    DataKind::Waveform => {
                                        let wf =
                                            deserialize_waveform(&bytes, &dev.data_type);
                                        if !wf.is_empty() {
                                            data.waveforms.insert(ch.key.clone(), wf);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        all_data.insert(dev.name.clone(), data);
    }

    (all_data, last_ids)
}

/// Per-host XREAD streaming thread
fn host_reader(
    host: String,
    port: u16,
    state: Arc<Mutex<SharedState>>,
    rediscover_interval: Duration,
) {
    let url = format!("redis://{}:{}/", host, port);
    let client = match redis::Client::open(url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Invalid Redis URL {}: {}", url, e);
            return;
        }
    };

    loop {
        let mut conn = match client.get_connection_with_timeout(Duration::from_secs(5)) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to connect to {}:{}: {}", host, port, e);
                thread::sleep(Duration::from_secs(2));
                continue;
            }
        };

        // Discover devices on this host
        let mut pool = RedisPool::new(vec![(host.clone(), port)]);
        let devices = pool.discover_on_host(&host, port);

        if devices.is_empty() {
            thread::sleep(Duration::from_secs(2));
            continue;
        }

        // Add devices to shared state
        {
            let mut s = state.lock().unwrap();
            // Remove old devices from this host, add new ones
            s.devices.retain(|d| d.redis_host != host || d.redis_port != port);
            s.devices.extend(devices.clone());
            s.devices.sort_by(|a, b| {
                a.device_type
                    .cmp(&b.device_type)
                    .then(a.name.cmp(&b.name))
            });
            s.connected = true;
        }

        // Build stream key list and device map
        let mut stream_keys: Vec<String> = Vec::new();
        // Map from stream_key -> (device_name, channel_key, kind, data_type)
        let mut key_map: HashMap<String, (String, String, DataKind, String)> = HashMap::new();

        for dev in &devices {
            for ch in &dev.channels {
                let full_key = dev.stream_key(&ch.key);
                stream_keys.push(full_key.clone());
                key_map.insert(
                    full_key,
                    (
                        dev.name.clone(),
                        ch.key.clone(),
                        ch.kind,
                        dev.data_type.clone(),
                    ),
                );
            }
        }

        if stream_keys.is_empty() {
            thread::sleep(Duration::from_secs(2));
            continue;
        }

        // Seed initial data with XREVRANGE
        let (initial_data, mut last_ids) = seed_initial_data(&mut conn, &devices);
        {
            let mut s = state.lock().unwrap();
            for (dev_name, data) in &initial_data {
                let entry = s.device_data.entry(dev_name.clone()).or_default();
                entry.scalars.extend(data.scalars.clone());
                entry.waveforms.extend(data.waveforms.clone());
            }
        }

        // For any keys without initial data, use "$" to get only new messages
        for key in &stream_keys {
            last_ids.entry(key.clone()).or_insert_with(|| "$".to_string());
        }

        let rediscover_start = std::time::Instant::now();

        // XREAD loop
        loop {
            // Check if we should re-discover
            if rediscover_start.elapsed() >= rediscover_interval {
                break; // Break inner loop to re-discover
            }

            // Non-blocking XREAD to grab all pending entries across all streams
            // We sleep manually after each cycle for rate control
            let mut cmd = redis::cmd("XREAD");
            cmd.arg("COUNT").arg(1).arg("STREAMS");

            for key in &stream_keys {
                cmd.arg(key);
            }
            for key in &stream_keys {
                let id = last_ids.get(key).map(|s| s.as_str()).unwrap_or("$");
                cmd.arg(id);
            }

            let response: RedisResult<redis::Value> = cmd.query(&mut conn);

            match response {
                Ok(ref val) => {
                    let parsed = parse_xread_response(val);
                    if parsed.is_empty() {
                        // Non-blocking XREAD returned nothing, sleep briefly
                        thread::sleep(Duration::from_millis(20));
                        continue;
                    }

                    let mut s = state.lock().unwrap();

                    for (stream_key, entries) in parsed {
                        if let Some((dev_name, ch_key, kind, data_type)) =
                            key_map.get(&stream_key)
                        {
                            let dev_data =
                                s.device_data.entry(dev_name.clone()).or_default();

                            for (entry_id, fields) in &entries {
                                // Update last ID
                                last_ids
                                    .insert(stream_key.clone(), entry_id.clone());

                                for (field_name, field_bytes) in fields {
                                    if field_name == "_" {
                                        match kind {
                                            DataKind::Scalar => {
                                                if let Some(val) =
                                                    deserialize_scalar(field_bytes)
                                                {
                                                    dev_data.scalars.insert(
                                                        ch_key.clone(),
                                                        val,
                                                    );
                                                }
                                            }
                                            DataKind::Waveform => {
                                                let wf = deserialize_waveform(
                                                    field_bytes,
                                                    data_type,
                                                );
                                                if !wf.is_empty() {
                                                    dev_data.waveforms.insert(
                                                        ch_key.clone(),
                                                        wf,
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[xread] ERROR on {}:{}: {}", host, port, e);
                    break; // Reconnect
                }
            }
        }
    }
}

/// Start background data fetcher - one thread per Redis host
pub fn start_fetcher(
    hosts: Vec<(String, u16)>,
    state: Arc<Mutex<SharedState>>,
    _refresh_ms: u64,
) -> Vec<thread::JoinHandle<()>> {
    let mut handles = Vec::new();

    for (host, port) in hosts {
        let st = state.clone();
        let h = thread::spawn(move || {
            host_reader(host, port, st, Duration::from_secs(30));
        });
        handles.push(h);
    }

    handles
}
