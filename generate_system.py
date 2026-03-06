#!/usr/bin/env python3
"""
Generate system-level configs and docker-compose for full accelerator simulation.

  125 BPMs (500 channels) via 70 twin+adapter pairs:
    55 pairs × 8 IQ channels = 440 channels (110 BPMs)
    15 pairs × 4 IQ channels =  60 channels ( 15 BPMs)

  130 BLMs (single channel each) via 130 twin+adapter pairs
    8 BLMs per digitizer → 17 Redis nodes

  20 BCMs (4 channels each) via 20 twin+adapter pairs
    4 BCMs per digitizer → 5 Redis nodes

  Each digitizer gets its own Redis instance.
  Grouping: 1 BPM pair per digitizer, 8 BLMs per digitizer, 4 BCMs per digitizer

  Total: 92 Redis + 220 twins + 220 adapters = 532 services
"""

import os
import math
import yaml

OUT_DIR = "system-configs"
COMPOSE_FILE = "docker-compose.system.yml"

# --- BPM ---
NUM_BPM_8CH = 55   # 8 IQ channels each (2 BPMs worth)
NUM_BPM_4CH = 15   # 4 IQ channels each (1 BPM)
BPM_SAMPLES = 10000
BPM_PER_DIGITIZER = 1  # each twin pair is already 1 digitizer

# --- BLM ---
NUM_BLM = 130
BLM_CHANNELS = 1
BLM_SAMPLES = 10000
BLM_PER_DIGITIZER = 8

# --- BCM ---
NUM_BCM = 20
BCM_CHANNELS = 4
BCM_SAMPLES = 10000
BCM_PER_DIGITIZER = 4


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def write_yaml(path, data):
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


# ============================================================
# Redis service helper
# ============================================================

def gen_redis_service():
    """Generate a per-digitizer Redis service definition."""
    return {
        "image": "redis:7.4",
        "command": "redis-server --appendonly no --save \"\" --bind 0.0.0.0 --protected-mode no",
        "healthcheck": {
            "test": ["CMD", "redis-cli", "ping"],
            "interval": "2s",
            "timeout": "3s",
            "retries": 5,
        },
    }


# ============================================================
# BPM config generators
# ============================================================

def gen_bpm_twin_config(idx, num_channels, redis_host):
    dev_name = f"BPM:{idx:04d}"
    return {
        "Device": {
            "DeviceName": dev_name,
            "RedisHost": redis_host,
            "OutputKeyIQ": "RAW_MUX_WF",
            "OutputKeyADC": "RAW_ADC",
            "Mode": 0,
            "UpdateIntervalMs": 50,
            "SamplesPerChannel": BPM_SAMPLES,
            "NumChannels": num_channels,
            "BeamX": round(2.0 + (idx % 5) * 0.5 - 1.0, 2),
            "BeamY": round(-1.0 + (idx % 3) * 0.3, 2),
            "BeamIntensity": 1.0,
            "Aperture": 10.0,
            "CarrierFreq": 5.0,
            "CarrierPhase": 0.0,
            "Noise": 0.02,
            "GateEnable": 1,
            "GateStart": 0.2,
            "GateWidth": 0.4,
            "ADCScale": 16000.0,
        }
    }


def gen_bpm_adapter_config(idx, num_channels, redis_host):
    dev_name = f"BPM:{idx:04d}"
    channels = []
    for ch in range(num_channels):
        entry = {"MagKey": f"MAG_CH{ch}", "PhaseKey": f"PHASE_CH{ch}"}
        if ch == 0:
            entry["FilterKey"] = "FILTERED_CH0"
            entry["FilterCoeff"] = 0.05
            entry["FFTMagKey"] = "FFT_MAG"
            entry["FFTPhaseKey"] = "FFT_PHASE"
            entry["BaselineSubKey"] = "DIFF_H"
            entry["BaselineChIdx"] = 1
        channels.append(entry)

    positions = []
    for base in range(0, num_channels, 4):
        if base + 3 < num_channels:
            positions.append({
                "ChannelA": base, "ChannelB": base + 1,
                "PositionKey": f"POS_H{base // 4}" if base > 0 else "POS_H",
                "IntensityKey": f"INT_H{base // 4}" if base > 0 else "INT_H",
                "PositionScale": 10.0, "IntensityScale": 1.0,
            })
            positions.append({
                "ChannelA": base + 2, "ChannelB": base + 3,
                "PositionKey": f"POS_V{base // 4}" if base > 0 else "POS_V",
                "IntensityKey": f"INT_V{base // 4}" if base > 0 else "INT_V",
                "PositionScale": 10.0, "IntensityScale": 1.0,
            })

    return {
        "Device": {
            "DeviceName": dev_name,
            "RedisHost": redis_host,
            "InputKey": "RAW_MUX_WF",
            "DataTypeIn": "float32",
            "DataTypeOut": "float32",
            "NumChannels": num_channels,
            "Channels": channels,
            "Positions": positions,
            "Integrate": {
                "Channel": 0,
                "BaselineChannel": 1,
                "Windows": [
                    {"OutputKey": "INTEG_NARROW", "Start": 0.2, "Width": 0.4,
                     "AvgKey": "INTEG_NARROW_AVG", "AvgSeconds": 300},
                    {"OutputKey": "INTEG_WIDE", "Start": 0.0, "Width": 1.0,
                     "AvgKey": "INTEG_WIDE_AVG", "AvgSeconds": 300},
                ],
            },
        }
    }


# ============================================================
# BLM config generators
# ============================================================

def gen_blm_twin_config(idx, redis_host):
    dev_name = f"BLM:{idx:04d}"
    return {
        "Device": {
            "DeviceName": dev_name,
            "RedisHost": redis_host,
            "OutputKey": "RAW_BLM_WF",
            "UpdateIntervalMs": 50,
            "SamplesPerChannel": BLM_SAMPLES,
            "NumChannels": BLM_CHANNELS,
            "Baseline": 0.001,
            "Noise": 0.0005,
            "LossEnable": 1,
            "LossAmplitude": round(0.5 + (idx % 10) * 0.1, 2),
            "LossPosition": 0.5,
            "LossWidth": 0.02,
            "ChannelScales": [1.0],
        }
    }


def gen_blm_adapter_config(idx, redis_host):
    dev_name = f"BLM:{idx:04d}"
    return {
        "Device": {
            "DeviceName": dev_name,
            "RedisHost": redis_host,
            "InputKey": "RAW_BLM_WF",
            "DataTypeIn": "float32",
            "DataTypeOut": "float32",
            "NumChannels": BLM_CHANNELS,
            "Channels": [
                {"RawKey": "RAW_CH0", "FilterKey": "FILTERED_CH0", "FilterCoeff": 0.05},
            ],
            "Integrate": [
                {
                    "Name": "10ms",
                    "BinSamples": 10000,
                    "AvgSeconds": 300,
                    "OutputKeys": ["INTEG_10MS_CH0"],
                    "AvgKeys": ["INTEG_10MS_AVG5M_CH0"],
                },
                {
                    "Name": "10ms-1min",
                    "BinSamples": 10000,
                    "AvgSeconds": 60,
                    "OutputKeys": [""],
                    "AvgKeys": ["INTEG_10MS_AVG1M_CH0"],
                },
                {
                    "Name": "full",
                    "BinSamples": BLM_SAMPLES,
                    "AvgSeconds": 300,
                    "OutputKeys": ["INTEG_FULL_CH0"],
                    "AvgKeys": ["INTEG_FULL_AVG5M_CH0"],
                },
                {
                    "Name": "full-1min",
                    "BinSamples": BLM_SAMPLES,
                    "AvgSeconds": 60,
                    "OutputKeys": [""],
                    "AvgKeys": ["INTEG_FULL_AVG1M_CH0"],
                },
            ],
        }
    }


# ============================================================
# BCM config generators
# ============================================================

def gen_bcm_twin_config(idx, redis_host):
    dev_name = f"BCM:{idx:04d}"
    return {
        "Device": {
            "DeviceName": dev_name,
            "RedisHost": redis_host,
            "OutputKeyRaw": "RAW_BCM_WF",
            "OutputKeyBkg": "BKG_BCM_WF",
            "UpdateIntervalMs": 50,
            "SamplesPerChannel": BCM_SAMPLES,
            "BeamCurrent": 100.0,
            "Baseline": 0.01,
            "Noise": 0.005,
            "CarrierFreq": 50.0,
            "CarrierPhase": 0.0,
            "GateEnable": 1,
            "GateStart": 0.2,
            "GateWidth": 0.4,
            "ChannelGains": [1.0, 0.95, 1.05, 0.98],
        }
    }


def gen_bcm_adapter_config(idx, redis_host):
    dev_name = f"BCM:{idx:04d}"
    channels = []
    for ch in range(BCM_CHANNELS):
        entry = {
            "SubKey": f"SUB_CH{ch}",
            "GatedKey": f"INTEG_GATED_CH{ch}",
            "GatedAvg1mKey": f"GATED_AVG1M_CH{ch}",
            "GatedAvg5mKey": f"GATED_AVG5M_CH{ch}",
            "UngatedKey": f"INTEG_UNGATED_CH{ch}",
            "UngatedAvg1mKey": f"UNGATED_AVG1M_CH{ch}",
            "UngatedAvg5mKey": f"UNGATED_AVG5M_CH{ch}",
        }
        if ch == 0:
            entry["FilterKey"] = "FILTERED_CH0"
            entry["FilterCoeff"] = 0.05
        channels.append(entry)

    return {
        "Device": {
            "DeviceName": dev_name,
            "RedisHost": redis_host,
            "InputKeyRaw": "RAW_BCM_WF",
            "InputKeyBkg": "BKG_BCM_WF",
            "DataTypeIn": "float32",
            "DataTypeOut": "float32",
            "NumChannels": BCM_CHANNELS,
            "GateStart": 0.2,
            "GateWidth": 0.4,
            "Channels": channels,
        }
    }


# ============================================================
# Docker compose generation
# ============================================================

def gen_compose(bpm_instances, blm_instances, bcm_instances):
    services = {}

    build_block = {"context": ".", "dockerfile": "Dockerfile"}
    redis_services = set()

    # BPM twins + adapters — 1 Redis per BPM digitizer (per twin pair)
    for idx, nch in bpm_instances:
        tag = f"{idx:03d}"
        redis_name = f"redis-bpm-{tag}"
        twin_name = f"bpm-twin-{tag}"
        adap_name = f"bpm-{tag}"
        twin_cfg = f"/etc/adapters/system/bpm-twin/bpm-twin-{tag}.yml"
        adap_cfg = f"/etc/adapters/system/bpm/bpm-{tag}.yml"

        if redis_name not in redis_services:
            services[redis_name] = gen_redis_service()
            redis_services.add(redis_name)

        services[twin_name] = {
            "build": build_block,
            "depends_on": {redis_name: {"condition": "service_healthy"}},
            "volumes": ["./system-configs/bpm-twin:/etc/adapters/system/bpm-twin:ro"],
            "command": ["/bpm-twin", twin_cfg],
        }
        services[adap_name] = {
            "build": build_block,
            "depends_on": {
                redis_name: {"condition": "service_healthy"},
                twin_name: {"condition": "service_started"},
            },
            "volumes": ["./system-configs/bpm:/etc/adapters/system/bpm:ro"],
            "command": ["/bpm", adap_cfg],
        }

    # BLM twins + adapters — 8 BLMs per digitizer/Redis
    num_blm_digitizers = math.ceil(len(blm_instances) / BLM_PER_DIGITIZER)
    for i, idx in enumerate(blm_instances):
        dig = (i // BLM_PER_DIGITIZER) + 1
        tag = f"{idx:03d}"
        redis_name = f"redis-blm-{dig:02d}"
        twin_name = f"blm-twin-{tag}"
        adap_name = f"blm-{tag}"
        twin_cfg = f"/etc/adapters/system/blm-twin/blm-twin-{tag}.yml"
        adap_cfg = f"/etc/adapters/system/blm/blm-{tag}.yml"

        if redis_name not in redis_services:
            services[redis_name] = gen_redis_service()
            redis_services.add(redis_name)

        services[twin_name] = {
            "build": build_block,
            "depends_on": {redis_name: {"condition": "service_healthy"}},
            "volumes": ["./system-configs/blm-twin:/etc/adapters/system/blm-twin:ro"],
            "command": ["/blm-twin", twin_cfg],
        }
        services[adap_name] = {
            "build": build_block,
            "depends_on": {
                redis_name: {"condition": "service_healthy"},
                twin_name: {"condition": "service_started"},
            },
            "volumes": ["./system-configs/blm:/etc/adapters/system/blm:ro"],
            "command": ["/blm", adap_cfg],
        }

    # BCM twins + adapters — 4 BCMs per digitizer/Redis
    num_bcm_digitizers = math.ceil(len(bcm_instances) / BCM_PER_DIGITIZER)
    for i, idx in enumerate(bcm_instances):
        dig = (i // BCM_PER_DIGITIZER) + 1
        tag = f"{idx:02d}"
        redis_name = f"redis-bcm-{dig:02d}"
        twin_name = f"bcm-twin-{tag}"
        adap_name = f"bcm-{tag}"
        twin_cfg = f"/etc/adapters/system/bcm-twin/bcm-twin-{tag}.yml"
        adap_cfg = f"/etc/adapters/system/bcm/bcm-{tag}.yml"

        if redis_name not in redis_services:
            services[redis_name] = gen_redis_service()
            redis_services.add(redis_name)

        services[twin_name] = {
            "build": build_block,
            "depends_on": {redis_name: {"condition": "service_healthy"}},
            "volumes": ["./system-configs/bcm-twin:/etc/adapters/system/bcm-twin:ro"],
            "command": ["/bcm-twin", twin_cfg],
        }
        services[adap_name] = {
            "build": build_block,
            "depends_on": {
                redis_name: {"condition": "service_healthy"},
                twin_name: {"condition": "service_started"},
            },
            "volumes": ["./system-configs/bcm:/etc/adapters/system/bcm:ro"],
            "command": ["/bcm", adap_cfg],
        }

    return {"services": services}, len(redis_services)


# ============================================================
# Main
# ============================================================

def main():
    # Create output directories
    for sub in ["bpm-twin", "bpm", "blm-twin", "blm", "bcm-twin", "bcm"]:
        ensure_dir(os.path.join(OUT_DIR, sub))

    # --- BPM instances ---
    bpm_instances = []
    idx = 1

    # 55 × 8-channel (2 BPMs worth each)
    for i in range(NUM_BPM_8CH):
        tag = f"{idx:03d}"
        redis_host = f"redis-bpm-{tag}"
        write_yaml(f"{OUT_DIR}/bpm-twin/bpm-twin-{tag}.yml", gen_bpm_twin_config(idx, 8, redis_host))
        write_yaml(f"{OUT_DIR}/bpm/bpm-{tag}.yml", gen_bpm_adapter_config(idx, 8, redis_host))
        bpm_instances.append((idx, 8))
        idx += 1

    # 15 × 4-channel (1 BPM each)
    for i in range(NUM_BPM_4CH):
        tag = f"{idx:03d}"
        redis_host = f"redis-bpm-{tag}"
        write_yaml(f"{OUT_DIR}/bpm-twin/bpm-twin-{tag}.yml", gen_bpm_twin_config(idx, 4, redis_host))
        write_yaml(f"{OUT_DIR}/bpm/bpm-{tag}.yml", gen_bpm_adapter_config(idx, 4, redis_host))
        bpm_instances.append((idx, 4))
        idx += 1

    total_bpm_channels = NUM_BPM_8CH * 8 + NUM_BPM_4CH * 4

    # --- BLM instances ---
    blm_instances = []
    for i in range(NUM_BLM):
        idx_blm = i + 1
        tag = f"{idx_blm:03d}"
        dig = (i // BLM_PER_DIGITIZER) + 1
        redis_host = f"redis-blm-{dig:02d}"
        write_yaml(f"{OUT_DIR}/blm-twin/blm-twin-{tag}.yml", gen_blm_twin_config(idx_blm, redis_host))
        write_yaml(f"{OUT_DIR}/blm/blm-{tag}.yml", gen_blm_adapter_config(idx_blm, redis_host))
        blm_instances.append(idx_blm)

    # --- BCM instances ---
    bcm_instances = []
    for i in range(NUM_BCM):
        idx_bcm = i + 1
        tag = f"{idx_bcm:02d}"
        dig = (i // BCM_PER_DIGITIZER) + 1
        redis_host = f"redis-bcm-{dig:02d}"
        write_yaml(f"{OUT_DIR}/bcm-twin/bcm-twin-{tag}.yml", gen_bcm_twin_config(idx_bcm, redis_host))
        write_yaml(f"{OUT_DIR}/bcm/bcm-{tag}.yml", gen_bcm_adapter_config(idx_bcm, redis_host))
        bcm_instances.append(idx_bcm)

    # --- Compose ---
    compose, num_redis = gen_compose(bpm_instances, blm_instances, bcm_instances)

    n_twins = len(bpm_instances) + NUM_BLM + NUM_BCM
    n_services = num_redis + 2 * n_twins

    num_blm_digitizers = math.ceil(NUM_BLM / BLM_PER_DIGITIZER)
    num_bcm_digitizers = math.ceil(NUM_BCM / BCM_PER_DIGITIZER)
    num_bpm_digitizers = len(bpm_instances)

    with open(COMPOSE_FILE, "w") as f:
        f.write("#\n")
        f.write("#  Full Accelerator Simulation System\n")
        f.write("#\n")
        f.write(f"#  125 BPMs ({total_bpm_channels} channels) via {len(bpm_instances)} twin+adapter pairs\n")
        f.write(f"#    {NUM_BPM_8CH} × 8ch + {NUM_BPM_4CH} × 4ch = {total_bpm_channels} channels\n")
        f.write(f"#    1 digitizer per pair → {num_bpm_digitizers} Redis nodes\n")
        f.write(f"#  {NUM_BLM} BLMs ({BLM_CHANNELS} channel each) via {NUM_BLM} twin+adapter pairs\n")
        f.write(f"#    {BLM_PER_DIGITIZER} BLMs per digitizer → {num_blm_digitizers} Redis nodes\n")
        f.write(f"#  {NUM_BCM} BCMs ({BCM_CHANNELS} channels each) via {NUM_BCM} twin+adapter pairs\n")
        f.write(f"#    {BCM_PER_DIGITIZER} BCMs per digitizer → {num_bcm_digitizers} Redis nodes\n")
        f.write("#\n")
        f.write(f"#  Total: {num_redis} Redis + {n_twins} twins + {n_twins} adapters = {n_services} services\n")
        f.write("#\n")
        f.write("#  Generated by generate_system.py\n")
        f.write("#\n")
        f.write("#  Usage:\n")
        f.write("#    python3 generate_system.py\n")
        f.write(f"#    docker compose -f {COMPOSE_FILE} build\n")
        f.write(f"#    docker compose -f {COMPOSE_FILE} up -d\n")
        f.write(f"#    docker compose -f {COMPOSE_FILE} down\n")
        f.write("#\n\n")
        yaml.dump(compose, f, default_flow_style=False, sort_keys=False)

    # Summary
    print(f"Generated system configs in {OUT_DIR}/")
    print(f"  BPM: {len(bpm_instances)} digitizers ({total_bpm_channels} channels)")
    print(f"    {NUM_BPM_8CH} × 8ch + {NUM_BPM_4CH} × 4ch")
    print(f"  BLM: {num_blm_digitizers} digitizers ({NUM_BLM} BLMs, {BLM_PER_DIGITIZER} per digitizer)")
    print(f"  BCM: {num_bcm_digitizers} digitizers ({NUM_BCM} BCMs, {BCM_PER_DIGITIZER} per digitizer)")
    print(f"  Redis: {num_redis} instances")
    print(f"  Total: {n_services} services")
    print(f"Generated {COMPOSE_FILE}")


if __name__ == "__main__":
    main()
