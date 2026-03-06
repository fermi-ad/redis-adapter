#!/usr/bin/env python3
"""
Generate system-level configs and a unified docker-compose.yml for all adapters.

Run from the adapters/ directory:
    cd adapters && python3 generate_system.py

Produces a single docker-compose.yml with all profiles:

  Demo profiles (single Redis, example configs):
    pipeline  — individual signal-processing adapters
    bpm-demo  — BPM composite adapter + twin
    blm-demo  — BLM adapter + twin
    bcm-demo  — BCM adapter + twin

  System profiles (per-digitizer Redis, generated configs):
    tiny   — fixed subset: 10 BPM + 10 BLM + 5 BCM
    small  — first 1/2 of all devices
    medium — first 2/3 of all devices
    large  — first 5/6 of all devices
    full   — all devices

  TUI tools (interactive, use with `docker compose run`):
    inst-tui  — instrument dashboard (BPM/BLM/BCM)
    redis-tui — Redis stream inspector

  System scale:
    125 BPMs (500 channels) via 70 twin+adapter pairs
    130 BLMs (single channel each) via 130 twin+adapter pairs
    20 BCMs (4 channels each) via 20 twin+adapter pairs
    Total: 92 Redis + 220 twins + 220 adapters = 532 services
"""

import os
import math
import yaml

OUT_DIR = "system-configs"
COMPOSE_FILE = "docker-compose.yml"

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


# --- Profile cutoffs ---
TINY_BPM = 10
TINY_BLM = 10
TINY_BCM = 5


def get_profiles(position, total, device_type):
    """Return profile list for a device based on its position (1-indexed) in the fleet."""
    tiny_limit = {"bpm": TINY_BPM, "blm": TINY_BLM, "bcm": TINY_BCM}[device_type]
    half = total // 2
    two_thirds = (total * 2 + 2) // 3
    five_sixths = (total * 5 + 5) // 6

    profiles = []
    if position <= tiny_limit:
        profiles.append("tiny")
    if position <= half:
        profiles.extend(["small", "medium", "large", "full"])
    elif position <= two_thirds:
        profiles.extend(["medium", "large", "full"])
    elif position <= five_sixths:
        profiles.extend(["large", "full"])
    else:
        profiles.append("full")
    return profiles


def broaden_profiles(existing, new):
    """Return the broadest (most permissive) profile set."""
    all_profiles = set(existing) | set(new)
    return [p for p in ["tiny", "small", "medium", "large", "full"] if p in all_profiles]


def write_yaml(path, data):
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


# ============================================================
# Demo services (static)
# ============================================================

def gen_demo_services():
    """Generate demo profile services (pipeline, bpm-demo, blm-demo, bcm-demo)."""
    build = {"context": "..", "dockerfile": "Dockerfile"}
    services = {}

    # Shared redis for all demo profiles
    services["redis"] = {
        "image": "redis:7.4",
        "profiles": ["pipeline", "bpm-demo", "blm-demo", "bcm-demo"],
        "ports": ["6379:6379"],
        "command": 'redis-server --appendonly no --save "" --bind 0.0.0.0 --protected-mode no',
        "healthcheck": {
            "test": ["CMD", "redis-cli", "ping"],
            "interval": "2s",
            "timeout": "3s",
            "retries": 5,
        },
    }

    # Pipeline demo
    pipeline_adapters = [
        ("device-twin", "acct-example.yml", ["redis"]),
        ("baseline-subtract", "baseline-subtract-example.yml", ["device-twin", "redis"]),
        ("integrate", "integrate-example.yml", ["device-twin", "redis"]),
        ("filter", "filter-example.yml", ["device-twin", "redis"]),
        ("magnitude", "magnitude-example.yml", ["device-twin", "redis"]),
        ("position-intensity", "position-intensity-example.yml", ["device-twin", "redis"]),
        ("demux", "demux-example.yml", ["device-twin", "redis"]),
        ("fft", "fft-example.yml", ["device-twin", "redis"]),
    ]
    for name, config, deps in pipeline_adapters:
        depends_on = {}
        for d in deps:
            if d == "redis":
                depends_on[d] = {"condition": "service_healthy"}
            else:
                depends_on[d] = {"condition": "service_started"}
        services[name] = {
            "build": build,
            "profiles": ["pipeline"],
            "depends_on": depends_on,
            "volumes": [f"./{name}/configs:/etc/adapters/{name}:ro"],
            "command": [f"/{name}", f"/etc/adapters/{name}/{config}"],
        }

    # BPM demo
    services["bpm-twin"] = {
        "build": build,
        "profiles": ["bpm-demo"],
        "depends_on": {"redis": {"condition": "service_healthy"}},
        "volumes": ["./bpm-twin/configs:/etc/adapters/bpm-twin:ro"],
        "command": ["/bpm-twin", "/etc/adapters/bpm-twin/bpm-demo.yml"],
    }
    services["bpm"] = {
        "build": build,
        "profiles": ["bpm-demo"],
        "depends_on": {
            "bpm-twin": {"condition": "service_started"},
            "redis": {"condition": "service_healthy"},
        },
        "volumes": ["./bpm/configs:/etc/adapters/bpm:ro"],
        "command": ["/bpm", "/etc/adapters/bpm/bpm-demo.yml"],
    }

    # BLM demo
    services["blm-twin"] = {
        "build": build,
        "profiles": ["blm-demo"],
        "depends_on": {"redis": {"condition": "service_healthy"}},
        "volumes": ["./blm-twin/configs:/etc/adapters/blm-twin:ro"],
        "command": ["/blm-twin", "/etc/adapters/blm-twin/blm-demo.yml"],
    }
    services["blm"] = {
        "build": build,
        "profiles": ["blm-demo"],
        "depends_on": {
            "blm-twin": {"condition": "service_started"},
            "redis": {"condition": "service_healthy"},
        },
        "volumes": ["./blm/configs:/etc/adapters/blm:ro"],
        "command": ["/blm", "/etc/adapters/blm/blm-demo.yml"],
    }

    # BCM demo
    services["bcm-twin"] = {
        "build": build,
        "profiles": ["bcm-demo"],
        "depends_on": {"redis": {"condition": "service_healthy"}},
        "volumes": ["./bcm-twin/configs:/etc/adapters/bcm-twin:ro"],
        "command": ["/bcm-twin", "/etc/adapters/bcm-twin/bcm-demo.yml"],
    }
    services["bcm"] = {
        "build": build,
        "profiles": ["bcm-demo"],
        "depends_on": {
            "bcm-twin": {"condition": "service_started"},
            "redis": {"condition": "service_healthy"},
        },
        "volumes": ["./bcm/configs:/etc/adapters/bcm:ro"],
        "command": ["/bcm", "/etc/adapters/bcm/bcm-demo.yml"],
    }

    return services


# ============================================================
# TUI services
# ============================================================

def gen_tui_services():
    """Generate inst-tui and redis-tui utility services."""
    build = {"context": "..", "dockerfile": "Dockerfile"}
    return {
        "inst-tui": {
            "build": build,
            "profiles": ["tui"],
            "stdin_open": True,
            "tty": True,
            "volumes": ["./system-configs:/etc/inst-tui:ro"],
            "command": ["/inst-tui"],
        },
        "redis-tui": {
            "build": build,
            "profiles": ["tui"],
            "stdin_open": True,
            "tty": True,
            "command": ["/redis-tui"],
        },
    }


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
# System services generation
# ============================================================

def gen_system_services(bpm_instances, blm_instances, bcm_instances):
    """Generate system-profile services (twins, adapters, per-digitizer Redis)."""
    services = {}

    build_block = {"context": "..", "dockerfile": "Dockerfile"}
    redis_profiles = {}

    total_bpm = len(bpm_instances)
    total_blm = len(blm_instances)
    total_bcm = len(bcm_instances)

    # BPM twins + adapters
    for pos, (idx, nch) in enumerate(bpm_instances, 1):
        tag = f"{idx:03d}"
        redis_name = f"redis-bpm-{tag}"
        twin_name = f"bpm-twin-{tag}"
        adap_name = f"bpm-{tag}"
        twin_cfg = f"/etc/adapters/system/bpm-twin/bpm-twin-{tag}.yml"
        adap_cfg = f"/etc/adapters/system/bpm/bpm-{tag}.yml"
        profiles = get_profiles(pos, total_bpm, "bpm")

        if redis_name not in redis_profiles:
            redis_profiles[redis_name] = profiles
        else:
            redis_profiles[redis_name] = broaden_profiles(redis_profiles[redis_name], profiles)

        services[twin_name] = {
            "build": build_block,
            "profiles": profiles,
            "depends_on": {redis_name: {"condition": "service_healthy"}},
            "volumes": ["./system-configs/bpm-twin:/etc/adapters/system/bpm-twin:ro"],
            "command": ["/bpm-twin", twin_cfg],
        }
        services[adap_name] = {
            "build": build_block,
            "profiles": profiles,
            "depends_on": {
                redis_name: {"condition": "service_healthy"},
                twin_name: {"condition": "service_started"},
            },
            "volumes": ["./system-configs/bpm:/etc/adapters/system/bpm:ro"],
            "command": ["/bpm", adap_cfg],
        }

    # BLM twins + adapters
    for i, idx in enumerate(blm_instances):
        pos = i + 1
        dig = (i // BLM_PER_DIGITIZER) + 1
        tag = f"{idx:03d}"
        redis_name = f"redis-blm-{dig:02d}"
        twin_name = f"blm-twin-{tag}"
        adap_name = f"blm-{tag}"
        twin_cfg = f"/etc/adapters/system/blm-twin/blm-twin-{tag}.yml"
        adap_cfg = f"/etc/adapters/system/blm/blm-{tag}.yml"
        profiles = get_profiles(pos, total_blm, "blm")

        if redis_name not in redis_profiles:
            redis_profiles[redis_name] = profiles
        else:
            redis_profiles[redis_name] = broaden_profiles(redis_profiles[redis_name], profiles)

        services[twin_name] = {
            "build": build_block,
            "profiles": profiles,
            "depends_on": {redis_name: {"condition": "service_healthy"}},
            "volumes": ["./system-configs/blm-twin:/etc/adapters/system/blm-twin:ro"],
            "command": ["/blm-twin", twin_cfg],
        }
        services[adap_name] = {
            "build": build_block,
            "profiles": profiles,
            "depends_on": {
                redis_name: {"condition": "service_healthy"},
                twin_name: {"condition": "service_started"},
            },
            "volumes": ["./system-configs/blm:/etc/adapters/system/blm:ro"],
            "command": ["/blm", adap_cfg],
        }

    # BCM twins + adapters
    for i, idx in enumerate(bcm_instances):
        pos = i + 1
        dig = (i // BCM_PER_DIGITIZER) + 1
        tag = f"{idx:02d}"
        redis_name = f"redis-bcm-{dig:02d}"
        twin_name = f"bcm-twin-{tag}"
        adap_name = f"bcm-{tag}"
        twin_cfg = f"/etc/adapters/system/bcm-twin/bcm-twin-{tag}.yml"
        adap_cfg = f"/etc/adapters/system/bcm/bcm-{tag}.yml"
        profiles = get_profiles(pos, total_bcm, "bcm")

        if redis_name not in redis_profiles:
            redis_profiles[redis_name] = profiles
        else:
            redis_profiles[redis_name] = broaden_profiles(redis_profiles[redis_name], profiles)

        services[twin_name] = {
            "build": build_block,
            "profiles": profiles,
            "depends_on": {redis_name: {"condition": "service_healthy"}},
            "volumes": ["./system-configs/bcm-twin:/etc/adapters/system/bcm-twin:ro"],
            "command": ["/bcm-twin", twin_cfg],
        }
        services[adap_name] = {
            "build": build_block,
            "profiles": profiles,
            "depends_on": {
                redis_name: {"condition": "service_healthy"},
                twin_name: {"condition": "service_started"},
            },
            "volumes": ["./system-configs/bcm:/etc/adapters/system/bcm:ro"],
            "command": ["/bcm", adap_cfg],
        }

    # Add Redis services with broadened profiles
    for redis_name, profiles in redis_profiles.items():
        svc = gen_redis_service()
        svc["profiles"] = profiles
        services[redis_name] = svc

    # Generate per-profile hosts files
    for profile in ["tiny", "small", "medium", "large", "full"]:
        profile_hosts = sorted(
            name for name, profs in redis_profiles.items()
            if profile in profs
        )
        hosts_file = os.path.join(OUT_DIR, f"inst-tui-hosts-{profile}.txt")
        with open(hosts_file, "w") as hf:
            for h in profile_hosts:
                hf.write(f"{h}\n")
        redis_tui_file = os.path.join(OUT_DIR, f"redis-tui-hosts-{profile}.txt")
        with open(redis_tui_file, "w") as hf:
            for h in profile_hosts:
                hf.write(f"redis://{h}:6379\n")

    return services, len(redis_profiles)


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

    for i in range(NUM_BPM_8CH):
        tag = f"{idx:03d}"
        redis_host = f"redis-bpm-{tag}"
        write_yaml(f"{OUT_DIR}/bpm-twin/bpm-twin-{tag}.yml", gen_bpm_twin_config(idx, 8, redis_host))
        write_yaml(f"{OUT_DIR}/bpm/bpm-{tag}.yml", gen_bpm_adapter_config(idx, 8, redis_host))
        bpm_instances.append((idx, 8))
        idx += 1

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

    # --- Build unified compose ---
    system_services, num_redis = gen_system_services(bpm_instances, blm_instances, bcm_instances)
    demo_services = gen_demo_services()
    tui_services = gen_tui_services()

    all_services = {}
    all_services.update(demo_services)
    all_services.update(tui_services)
    all_services.update(system_services)

    compose = {"services": all_services}

    n_twins = len(bpm_instances) + NUM_BLM + NUM_BCM
    n_services = num_redis + 2 * n_twins

    num_blm_digitizers = math.ceil(NUM_BLM / BLM_PER_DIGITIZER)
    num_bcm_digitizers = math.ceil(NUM_BCM / BCM_PER_DIGITIZER)
    num_bpm_digitizers = len(bpm_instances)

    n_bpm_tiny = TINY_BPM
    n_blm_tiny = TINY_BLM
    n_bcm_tiny = TINY_BCM
    n_bpm_small = len(bpm_instances) // 2
    n_bpm_medium = (len(bpm_instances) * 2 + 2) // 3
    n_bpm_large = (len(bpm_instances) * 5 + 5) // 6
    n_blm_small = NUM_BLM // 2
    n_blm_medium = (NUM_BLM * 2 + 2) // 3
    n_blm_large = (NUM_BLM * 5 + 5) // 6
    n_bcm_small = NUM_BCM // 2
    n_bcm_medium = (NUM_BCM * 2 + 2) // 3
    n_bcm_large = (NUM_BCM * 5 + 5) // 6

    def count_profile_services(profile):
        return sum(1 for s in compose["services"].values()
                   if profile in s.get("profiles", []))

    n_tiny = count_profile_services("tiny")
    n_small = count_profile_services("small")
    n_medium = count_profile_services("medium")
    n_large = count_profile_services("large")
    n_full = count_profile_services("full")

    with open(COMPOSE_FILE, "w") as f:
        f.write("#\n")
        f.write("#  Unified Adapter Compose\n")
        f.write("#\n")
        f.write("#  Demo profiles (single Redis, example configs):\n")
        f.write("#    pipeline  — individual signal-processing adapters\n")
        f.write("#    bpm-demo  — BPM composite adapter + twin\n")
        f.write("#    blm-demo  — BLM adapter + twin\n")
        f.write("#    bcm-demo  — BCM adapter + twin\n")
        f.write("#\n")
        f.write(f"#  System profiles ({num_redis} Redis + {n_twins} twins + {n_twins} adapters):\n")
        f.write(f"#    tiny   — {n_bpm_tiny} BPM + {n_blm_tiny} BLM + {n_bcm_tiny} BCM ({n_tiny} services)\n")
        f.write(f"#    small  — {n_bpm_small} BPM + {n_blm_small} BLM + {n_bcm_small} BCM ({n_small} services)\n")
        f.write(f"#    medium — {n_bpm_medium} BPM + {n_blm_medium} BLM + {n_bcm_medium} BCM ({n_medium} services)\n")
        f.write(f"#    large  — {n_bpm_large} BPM + {n_blm_large} BLM + {n_bcm_large} BCM ({n_large} services)\n")
        f.write(f"#    full   — {len(bpm_instances)} BPM + {NUM_BLM} BLM + {NUM_BCM} BCM ({n_full} services)\n")
        f.write("#\n")
        f.write("#  TUI tools (interactive, use with `docker compose run`):\n")
        f.write("#    inst-tui  — instrument dashboard (BPM/BLM/BCM)\n")
        f.write("#    redis-tui — Redis stream inspector\n")
        f.write("#\n")
        f.write("#  Generated by generate_system.py\n")
        f.write("#\n")
        f.write("#  Usage:\n")
        f.write("#    cd adapters && python3 generate_system.py\n")
        f.write("#    docker compose --profile bpm-demo up -d\n")
        f.write("#    docker compose --profile tiny up -d\n")
        f.write("#    docker compose --profile tui run --rm inst-tui\n")
        f.write("#    docker compose --profile tui run --rm inst-tui /inst-tui --config /etc/inst-tui/inst-tui-hosts-tiny.txt\n")
        f.write("#    docker compose --profile tui run --rm redis-tui /redis-tui redis-bpm-001:6379\n")
        f.write("#    docker compose --profile tiny down\n")
        f.write("#\n\n")
        yaml.dump(compose, f, default_flow_style=False, sort_keys=False)

    print(f"Generated system configs in {OUT_DIR}/")
    print(f"  BPM: {len(bpm_instances)} digitizers ({total_bpm_channels} channels)")
    print(f"    {NUM_BPM_8CH} x 8ch + {NUM_BPM_4CH} x 4ch")
    print(f"  BLM: {num_blm_digitizers} digitizers ({NUM_BLM} BLMs, {BLM_PER_DIGITIZER} per digitizer)")
    print(f"  BCM: {num_bcm_digitizers} digitizers ({NUM_BCM} BCMs, {BCM_PER_DIGITIZER} per digitizer)")
    print(f"  Redis: {num_redis} instances")
    print(f"  Total system: {n_services} services")
    print()
    print("  Demo profiles: pipeline, bpm-demo, blm-demo, bcm-demo")
    print()
    print("  System profiles:")
    print(f"    tiny   — {n_bpm_tiny} BPM + {n_blm_tiny} BLM + {n_bcm_tiny} BCM = {n_tiny} services")
    print(f"    small  — {n_bpm_small} BPM + {n_blm_small} BLM + {n_bcm_small} BCM = {n_small} services")
    print(f"    medium — {n_bpm_medium} BPM + {n_blm_medium} BLM + {n_bcm_medium} BCM = {n_medium} services")
    print(f"    large  — {n_bpm_large} BPM + {n_blm_large} BLM + {n_bcm_large} BCM = {n_large} services")
    print(f"    full   — {len(bpm_instances)} BPM + {NUM_BLM} BLM + {NUM_BCM} BCM = {n_full} services")
    print()
    print("  TUI tools: inst-tui, redis-tui (profile: tui)")
    print(f"Generated {COMPOSE_FILE}")


if __name__ == "__main__":
    main()
