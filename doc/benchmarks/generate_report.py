#!/usr/bin/env python3
"""
Generate benchmark comparison charts from Google Benchmark JSON output.

Usage: python3 generate_report.py
Output: doc/benchmarks/report.html
"""

import json
import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

SCRIPT_DIR = Path(__file__).parent
OUT_DIR = SCRIPT_DIR
REPORT_HTML = SCRIPT_DIR / "report.html"

# Colors
C_70 = '#2196F3'   # Blue - Redis 7.0
C_74 = '#9C27B0'   # Purple - Redis 7.4
C_80 = '#FF9800'   # Orange - Redis 8.0
C_86 = '#4CAF50'   # Green - Redis 8.6
C_TCP = '#1976D2'   # Dark blue
C_UDS = '#D32F2F'   # Red

VERSIONS = {
    'redis-7.0.15.json': '7.0.15',
    'redis-7.4.8.json': '7.4.8',
    'redis-8.0.2.json': '8.0.2',
    'redis-8.6.1.json': '8.6.1',
}

def load_benchmarks():
    """Load all benchmark JSON files."""
    data = {}
    for fname, label in VERSIONS.items():
        path = SCRIPT_DIR / fname
        if path.exists():
            with open(path) as f:
                raw = json.load(f)
            benchmarks = {}
            for b in raw.get('benchmarks', []):
                benchmarks[b['name']] = b
            data[label] = benchmarks
            print(f"Loaded {label}: {len(benchmarks)} benchmarks")
        else:
            print(f"Skipping {label}: {path} not found")
    return data


def get_time(benchmarks, name, unit='us'):
    """Extract real_time from benchmark, converting to desired unit."""
    b = benchmarks.get(name)
    if not b:
        return None
    ns = b['real_time']
    if unit == 'us':
        return ns / 1000
    elif unit == 'ms':
        return ns / 1_000_000
    return ns


def get_metric(benchmarks, name, metric):
    """Extract a user counter metric."""
    b = benchmarks.get(name)
    if not b:
        return None
    return b.get(metric)


def save_fig(fig, name):
    """Save figure as PNG."""
    path = OUT_DIR / f"{name}.png"
    fig.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print(f"  Saved {path.name}")
    return path.name


def chart_single_ops(data):
    """Bar chart: single operation latency across versions, TCP and UDS."""
    ops = [
        ('BM_TCP_AddDouble', 'addDouble'),
        ('BM_TCP_AddInt', 'addInt'),
        ('BM_TCP_AddString/16', 'addString\n(16B)'),
        ('BM_TCP_GetDouble', 'getDouble'),
        ('BM_TCP_GetInt', 'getInt'),
        ('BM_TCP_GetString/16', 'getString\n(16B)'),
        ('BM_TCP_Connected', 'PING'),
    ]

    versions = [v for v in VERSIONS.values() if v in data]
    colors = [C_70, C_74, C_80, C_86][:len(versions)]
    x = np.arange(len(ops))
    width = 0.8 / len(versions)

    fig, ax = plt.subplots(figsize=(14, 6))
    for i, ver in enumerate(versions):
        vals = [get_time(data[ver], op[0]) or 0 for op in ops]
        bars = ax.bar(x + i * width - (len(versions)-1)*width/2, vals, width,
                      label=f'Redis {ver}', color=colors[i], edgecolor='white', linewidth=0.5)
        for bar, val in zip(bars, vals):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                        f'{val:.0f}', ha='center', va='bottom', fontsize=7)

    ax.set_ylabel('Latency (us)')
    ax.set_title('Single Operation Latency — TCP (lower is better)')
    ax.set_xticks(x)
    ax.set_xticklabels([op[1] for op in ops])
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    return save_fig(fig, '01_single_ops_tcp')


def chart_single_ops_uds(data):
    """Bar chart: single operation latency across versions, UDS."""
    ops = [
        ('BM_UDS_AddDouble', 'addDouble'),
        ('BM_UDS_AddInt', 'addInt'),
        ('BM_UDS_AddString/16', 'addString\n(16B)'),
        ('BM_UDS_GetDouble', 'getDouble'),
        ('BM_UDS_GetInt', 'getInt'),
        ('BM_UDS_GetString/16', 'getString\n(16B)'),
        ('BM_UDS_Connected', 'PING'),
    ]

    versions = [v for v in VERSIONS.values() if v in data]
    colors = [C_70, C_74, C_80, C_86][:len(versions)]
    x = np.arange(len(ops))
    width = 0.8 / len(versions)

    fig, ax = plt.subplots(figsize=(14, 6))
    for i, ver in enumerate(versions):
        vals = [get_time(data[ver], op[0]) or 0 for op in ops]
        bars = ax.bar(x + i * width - (len(versions)-1)*width/2, vals, width,
                      label=f'Redis {ver}', color=colors[i], edgecolor='white', linewidth=0.5)
        for bar, val in zip(bars, vals):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                        f'{val:.0f}', ha='center', va='bottom', fontsize=7)

    ax.set_ylabel('Latency (us)')
    ax.set_title('Single Operation Latency — UDS (lower is better)')
    ax.set_xticks(x)
    ax.set_xticklabels([op[1] for op in ops])
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    return save_fig(fig, '02_single_ops_uds')


def chart_tcp_vs_uds(data):
    """Grouped bar chart: TCP vs UDS for best version (7.0.15)."""
    ops = [
        ('AddDouble', 'addDouble'),
        ('AddInt', 'addInt'),
        ('AddString/16', 'addString(16B)'),
        ('GetDouble', 'getDouble'),
        ('GetInt', 'getInt'),
        ('GetString/16', 'getString(16B)'),
        ('Connected', 'PING'),
        ('AddGetCycle', 'Add+Get'),
    ]

    ver = '7.0.15'
    if ver not in data:
        return None

    tcp_vals = [get_time(data[ver], f'BM_TCP_{op[0]}') or 0 for op in ops]
    uds_vals = [get_time(data[ver], f'BM_UDS_{op[0]}') or 0 for op in ops]

    x = np.arange(len(ops))
    width = 0.35

    fig, ax = plt.subplots(figsize=(14, 6))
    bars1 = ax.bar(x - width/2, tcp_vals, width, label='TCP', color=C_TCP, edgecolor='white')
    bars2 = ax.bar(x + width/2, uds_vals, width, label='UDS', color=C_UDS, edgecolor='white')

    for bar, val in zip(bars1, tcp_vals):
        if val > 0:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                    f'{val:.1f}', ha='center', va='bottom', fontsize=7)
    for bar, val in zip(bars2, uds_vals):
        if val > 0:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                    f'{val:.1f}', ha='center', va='bottom', fontsize=7)

    ax.set_ylabel('Latency (us)')
    ax.set_title(f'TCP vs UDS — Redis {ver} (lower is better)')
    ax.set_xticks(x)
    ax.set_xticklabels([op[1] for op in ops])
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    return save_fig(fig, '03_tcp_vs_uds')


def chart_range_queries(data):
    """Line chart: range query latency vs count, all versions, TCP."""
    counts = [10, 100, 1000]
    versions = [v for v in VERSIONS.values() if v in data]
    colors = [C_70, C_74, C_80, C_86][:len(versions)]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Forward range - TCP
    for i, ver in enumerate(versions):
        vals = [get_time(data[ver], f'BM_TCP_GetDoubles_Range/{c}') for c in counts]
        if any(v is not None for v in vals):
            ax1.plot(counts, [v or 0 for v in vals], 'o-', color=colors[i],
                     label=f'Redis {ver}', linewidth=2, markersize=6)
    ax1.set_xscale('log')
    ax1.set_yscale('log')
    ax1.set_xlabel('Entry count')
    ax1.set_ylabel('Latency (us)')
    ax1.set_title('Forward Range (XRANGE) — TCP')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(counts)
    ax1.get_xaxis().set_major_formatter(ticker.ScalarFormatter())

    # Forward range - UDS
    for i, ver in enumerate(versions):
        vals = [get_time(data[ver], f'BM_UDS_GetDoubles_Range/{c}') for c in counts]
        if any(v is not None for v in vals):
            ax2.plot(counts, [v or 0 for v in vals], 'o-', color=colors[i],
                     label=f'Redis {ver}', linewidth=2, markersize=6)
    ax2.set_xscale('log')
    ax2.set_yscale('log')
    ax2.set_xlabel('Entry count')
    ax2.set_ylabel('Latency (us)')
    ax2.set_title('Forward Range (XRANGE) — UDS')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(counts)
    ax2.get_xaxis().set_major_formatter(ticker.ScalarFormatter())

    fig.tight_layout()
    return save_fig(fig, '04_range_queries')


def chart_bulk_add(data):
    """Bar chart: bulk add throughput (items/s) across versions."""
    counts = [10, 100, 1000]
    versions = [v for v in VERSIONS.values() if v in data]
    colors_tcp = [C_70, C_74, C_80, C_86][:len(versions)]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # TCP
    x = np.arange(len(counts))
    width = 0.8 / len(versions)
    for i, ver in enumerate(versions):
        vals = []
        for c in counts:
            b = data[ver].get(f'BM_TCP_AddDoubles_Bulk/{c}')
            if b and 'items_per_second' in b:
                vals.append(b['items_per_second'] / 1000)  # k ops/s
            else:
                vals.append(0)
        ax1.bar(x + i*width - (len(versions)-1)*width/2, vals, width,
                label=f'Redis {ver}', color=colors_tcp[i], edgecolor='white')
    ax1.set_ylabel('Throughput (k items/s)')
    ax1.set_title('Bulk Add — TCP (higher is better)')
    ax1.set_xticks(x)
    ax1.set_xticklabels([str(c) for c in counts])
    ax1.set_xlabel('Batch size')
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)

    # UDS
    for i, ver in enumerate(versions):
        vals = []
        for c in counts:
            b = data[ver].get(f'BM_UDS_AddDoubles_Bulk/{c}')
            if b and 'items_per_second' in b:
                vals.append(b['items_per_second'] / 1000)
            else:
                vals.append(0)
        ax2.bar(x + i*width - (len(versions)-1)*width/2, vals, width,
                label=f'Redis {ver}', color=colors_tcp[i], edgecolor='white')
    ax2.set_ylabel('Throughput (k items/s)')
    ax2.set_title('Bulk Add — UDS (higher is better)')
    ax2.set_xticks(x)
    ax2.set_xticklabels([str(c) for c in counts])
    ax2.set_xlabel('Batch size')
    ax2.legend()
    ax2.grid(axis='y', alpha=0.3)

    fig.tight_layout()
    return save_fig(fig, '05_bulk_add')


def chart_blob_throughput(data):
    """Line chart: blob write throughput vs payload size."""
    sizes = [1024, 16384, 65536, 262144, 1048576]
    size_labels = ['1K', '16K', '64K', '256K', '1M']
    versions = [v for v in VERSIONS.values() if v in data]
    colors = [C_70, C_74, C_80, C_86][:len(versions)]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # TCP
    for i, ver in enumerate(versions):
        vals = []
        for s in sizes:
            b = data[ver].get(f'BM_TCP_Stress_LargeBlob/{s}/real_time')
            if b and 'bytes_per_second' in b:
                vals.append(b['bytes_per_second'] / (1024**3))  # GiB/s
            else:
                vals.append(0)
        if any(v > 0 for v in vals):
            ax1.plot(range(len(sizes)), vals, 'o-', color=colors[i],
                     label=f'Redis {ver}', linewidth=2, markersize=6)
    ax1.set_ylabel('Throughput (GiB/s)')
    ax1.set_title('Large Blob Write — TCP (higher is better)')
    ax1.set_xticks(range(len(sizes)))
    ax1.set_xticklabels(size_labels)
    ax1.set_xlabel('Payload size')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # UDS
    for i, ver in enumerate(versions):
        vals = []
        for s in sizes:
            b = data[ver].get(f'BM_UDS_Stress_LargeBlob/{s}/real_time')
            if b and 'bytes_per_second' in b:
                vals.append(b['bytes_per_second'] / (1024**3))
            else:
                vals.append(0)
        if any(v > 0 for v in vals):
            ax2.plot(range(len(sizes)), vals, 'o-', color=colors[i],
                     label=f'Redis {ver}', linewidth=2, markersize=6)
    ax2.set_ylabel('Throughput (GiB/s)')
    ax2.set_title('Large Blob Write — UDS (higher is better)')
    ax2.set_xticks(range(len(sizes)))
    ax2.set_xticklabels(size_labels)
    ax2.set_xlabel('Payload size')
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    fig.tight_layout()
    return save_fig(fig, '06_blob_throughput')


def chart_parallel_writers(data):
    """Line chart: parallel writer aggregate throughput."""
    threads = [1, 2, 4, 8]
    versions = [v for v in VERSIONS.values() if v in data]
    colors = [C_70, C_74, C_80, C_86][:len(versions)]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # TCP
    for i, ver in enumerate(versions):
        vals = []
        for t in threads:
            b = data[ver].get(f'BM_TCP_Stress_ParallelWriters/{t}/real_time')
            if b and 'items_per_second' in b:
                vals.append(b['items_per_second'] / 1000)
            else:
                vals.append(0)
        if any(v > 0 for v in vals):
            ax1.plot(threads, vals, 'o-', color=colors[i],
                     label=f'Redis {ver}', linewidth=2, markersize=6)
    ax1.set_xlabel('Writer threads')
    ax1.set_ylabel('Aggregate ops/s (thousands)')
    ax1.set_title('Parallel Writers — TCP (higher is better)')
    ax1.set_xticks(threads)
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # UDS
    for i, ver in enumerate(versions):
        vals = []
        for t in threads:
            b = data[ver].get(f'BM_UDS_Stress_ParallelWriters/{t}/real_time')
            if b and 'items_per_second' in b:
                vals.append(b['items_per_second'] / 1000)
            else:
                vals.append(0)
        if any(v > 0 for v in vals):
            ax2.plot(threads, vals, 'o-', color=colors[i],
                     label=f'Redis {ver}', linewidth=2, markersize=6)
    ax2.set_xlabel('Writer threads')
    ax2.set_ylabel('Aggregate ops/s (thousands)')
    ax2.set_title('Parallel Writers — UDS (higher is better)')
    ax2.set_xticks(threads)
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    fig.tight_layout()
    return save_fig(fig, '07_parallel_writers')


def chart_mixed_rw(data):
    """Line chart: mixed read/write throughput."""
    threads = [2, 4, 8, 16]
    versions = [v for v in VERSIONS.values() if v in data]
    colors = [C_70, C_74, C_80, C_86][:len(versions)]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    for ax, transport, title in [(ax1, 'TCP', 'TCP'), (ax2, 'UDS', 'UDS')]:
        for i, ver in enumerate(versions):
            vals = []
            for t in threads:
                b = data[ver].get(f'BM_{transport}_Stress_MixedReadWrite/{t}/real_time')
                if b and 'items_per_second' in b:
                    vals.append(b['items_per_second'] / 1000)
                else:
                    vals.append(0)
            if any(v > 0 for v in vals):
                ax.plot(threads, vals, 'o-', color=colors[i],
                        label=f'Redis {ver}', linewidth=2, markersize=6)
        ax.set_xlabel('Total threads (half writers, half readers)')
        ax.set_ylabel('Aggregate ops/s (thousands)')
        ax.set_title(f'Mixed Read/Write — {title} (higher is better)')
        ax.set_xticks(threads)
        ax.legend()
        ax.grid(True, alpha=0.3)

    fig.tight_layout()
    return save_fig(fig, '08_mixed_rw')


def chart_deep_stream(data):
    """Bar chart: deep stream query latency."""
    counts = [100, 1000, 10000, 50000]
    count_labels = ['100', '1K', '10K', '50K']
    versions = [v for v in VERSIONS.values() if v in data]
    colors = [C_70, C_74, C_80, C_86][:len(versions)]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    for ax, transport, title in [(ax1, 'TCP', 'TCP'), (ax2, 'UDS', 'UDS')]:
        x = np.arange(len(counts))
        width = 0.8 / len(versions)
        for i, ver in enumerate(versions):
            vals = [get_time(data[ver], f'BM_{transport}_Stress_DeepStream/{c}/real_time', 'ms') or 0
                    for c in counts]
            ax.bar(x + i*width - (len(versions)-1)*width/2, vals, width,
                   label=f'Redis {ver}', color=colors[i], edgecolor='white')
        ax.set_ylabel('Latency (ms)')
        ax.set_title(f'Deep Stream Query (100K entries) — {title} (lower is better)')
        ax.set_xticks(x)
        ax.set_xticklabels(count_labels)
        ax.set_xlabel('Query size')
        ax.legend()
        ax.grid(axis='y', alpha=0.3)

    fig.tight_layout()
    return save_fig(fig, '09_deep_stream')


def chart_many_streams(data):
    """Bar chart: many streams throughput."""
    counts = [10, 100, 1000]
    versions = [v for v in VERSIONS.values() if v in data]
    colors = [C_70, C_74, C_80, C_86][:len(versions)]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    for ax, transport, title in [(ax1, 'TCP', 'TCP'), (ax2, 'UDS', 'UDS')]:
        x = np.arange(len(counts))
        width = 0.8 / len(versions)
        for i, ver in enumerate(versions):
            vals = []
            for c in counts:
                b = data[ver].get(f'BM_{transport}_Stress_ManyStreams/{c}/real_time')
                if b and 'items_per_second' in b:
                    vals.append(b['items_per_second'] / 1000)
                else:
                    vals.append(0)
            ax.bar(x + i*width - (len(versions)-1)*width/2, vals, width,
                   label=f'Redis {ver}', color=colors[i], edgecolor='white')
        ax.set_ylabel('Throughput (k ops/s)')
        ax.set_title(f'Many Streams — {title} (higher is better)')
        ax.set_xticks(x)
        ax.set_xticklabels([str(c) for c in counts])
        ax.set_xlabel('Number of streams')
        ax.legend()
        ax.grid(axis='y', alpha=0.3)

    fig.tight_layout()
    return save_fig(fig, '10_many_streams')


def chart_serialization(data):
    """Bar chart: serialization helper latency (version-independent)."""
    ver = list(data.keys())[0]  # Any version, CPU-only
    ops = [
        ('BM_FromDouble', 'from_double'),
        ('BM_ToDouble', 'to_double'),
        ('BM_FromInt', 'from_int'),
        ('BM_ToInt', 'to_int'),
        ('BM_FromBlob/64', 'from_blob\n(64B)'),
        ('BM_FromBlob/1024', 'from_blob\n(1KiB)'),
        ('BM_TimeToStreamId', 'Time→ID'),
        ('BM_TimeFromStreamId', 'ID→Time'),
    ]

    vals = [get_time(data[ver], op[0], 'us') or 0 for op in ops]
    # Convert to ns for small values
    vals_ns = [(get_time(data[ver], op[0], 'us') or 0) * 1000 for op in ops]

    fig, ax = plt.subplots(figsize=(12, 5))
    bars = ax.bar(range(len(ops)), vals_ns, color='#7E57C2', edgecolor='white')
    for bar, val in zip(bars, vals_ns):
        if val > 0:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f'{val:.1f}', ha='center', va='bottom', fontsize=8)
    ax.set_ylabel('Latency (ns)')
    ax.set_title('Serialization & Timestamp Helpers — Pure CPU (lower is better)')
    ax.set_xticks(range(len(ops)))
    ax.set_xticklabels([op[1] for op in ops])
    ax.grid(axis='y', alpha=0.3)
    return save_fig(fig, '00_serialization')


def generate_html(charts):
    """Generate HTML report with all charts."""
    sections = [
        ('00_serialization.png', 'Serialization Helpers',
         'memcpy-based encode/decode performance. These are pure CPU — no Redis involved. '
         'Decoding (to_double, to_int) is ~2ns. Encoding allocates a std::string (~42ns).'),
        ('01_single_ops_tcp.png', 'Single Operations — TCP',
         'Latency for individual add/get operations over TCP loopback. '
         'Redis 7.0.15 is consistently ~2x faster than 8.x for single-threaded operations.'),
        ('02_single_ops_uds.png', 'Single Operations — UDS',
         'Same operations over Unix domain socket. UDS reduces latency by 5-8us vs TCP on 7.0.15.'),
        ('03_tcp_vs_uds.png', 'TCP vs UDS — Redis 7.0.15',
         'Direct transport comparison on the fastest Redis version. UDS wins by 1.3-1.9x on every operation.'),
        ('04_range_queries.png', 'Range Query Scaling',
         'How latency scales with query size (XRANGE). All versions converge at large batch sizes — '
         'the transport round-trip becomes negligible vs Redis-side serialization.'),
        ('05_bulk_add.png', 'Bulk Add Throughput',
         'Individual XADD calls in a loop. UDS advantage compounds linearly with batch size.'),
        ('06_blob_throughput.png', 'Large Blob Throughput',
         'Write throughput vs payload size. All versions converge at 1 MiB (~1.2 GiB/s) — '
         'memory copy and TCP/UDS bandwidth become the limit.'),
        ('07_parallel_writers.png', 'Parallel Writer Scaling',
         'Aggregate throughput with N concurrent writer threads. Redis 7.0.15+UDS peaks at ~250k ops/s. '
         'Redis 8.x io-threads help at 8 threads but don\'t overcome the single-op latency penalty.'),
        ('08_mixed_rw.png', 'Mixed Read/Write',
         'Half writers, half readers on the same stream. Simulates realistic workloads.'),
        ('09_deep_stream.png', 'Deep Stream Queries',
         'Range queries on a 100K-entry stream. Redis 8.x shows some improvement at small query sizes.'),
        ('10_many_streams.png', 'Many Streams (Key-Space Pressure)',
         'Write one entry to N distinct stream keys then delete. Tests key creation overhead.'),
    ]

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>RedisAdapterLite Benchmark Report</title>
<style>
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    max-width: 1200px;
    margin: 0 auto;
    padding: 20px;
    background: #fafafa;
    color: #333;
  }
  h1 { color: #1a1a1a; border-bottom: 3px solid #2196F3; padding-bottom: 10px; }
  h2 { color: #1565C0; margin-top: 40px; }
  .chart { margin: 20px 0; text-align: center; }
  .chart img { max-width: 100%; border: 1px solid #ddd; border-radius: 4px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
  .desc { color: #555; font-size: 0.95em; margin: 8px 0 20px 0; line-height: 1.5; }
  .meta { background: #e3f2fd; padding: 15px; border-radius: 6px; margin-bottom: 30px; }
  .meta code { background: #bbdefb; padding: 2px 6px; border-radius: 3px; }
  .takeaway { background: #e8f5e9; padding: 15px; border-radius: 6px; margin-top: 30px; }
  .takeaway h2 { color: #2E7D32; margin-top: 0; }
  .takeaway li { margin: 8px 0; line-height: 1.5; }
  .legend { display: flex; gap: 20px; margin: 10px 0; }
  .legend span { display: inline-flex; align-items: center; gap: 6px; }
  .dot { width: 12px; height: 12px; border-radius: 50%; display: inline-block; }
</style>
</head>
<body>
<h1>RedisAdapterLite Benchmark Report</h1>

<div class="meta">
  <strong>System:</strong> 20-core x86_64, 2.4-4.0 GHz, L3 8 MiB x2<br>
  <strong>Build:</strong> Release (<code>-O2</code>), Google Benchmark v1.9.1<br>
  <strong>Transports:</strong> TCP loopback, Unix domain socket<br>
  <strong>Persistence:</strong> Disabled (<code>save ""</code>, <code>appendonly no</code>)<br>
  <strong>Versions:</strong>
  <div class="legend">
    <span><span class="dot" style="background:#2196F3"></span> Redis 7.0.15 (system package)</span>
    <span><span class="dot" style="background:#9C27B0"></span> Redis 7.4.8 (built from source)</span>
    <span><span class="dot" style="background:#FF9800"></span> Redis 8.0.2 (io-threads 4)</span>
    <span><span class="dot" style="background:#4CAF50"></span> Redis 8.6.1 (io-threads 4)</span>
  </div>
</div>
"""

    for filename, title, desc in sections:
        if filename in charts:
            html += f"""
<h2>{title}</h2>
<p class="desc">{desc}</p>
<div class="chart"><img src="{filename}" alt="{title}"></div>
"""

    html += """
<div class="takeaway">
<h2>Key Takeaways</h2>
<ol>
<li><strong>Redis 7.0.15 is the fastest</strong> for single-threaded and low-thread workloads — ~2x faster per-operation than 8.x</li>
<li><strong>UDS is 1.3-1.9x faster than TCP</strong> on the same Redis version (saves 5-8us per round-trip)</li>
<li><strong>Best config: Redis 7.0.15 + UDS</strong> — peaks at ~100k single-thread ops/s and ~250k aggregate ops/s</li>
<li><strong>Redis 8.x io-threads help at high concurrency</strong> (8+ threads) but don't overcome the per-operation latency regression</li>
<li><strong>Serialization is free</strong> — memcpy helpers are 2-42ns vs 10-18us round-trip</li>
<li><strong>Transport advantage disappears for large batches</strong> — at 1000+ entry reads, server-side work dominates</li>
<li><strong>All versions converge at ~1.2 GiB/s</strong> for 1 MiB blob writes (memory copy limit)</li>
</ol>
</div>

<p style="color:#999; margin-top: 40px; font-size: 0.85em;">
Generated from Google Benchmark JSON data. Raw data in <code>doc/benchmarks/*.json</code>.
</p>
</body>
</html>"""

    with open(REPORT_HTML, 'w') as f:
        f.write(html)
    print(f"\nReport: {REPORT_HTML}")


def main():
    print("Loading benchmark data...")
    data = load_benchmarks()
    if not data:
        print("No benchmark data found!")
        sys.exit(1)

    print("\nGenerating charts...")
    charts = set()

    for gen in [chart_serialization, chart_single_ops, chart_single_ops_uds,
                chart_tcp_vs_uds, chart_range_queries, chart_bulk_add,
                chart_blob_throughput, chart_parallel_writers, chart_mixed_rw,
                chart_deep_stream, chart_many_streams]:
        try:
            result = gen(data)
            if result:
                charts.add(result)
        except Exception as e:
            print(f"  Warning: {gen.__name__} failed: {e}")

    print("\nGenerating HTML report...")
    generate_html(charts)
    print("Done!")


if __name__ == '__main__':
    main()
