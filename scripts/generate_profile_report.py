#!/usr/bin/env python3
"""
generate_profile_report.py

Generates an HTML performance report from:
  1. Google Benchmark JSON output  (--benchmark-json)
  2. gprof flat profile text       (--gprof-txt)

Usage:
  # Run benchmarks with JSON output:
  ./ral-benchmark --benchmark_format=json --benchmark_out=benchmark_results.json

  # Run gprof:
  gprof ral-benchmark gmon.out > gprof_report.txt

  # Generate report:
  python3 scripts/generate_profile_report.py \
      --benchmark-json build-profile/benchmark_results.json \
      --gprof-txt      build-profile/gprof_report.txt \
      --output          build-profile/profile_report.html
"""

import argparse
import json
import re
import sys
import html
from datetime import datetime
from pathlib import Path


# ── gprof parser ──────────────────────────────────────────────────────────

def parse_gprof(path: str) -> list[dict]:
    """Parse gprof flat profile into a list of dicts."""
    rows = []
    in_table = False
    with open(path) as f:
        for line in f:
            line = line.rstrip()
            # gprof header is two lines: "%  cumulative  self..." then "time  seconds  seconds..."
            if line.lstrip().startswith("time") and "seconds" in line and "name" in line:
                in_table = True
                continue
            if in_table:
                if not line.strip():
                    break
                # Format: %time  cum_s  self_s  [calls  self_ms  total_ms]  name
                # When calls is blank, columns 3-5 are empty whitespace.
                # Use fixed-width parsing: first 3 floats, then check if calls present
                parts = line.split()
                if len(parts) < 4:
                    continue
                try:
                    pct = float(parts[0])
                except ValueError:
                    continue
                cum = float(parts[1])
                self_s = float(parts[2])
                # Try to parse parts[3] as int (calls count)
                try:
                    calls = int(parts[3])
                    # calls present: parts[4]=self_ms, parts[5]=total_ms, parts[6:]=name
                    name = " ".join(parts[6:]) if len(parts) > 6 else (parts[5] if len(parts) > 5 else "?")
                except ValueError:
                    # No calls — parts[3:] is the function name
                    calls = 0
                    name = " ".join(parts[3:])
                rows.append(dict(pct=pct, cum=cum, self_s=self_s, calls=calls, name=name))
    return rows


def classify_function(name: str) -> tuple[str, str, str]:
    """Return (short_name, layer, css_class) for a gprof function name."""
    # Simplify C++ mangled names
    short = name

    # Strip std:: template noise
    if "std::_Hashtable" in name or "std::unordered_map" in name:
        if "clear" in name:
            short = "unordered_map::clear()"
        elif "_M_rehash" in name:
            short = "unordered_map::_M_rehash()"
        elif "_M_insert_unique_node" in name:
            short = "unordered_map::_M_insert_unique_node()"
        elif "_Hashtable<" in name and "const*>" in name:
            short = "unordered_map::_Hashtable() [range construct]"
        elif "find" in name:
            short = "unordered_map::find()"
        else:
            short = "unordered_map::" + name.split("::")[-1].split("(")[0] + "()"
        return short, "STL", "tag-stl"

    if "std::vector" in name:
        if "_M_realloc_insert" in name:
            # Try to extract the element type
            if "TimeAttrsList" in name or "unordered_map" in name:
                short = "vector<TimeAttrs>::_M_realloc_insert()"
            elif "double" in name:
                short = "vector<TimeDouble>::_M_realloc_insert()"
            elif "RAL_Time" in name:
                short = "vector<RAL_Time>::_M_realloc_insert()"
            else:
                short = "vector::_M_realloc_insert()"
        elif "reserve" in name:
            if "char const*" in name:
                short = "vector<const char*>::reserve()"
            elif "unsigned long" in name:
                short = "vector<size_t>::reserve()"
            else:
                short = "vector::reserve()"
        elif "~vector" in name:
            short = "vector<TimeAttrs>::~vector()"
        else:
            short = "vector::" + name.split("::")[-1].split("(")[0] + "()"
        return short, "STL", "tag-stl"

    if "std::__cxx11::basic_string" in name:
        short = "string::string() [constprop]"
        return short, "STL", "tag-stl"

    if "std::thread" in name:
        short = "thread::~_State_impl()"
        return short, "STL", "tag-stl"

    if "std::mersenne_twister" in name:
        short = "mt19937::_M_gen_rand()"
        return short, "STL", "tag-stl"

    # RAL functions
    for prefix in ["RAL_Time::", "RedisAdapterLite::", "HiredisConnection::", "parse_entry_fields", "parse_stream_reply", "parse_xread_reply", "ThreadPool"]:
        if prefix in name:
            # Clean up the name
            short = re.sub(r'std::__cxx11::basic_string<.*?>', 'string', name)
            short = re.sub(r'std::unordered_map<.*?>', 'Attrs', short)
            short = re.sub(r'std::vector<.*?>', 'vector', short)
            short = re.sub(r'\[abi:cxx11\]', '', short)
            short = short.strip()
            if len(short) > 80:
                # Further simplify
                m = re.match(r'([\w:]+::\w+)\(', short)
                if m:
                    short = m.group(1) + "(...)"
                else:
                    short = short[:80] + "..."
            return short, "RAL", "tag-ral"

    # hiredis internals
    hiredis_funcs = ["redis", "freeReplyObject", "createStringObject", "createArrayObject", "sds"]
    for hf in hiredis_funcs:
        if name.startswith(hf) or name.startswith("free") and "Reply" in name:
            return name, "hiredis", "tag-hiredis"
        if hf in name:
            return name, "hiredis", "tag-hiredis"

    # Benchmark harness
    if "benchmark::" in name or name.startswith("BM_") or name.startswith("Stress_"):
        return name.split("(")[0] + "()", "benchmark", "tag-bench"

    # System / other
    if name.startswith("__") or name == "_init":
        return name, "system", "tag-sys"

    return name if len(name) < 60 else name[:57] + "...", "other", "tag-other"


# ── benchmark parser ──────────────────────────────────────────────────────

def parse_benchmarks(path: str) -> list[dict]:
    """Parse Google Benchmark JSON output."""
    with open(path) as f:
        data = json.load(f)
    return data.get("benchmarks", [])


def fmt_time(ns: float) -> str:
    """Format nanoseconds into human-readable string."""
    if ns < 1000:
        return f"{ns:.1f} ns"
    elif ns < 1_000_000:
        return f"{ns/1000:.1f} &micro;s"
    elif ns < 1_000_000_000:
        return f"{ns/1_000_000:.2f} ms"
    else:
        return f"{ns/1_000_000_000:.2f} s"


def fmt_rate(val: float, unit: str) -> str:
    """Format a rate value."""
    if unit == "bytes_per_second":
        if val >= 1024**3:
            return f"{val/1024**3:.1f} GB/s"
        elif val >= 1024**2:
            return f"{val/1024**2:.1f} MB/s"
        elif val >= 1024:
            return f"{val/1024:.1f} KB/s"
        else:
            return f"{val:.0f} B/s"
    elif unit == "items_per_second":
        if val >= 1_000_000:
            return f"{val/1_000_000:.2f}M/s"
        elif val >= 1_000:
            return f"{val/1_000:.1f}K/s"
        else:
            return f"{val:.0f}/s"
    return f"{val:.2f}"


def fmt_calls(n: int) -> str:
    """Format call count."""
    if n == 0:
        return "—"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def categorize_benchmark(name: str) -> str:
    """Return a category for grouping benchmarks."""
    if name.startswith("BM_From") or name.startswith("BM_To") or name.startswith("BM_Time"):
        return "Serialization"
    if "Stress" in name:
        return "Stress Tests"
    if "Add" in name and "Get" not in name:
        if "Bulk" in name or "Doubles" in name and "Range" not in name:
            return "Bulk Add"
        if "Attrs" in name:
            return "Attrs Operations"
        return "Single Value Add"
    if "Get" in name:
        if "Range" in name or "Before" in name:
            return "Range Queries"
        if "Attrs" in name:
            return "Attrs Operations"
        return "Single Value Get"
    if "Connected" in name:
        return "Connection"
    if "Cycle" in name:
        return "Add+Get Cycle"
    if "ManyStreams" in name:
        return "Stress Tests"
    return "Other"


def extract_throughput(bm: dict) -> tuple[str, str]:
    """Extract throughput metric and unit from a benchmark entry."""
    for key in ["bytes_per_second", "items_per_second"]:
        if key in bm:
            return fmt_rate(bm[key], key), key
    return "", ""


# ── HTML generation ───────────────────────────────────────────────────────

CSS = """
:root { --bg: #0d1117; --card: #161b22; --border: #30363d; --text: #c9d1d9;
        --text2: #8b949e; --accent: #58a6ff; --green: #3fb950; --yellow: #d29922;
        --red: #f85149; --orange: #db6d28; }
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
       background: var(--bg); color: var(--text); line-height: 1.6; padding: 2rem; max-width: 1200px; margin: 0 auto; }
h1 { font-size: 1.8rem; margin-bottom: 0.3rem; }
h2 { font-size: 1.3rem; margin: 2rem 0 1rem; color: var(--accent); border-bottom: 1px solid var(--border); padding-bottom: 0.4rem; }
h3 { font-size: 1.05rem; margin: 1.5rem 0 0.5rem; color: var(--text); }
.subtitle { color: var(--text2); font-size: 0.9rem; margin-bottom: 2rem; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin: 1rem 0; }
.stat-card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 1rem; }
.stat-card .label { color: var(--text2); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; }
.stat-card .value { font-size: 1.6rem; font-weight: 700; margin: 0.2rem 0; }
.stat-card .detail { color: var(--text2); font-size: 0.8rem; }
table { width: 100%; border-collapse: collapse; background: var(--card); border-radius: 8px; overflow: hidden; margin: 0.5rem 0 1.5rem; }
th { background: #1c2128; text-align: left; padding: 0.5rem 0.8rem; font-size: 0.75rem; text-transform: uppercase;
     letter-spacing: 0.05em; color: var(--text2); border-bottom: 1px solid var(--border); }
td { padding: 0.4rem 0.8rem; border-bottom: 1px solid var(--border); font-size: 0.85rem; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: #1c2128; }
.mono { font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace; font-size: 0.82rem; }
.bar-container { width: 100%; height: 6px; background: var(--border); border-radius: 3px; overflow: hidden; display: inline-block; vertical-align: middle; }
.bar { height: 100%; border-radius: 3px; }
.tag { display: inline-block; padding: 0.1rem 0.45rem; border-radius: 10px; font-size: 0.7rem; font-weight: 600; }
.tag-hiredis { background: #3b1f2b; color: var(--red); }
.tag-ral { background: #1b3a4b; color: var(--accent); }
.tag-stl { background: #2a2f1a; color: var(--yellow); }
.tag-redis { background: #1a2f1a; color: var(--green); }
.tag-bench { background: #2a2a2a; color: var(--text2); }
.tag-sys { background: #1a1a2a; color: #7c7caa; }
.tag-other { background: #222; color: var(--text2); }
.right { text-align: right; }
.nowrap { white-space: nowrap; }
.insight { background: var(--card); border-left: 3px solid var(--accent); padding: 0.8rem 1rem; margin: 0.8rem 0; border-radius: 0 6px 6px 0; font-size: 0.9rem; }
.insight-warn { border-left-color: var(--yellow); }
.section { margin-bottom: 2.5rem; }
footer { margin-top: 3rem; padding-top: 1rem; border-top: 1px solid var(--border); color: var(--text2); font-size: 0.75rem; }
"""


def bar_color(pct: float) -> str:
    if pct >= 8: return "var(--red)"
    if pct >= 4: return "var(--orange)"
    if pct >= 2: return "var(--yellow)"
    return "var(--accent)"


def generate_html(benchmarks: list[dict], gprof_rows: list[dict], output: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ── compute summary stats from benchmarks ──
    stats = {}
    for bm in benchmarks:
        n = bm["name"]
        t = bm.get("real_time", bm.get("cpu_time", 0))
        if n == "BM_TCP_AddDouble":
            stats["add_latency"] = fmt_time(t)
        elif n == "BM_TCP_GetDouble":
            stats["get_latency"] = fmt_time(t)
        elif n == "BM_FromDouble":
            stats["ser_time"] = fmt_time(t)
        elif n == "BM_ToDouble":
            stats["deser_time"] = fmt_time(t)
        elif n == "BM_TCP_AddBlob/1048576":
            stats["blob_tp"] = extract_throughput(bm)[0]
        elif n == "BM_TCP_Connected":
            stats["ping"] = fmt_time(t)

    total_time = gprof_rows[-1]["cum"] if gprof_rows else 0

    parts = []
    parts.append("<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'>")
    parts.append("<title>RedisAdapterLite — Performance Profile</title>")
    parts.append(f"<style>{CSS}</style></head><body>")

    parts.append("<h1>RedisAdapterLite — Performance Profile</h1>")
    parts.append(f"<p class='subtitle'>gprof + Google Benchmark &middot; TCP transport &middot; {now}</p>")

    # ── summary cards ──
    parts.append("<div class='grid'>")
    cards = [
        ("Total Profile Time", f"{total_time:.2f}s", "CPU time under benchmark"),
        ("Add Latency", stats.get("add_latency", "—"), "Single double add (TCP)"),
        ("Get Latency", stats.get("get_latency", "—"), "Single double get (TCP)"),
        ("Serialization", stats.get("ser_time", "—"), "ral_from_double"),
        ("Deserialization", stats.get("deser_time", "—"), "ral_to_double (memcpy)"),
        ("Blob Throughput", stats.get("blob_tp", "—"), "1 MB blobs over TCP"),
        ("PING Latency", stats.get("ping", "—"), "connected() round-trip"),
    ]
    for label, value, detail in cards:
        parts.append(f"<div class='stat-card'><div class='label'>{label}</div>"
                     f"<div class='value'>{value}</div><div class='detail'>{detail}</div></div>")
    parts.append("</div>")

    # ── gprof hotspots ──
    if gprof_rows:
        parts.append("<div class='section'><h2>CPU Hotspots (gprof flat profile)</h2>")
        parts.append("<p style='color:var(--text2);font-size:0.82rem;margin-bottom:0.6rem'>")
        parts.append("Functions consuming &ge;0.5% of total CPU time, grouped by subsystem.</p>")

        # Compute subsystem totals
        subsystem_pct: dict[str, float] = {}
        filtered = [r for r in gprof_rows if r["pct"] >= 0.5]
        for r in gprof_rows:
            _, layer, _ = classify_function(r["name"])
            subsystem_pct[layer] = subsystem_pct.get(layer, 0) + r["pct"]

        parts.append("<table><thead><tr>")
        parts.append("<th style='width:5%'>%</th><th style='width:6%'>Self</th>")
        parts.append("<th style='width:7%'>Calls</th><th style='width:20%'>Bar</th>")
        parts.append("<th>Function</th><th style='width:8%'>Layer</th>")
        parts.append("</tr></thead><tbody>")

        for r in filtered:
            short, layer, css = classify_function(r["name"])
            color = bar_color(r["pct"])
            width = min(r["pct"] * 3, 100)  # scale for visual
            parts.append(f"<tr>")
            parts.append(f"<td class='right'>{r['pct']:.1f}%</td>")
            parts.append(f"<td class='right nowrap'>{r['self_s']:.2f}s</td>")
            parts.append(f"<td class='right mono'>{fmt_calls(r['calls'])}</td>")
            parts.append(f"<td><div class='bar-container'><div class='bar' style='width:{width}%;background:{color}'></div></div></td>")
            parts.append(f"<td class='mono'>{html.escape(short)}</td>")
            parts.append(f"<td><span class='tag {css}'>{html.escape(layer)}</span></td>")
            parts.append("</tr>")

        parts.append("</tbody></table>")

        # Subsystem summary
        parts.append("<div class='insight'>")
        parts.append("<strong>Time by subsystem:</strong> &nbsp; ")
        layer_order = sorted(subsystem_pct.items(), key=lambda x: -x[1])
        css_map = {"hiredis": "tag-hiredis", "RAL": "tag-ral", "STL": "tag-stl",
                    "benchmark": "tag-bench", "system": "tag-sys", "other": "tag-other"}
        for layer, pct in layer_order:
            css = css_map.get(layer, "tag-other")
            parts.append(f"<span class='tag {css}'>{html.escape(layer)}</span> {pct:.1f}% &nbsp; ")
        parts.append("</div></div>")

    # ── benchmark results ──
    if benchmarks:
        # Group by category
        categories: dict[str, list] = {}
        for bm in benchmarks:
            if bm.get("run_type") == "aggregate":
                continue
            cat = categorize_benchmark(bm["name"])
            categories.setdefault(cat, []).append(bm)

        cat_order = ["Serialization", "Single Value Add", "Single Value Get",
                     "Attrs Operations", "Range Queries", "Bulk Add",
                     "Add+Get Cycle", "Connection", "Stress Tests", "Other"]

        parts.append("<div class='section'><h2>Benchmark Results</h2>")

        for cat in cat_order:
            bms = categories.get(cat, [])
            if not bms:
                continue

            parts.append(f"<h3>{html.escape(cat)}</h3>")
            parts.append("<table><thead><tr>")
            parts.append("<th>Benchmark</th><th class='right'>Time (wall)</th>")
            parts.append("<th class='right'>CPU</th><th class='right'>Iterations</th>")
            parts.append("<th class='right'>Throughput</th>")
            parts.append("</tr></thead><tbody>")

            for bm in bms:
                name = bm["name"]
                # Clean up name
                display = name.replace("BM_TCP_", "").replace("BM_UDS_", "[UDS] ").replace("BM_", "")
                real = bm.get("real_time", bm.get("cpu_time", 0))
                cpu = bm.get("cpu_time", 0)
                iters = bm.get("iterations", 0)
                tp, _ = extract_throughput(bm)
                time_unit = bm.get("time_unit", "ns")

                # Convert to ns for consistent formatting
                multiplier = {"ns": 1, "us": 1000, "ms": 1_000_000, "s": 1_000_000_000}.get(time_unit, 1)
                real_ns = real * multiplier
                cpu_ns = cpu * multiplier

                parts.append(f"<tr>")
                parts.append(f"<td class='mono'>{html.escape(display)}</td>")
                parts.append(f"<td class='right nowrap'>{fmt_time(real_ns)}</td>")
                parts.append(f"<td class='right nowrap'>{fmt_time(cpu_ns)}</td>")
                parts.append(f"<td class='right mono'>{iters:,}</td>")
                parts.append(f"<td class='right nowrap'>{tp if tp else '—'}</td>")
                parts.append("</tr>")

            parts.append("</tbody></table>")

        parts.append("</div>")

    # ── recommendations ──
    parts.append("<div class='section'><h2>Optimization Recommendations</h2>")
    recommendations = [
        ("Pipeline bulk operations",
         "<code>addDoubles()</code> issues N separate round-trips. "
         "Redis pipelining (batch send, batch receive) could reduce latency by ~10&times;."),
        ("Flatten single-field Attrs",
         "<code>unordered_map</code> construction/hashing accounts for ~14% of CPU. "
         "Single-value ops always use field <code>\"_\"</code> — a specialized path would eliminate this overhead."),
        ("Pre-allocate parse vectors",
         "<code>vector&lt;TimeAttrs&gt;::_M_realloc_insert</code> (~3.5%) can be avoided by calling "
         "<code>reserve(reply-&gt;elements)</code> since the count is known from the Redis reply."),
        ("Consider UDS transport",
         "Unix domain sockets eliminate TCP overhead. If Redis is local, "
         "UDS can reduce per-operation latency by 30–50%."),
    ]
    for title, body in recommendations:
        parts.append(f"<div class='insight insight-warn'><strong>{title}</strong><br>{body}</div>")
    parts.append("</div>")

    parts.append(f"<footer>Generated by <code>scripts/generate_profile_report.py</code> &middot; {now}</footer>")
    parts.append("</body></html>")

    Path(output).parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w") as f:
        f.write("\n".join(parts))
    print(f"Report written to {output}")


# ── main ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate HTML performance profile report")
    parser.add_argument("--benchmark-json", required=True,
                        help="Path to Google Benchmark JSON output")
    parser.add_argument("--gprof-txt", required=True,
                        help="Path to gprof flat profile text (gprof -b binary gmon.out)")
    parser.add_argument("--output", default="profile_report.html",
                        help="Output HTML file path (default: profile_report.html)")
    args = parser.parse_args()

    gprof_rows = []
    if Path(args.gprof_txt).exists():
        gprof_rows = parse_gprof(args.gprof_txt)
        print(f"Parsed {len(gprof_rows)} gprof entries")
    else:
        print(f"Warning: gprof file not found: {args.gprof_txt}", file=sys.stderr)

    benchmarks = []
    if Path(args.benchmark_json).exists():
        benchmarks = parse_benchmarks(args.benchmark_json)
        print(f"Parsed {len(benchmarks)} benchmark entries")
    else:
        print(f"Warning: benchmark JSON not found: {args.benchmark_json}", file=sys.stderr)

    if not gprof_rows and not benchmarks:
        print("Error: no data to report", file=sys.stderr)
        sys.exit(1)

    generate_html(benchmarks, gprof_rows, args.output)


if __name__ == "__main__":
    main()
