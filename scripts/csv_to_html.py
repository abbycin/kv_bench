#!/usr/bin/env python3

import argparse
import base64
import csv
import json
import re
import statistics
import tomllib
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any


ENGINE_STYLE_FALLBACK = [
    "solid",
    "hatch",
    "dot",
]

WORKLOAD_TEMPLATE = [
    ("W1", "W1 (95R/5U, uniform)"),
    ("W2", "W2 (95R/5U, zipf)"),
    ("W3", "W3 (50R/50U, uniform)"),
    ("W4", "W4 (5R/95U, uniform)"),
    ("W5", "W5 (70R/25U/5S, uniform)"),
    ("W6", "W6 (100% scan, uniform)"),
]
WORKLOAD_LABELS = dict(WORKLOAD_TEMPLATE)
TEST_TOOL_SOURCE_URL = "https://github.com/abbycin/kv_bench"


SEMVER_RE = re.compile(r"^v?(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:[-+].*)?$")


def parse_semver(version: str) -> tuple[int, int, int] | None:
    m = SEMVER_RE.match(version.strip())
    if not m:
        return None
    major = int(m.group(1))
    minor = int(m.group(2) or "0")
    patch = int(m.group(3) or "0")
    return (major, minor, patch)


def parse_semver_with_components(version: str) -> tuple[tuple[int, int, int], int] | None:
    m = SEMVER_RE.match(version.strip())
    if not m:
        return None
    major = int(m.group(1))
    has_minor = m.group(2) is not None
    has_patch = m.group(3) is not None
    minor = int(m.group(2) or "0")
    patch = int(m.group(3) or "0")
    count = 1 + int(has_minor) + int(has_patch)
    return (major, minor, patch), count


def wildcard_bounds(token: str) -> tuple[tuple[int, int, int], tuple[int, int, int]] | None:
    parts = [p.strip() for p in token.split(".")]
    prefix: list[int] = []
    wildcard_seen = False
    for part in parts:
        if part in ("*", "x", "X"):
            wildcard_seen = True
            break
        if not part.isdigit():
            return None
        prefix.append(int(part))
    if not wildcard_seen:
        return None
    if not prefix:
        return ((0, 0, 0), (10**9, 0, 0))

    lower = (prefix + [0, 0, 0])[:3]
    if len(prefix) == 1:
        upper = (prefix[0] + 1, 0, 0)
    elif len(prefix) == 2:
        upper = (prefix[0], prefix[1] + 1, 0)
    else:
        upper = (prefix[0], prefix[1], prefix[2] + 1)
    return ((lower[0], lower[1], lower[2]), upper)


def version_satisfies_clause(version: tuple[int, int, int], clause: str) -> bool:
    part = clause.strip()
    if not part:
        return True
    if part in ("*", "x", "X"):
        return True

    if part.startswith("^"):
        parsed = parse_semver(part[1:].strip())
        if parsed is None:
            return False
        major, minor, patch = parsed
        if major > 0:
            upper = (major + 1, 0, 0)
        elif minor > 0:
            upper = (0, minor + 1, 0)
        else:
            upper = (0, 0, patch + 1)
        return version >= parsed and version < upper

    if part.startswith("~"):
        parsed_with_count = parse_semver_with_components(part[1:].strip())
        if parsed_with_count is None:
            return False
        parsed, count = parsed_with_count
        major, minor, _ = parsed
        if count <= 1:
            upper = (major + 1, 0, 0)
        else:
            upper = (major, minor + 1, 0)
        return version >= parsed and version < upper

    wildcard = wildcard_bounds(part)
    if wildcard is not None:
        lower, upper = wildcard
        return version >= lower and version < upper

    m = re.match(r"^(>=|<=|>|<|=)?\s*(.+)$", part)
    if not m:
        return False
    op = m.group(1)
    raw_token = m.group(2).strip()
    token = parse_semver(raw_token)
    if token is None:
        return False

    if op == ">=":
        return version >= token
    if op == "<=":
        return version <= token
    if op == ">":
        return version > token
    if op == "<":
        return version < token
    if op == "=":
        return version == token

    # Cargo default when no operator is caret.
    return version_satisfies_clause(version, f"^{raw_token}")


def version_satisfies_requirement(version_text: str, requirement: str) -> bool:
    version = parse_semver(version_text)
    if version is None:
        return False
    req = requirement.strip()
    if not req or req == "*":
        return True
    clauses = [part.strip() for part in req.split(",")]
    return all(version_satisfies_clause(version, clause) for clause in clauses)


def resolve_crates_io_version(crate_name: str, requirement: str) -> str:
    url = f"https://crates.io/api/v1/crates/{crate_name}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "kv_bench-csv_to_html",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, json.JSONDecodeError):
        return "unknown"

    candidates: list[str] = []
    for item in payload.get("versions", []):
        ver = item.get("num")
        if not isinstance(ver, str):
            continue
        if item.get("yanked", False):
            continue
        if "-" in ver:
            continue
        if version_satisfies_requirement(ver, requirement):
            candidates.append(ver)

    if not candidates:
        return "unknown"

    candidates.sort(key=lambda v: parse_semver(v) or (0, 0, 0), reverse=True)
    return candidates[0]


def resolve_lockfile_version(
    repo_root: Path,
    crate_name: str,
    requirement: str,
) -> str:
    lock_path = repo_root / "Cargo.lock"
    if not lock_path.exists():
        return "unknown"
    try:
        with lock_path.open("rb") as f:
            lock_obj = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError):
        return "unknown"

    versions: list[str] = []
    packages = lock_obj.get("package", [])
    if not isinstance(packages, list):
        return "unknown"
    for pkg in packages:
        if not isinstance(pkg, dict):
            continue
        if pkg.get("name") != crate_name:
            continue
        ver = pkg.get("version")
        if not isinstance(ver, str):
            continue
        if "-" in ver:
            continue
        if version_satisfies_requirement(ver, requirement):
            versions.append(ver)

    if not versions:
        return "unknown"

    versions.sort(key=lambda v: parse_semver(v) or (0, 0, 0), reverse=True)
    return versions[0]


def resolve_git_head(repo_path: Path) -> str:
    git_dir = repo_path / ".git"
    head_path = git_dir / "HEAD"
    if not head_path.exists():
        return "unknown"
    try:
        head_text = head_path.read_text(encoding="utf-8").strip()
    except OSError:
        return "unknown"

    if head_text.startswith("ref: "):
        ref_rel = head_text[5:].strip()
        ref_path = git_dir / ref_rel
        if ref_path.exists():
            try:
                return ref_path.read_text(encoding="utf-8").strip()
            except OSError:
                return "unknown"
        packed_refs = git_dir / "packed-refs"
        if packed_refs.exists():
            try:
                for line in packed_refs.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line or line.startswith("#") or line.startswith("^"):
                        continue
                    parts = line.split(" ")
                    if len(parts) == 2 and parts[1] == ref_rel:
                        return parts[0]
            except OSError:
                return "unknown"
        return "unknown"
    return head_text


def infer_mace_identity(repo_root: Path) -> tuple[str, str]:
    cargo_path = repo_root / "Cargo.toml"
    if not cargo_path.exists():
        return ("mace commit id", "unknown")

    try:
        with cargo_path.open("rb") as f:
            cargo_obj = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError):
        return ("mace commit id", "unknown")

    deps = cargo_obj.get("dependencies", {})
    if not isinstance(deps, dict):
        return ("mace commit id", "unknown")
    mace_dep = deps.get("mace-kv")
    if mace_dep is None:
        return ("mace commit id", "unknown")

    if isinstance(mace_dep, str):
        requirement = mace_dep.strip() or "*"
        lock_version = resolve_lockfile_version(repo_root, "mace-kv", requirement)
        if lock_version != "unknown":
            return ("mace version", lock_version)
        return ("mace version", resolve_crates_io_version("mace-kv", requirement))

    if isinstance(mace_dep, dict):
        path_value = mace_dep.get("path")
        if isinstance(path_value, str) and path_value.strip():
            dep_path = Path(path_value.strip())
            mace_repo = dep_path if dep_path.is_absolute() else (repo_root / dep_path)
            return ("mace commit id", resolve_git_head(mace_repo.resolve()))
        version_req = mace_dep.get("version")
        if isinstance(version_req, str) and version_req.strip():
            requirement = version_req.strip()
            lock_version = resolve_lockfile_version(repo_root, "mace-kv", requirement)
            if lock_version != "unknown":
                return ("mace version", lock_version)
            return ("mace version", resolve_crates_io_version("mace-kv", requirement))

    return ("mace commit id", "unknown")


def infer_rocksdb_version(repo_root: Path) -> str:
    vcpkg_path = repo_root / "rocksdb" / "vcpkg.json"
    if not vcpkg_path.exists():
        return "unknown"

    try:
        obj = json.loads(vcpkg_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "unknown"

    for item in obj.get("overrides", []):
        if item.get("name") == "rocksdb" and isinstance(item.get("version"), str):
            return item["version"]

    return "unknown"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a single-page HTML report from benchmark CSV. "
            "Each workload gets two line charts: ops and p99_us."
        )
    )
    parser.add_argument("csv_path", help="Input CSV path, e.g. benchmark_results.csv")
    parser.add_argument(
        "-o",
        "--output",
        help="Output HTML path (default: <csv_stem>_report.html)",
    )
    return parser.parse_args()


def to_int(value: str) -> int:
    return int(float(value))


def to_float(value: str) -> float:
    return float(value)


def format_metric(metric: str, value: float) -> str:
    if metric == "ops":
        if abs(value) >= 100:
            return f"{value:,.0f}"
        return f"{value:.2f}"
    if abs(value) >= 100:
        return f"{value:,.0f}"
    return f"{value:.2f}"


def workload_sort_key(workload: str) -> tuple[int, str, int]:
    prefix = ""
    suffix = ""
    for idx, ch in enumerate(workload):
        if ch.isdigit():
            prefix = workload[:idx]
            suffix = workload[idx:]
            break
    if suffix.isdigit():
        return (0, prefix, int(suffix))
    return (1, workload, 0)


def workload_label(workload: str) -> str:
    return WORKLOAD_LABELS.get(workload, workload)


def engine_style(engine: str, index: int) -> str:
    normalized = engine.strip().lower()
    if normalized == "mace":
        return "solid"
    if normalized == "rocksdb":
        return "hatch"
    return ENGINE_STYLE_FALLBACK[index % len(ENGINE_STYLE_FALLBACK)]


def color_for_index(index: int) -> str:
    hue = (index * 137.508) % 360.0
    return f"hsl({hue:.1f}, 70%, 45%)"


def read_and_aggregate(csv_path: Path) -> tuple[list[dict[str, Any]], set[str], set[tuple[int, int]]]:
    required = {
        "engine",
        "workload_id",
        "threads",
        "key_size",
        "value_size",
        "ops",
        "p99_us",
    }

    grouped: dict[tuple[str, str, int, int, int], dict[str, list[float]]] = defaultdict(
        lambda: {"ops": [], "p99_us": []}
    )
    engines: set[str] = set()
    kv_pairs: set[tuple[int, int]] = set()

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("CSV header is missing")

        missing = required - set(reader.fieldnames)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

        skipped = 0
        for row in reader:
            try:
                engine = str(row["engine"]).strip()
                workload = str(row["workload_id"]).strip()
                threads = to_int(str(row["threads"]))
                key_size = to_int(str(row["key_size"]))
                value_size = to_int(str(row["value_size"]))
                ops = to_float(str(row["ops"]))
                p99 = to_float(str(row["p99_us"]))
            except (TypeError, ValueError):
                skipped += 1
                continue

            if not engine or not workload:
                skipped += 1
                continue

            key = (workload, engine, key_size, value_size, threads)
            grouped[key]["ops"].append(ops)
            grouped[key]["p99_us"].append(p99)
            engines.add(engine)
            kv_pairs.add((key_size, value_size))

    rows: list[dict[str, Any]] = []
    for (workload, engine, key_size, value_size, threads), values in grouped.items():
        if not values["ops"] or not values["p99_us"]:
            continue
        rows.append(
            {
                "workload": workload,
                "engine": engine,
                "key_size": key_size,
                "value_size": value_size,
                "threads": threads,
                "ops": float(statistics.median(values["ops"])),
                "p99_us": float(statistics.median(values["p99_us"])),
            }
        )

    if not rows:
        raise ValueError("No valid rows parsed from CSV")

    return rows, engines, kv_pairs


def build_report_payload(rows: list[dict[str, Any]], engines: set[str], kv_pairs: set[tuple[int, int]]) -> dict[str, Any]:
    engine_order = sorted(engines)
    kv_order = sorted(kv_pairs)

    engine_to_style = {
        engine: engine_style(engine, idx)
        for idx, engine in enumerate(engine_order)
    }
    kv_to_color = {
        kv: color_for_index(idx)
        for idx, kv in enumerate(kv_order)
    }

    by_workload: dict[str, dict[tuple[str, int, int], list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        series_key = (row["engine"], row["key_size"], row["value_size"])
        by_workload[row["workload"]][series_key].append(row)

    workload_items = []
    for workload in sorted(by_workload.keys(), key=workload_sort_key):
        metric_datasets: dict[str, list[dict[str, Any]]] = {"ops": [], "p99_us": []}
        series_map = by_workload[workload]

        for engine, key_size, value_size in sorted(series_map.keys(), key=lambda x: (x[1], x[2], x[0])):
            points = sorted(series_map[(engine, key_size, value_size)], key=lambda x: x["threads"])
            color = kv_to_color[(key_size, value_size)]
            style = engine_to_style[engine]

            for metric in ("ops", "p99_us"):
                data_points = [
                    {
                        "x": p["threads"],
                        "y": p[metric],
                        "label": format_metric(metric, p[metric]),
                    }
                    for p in points
                ]
                metric_datasets[metric].append(
                    {
                        "label": f"{engine} (k={key_size}, v={value_size})",
                        "data": data_points,
                        "borderColor": color,
                        "backgroundColor": color,
                        "borderWidth": 2,
                        "engineStyle": style,
                    }
                )

        workload_items.append(
            {
                "id": workload,
                "label": workload_label(workload),
                "charts": metric_datasets,
            }
        )

    legend_pairs = [
        {"key_size": k, "value_size": v, "color": kv_to_color[(k, v)]}
        for k, v in kv_order
    ]
    legend_engines = [
        {"engine": engine, "style": engine_to_style[engine]}
        for engine in engine_order
    ]

    return {
        "workloads": workload_items,
        "kvLegend": legend_pairs,
        "engineLegend": legend_engines,
    }


def render_html(
    payload: dict[str, Any],
    source_csv: str,
    source_csv_name: str,
    source_csv_b64: str,
    mace_label: str,
    mace_value: str,
    rocksdb_version: str,
) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Benchmark Report</title>
  <style>
    :root {{
      --bg: #f7f8fc;
      --card: #ffffff;
      --text: #1f2937;
      --muted: #6b7280;
      --border: #e5e7eb;
      --accent: #1d4ed8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", sans-serif;
      color: var(--text);
      background: radial-gradient(circle at top right, #e8eeff 0%, var(--bg) 45%);
    }}
    .container {{
      max-width: 1480px;
      margin: 0 auto;
      padding: 24px 20px 40px;
    }}
    h1 {{ margin: 0 0 10px; font-size: 30px; }}
    .sub {{ color: var(--muted); margin-bottom: 18px; }}
    .source-link {{
      color: var(--accent);
      text-decoration: none;
      border-bottom: 1px dashed var(--accent);
    }}
    .source-link:hover {{ opacity: 0.85; }}
    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 2px 10px rgba(17, 24, 39, 0.04);
    }}
    .legend-wrap {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      margin-bottom: 18px;
    }}
    .legend-title {{ font-weight: 700; margin-bottom: 10px; }}
    .legend-list {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 16px;
      font-size: 14px;
    }}
    .legend-item {{ display: inline-flex; align-items: center; gap: 8px; color: #111827; }}
    .swatch {{ width: 22px; height: 0; border-top: 4px solid #000; border-radius: 2px; }}
    .engine-swatch {{
      width: 28px;
      height: 14px;
      border: 2px solid #111827;
      border-radius: 3px;
      background: #111827;
      display: inline-block;
    }}
    .engine-swatch.hatch {{
      background-image: repeating-linear-gradient(
        45deg,
        #111827 0px,
        #111827 4px,
        #ffffff 4px,
        #ffffff 7px
      );
    }}
    .engine-swatch.dot {{
      background-image: radial-gradient(#111827 28%, transparent 30%);
      background-size: 6px 6px;
      background-color: #ffffff;
    }}
    .workload {{ margin-top: 22px; }}
    .workload-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin: 0 0 10px;
      flex-wrap: wrap;
    }}
    .workload h2 {{ margin: 0; font-size: 22px; color: var(--accent); }}
    .metric-toggle {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 10px;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: #ffffff;
    }}
    .metric-label {{
      font-size: 13px;
      color: #374151;
      font-weight: 700;
      text-transform: lowercase;
    }}
    .metric-current {{
      font-size: 13px;
      color: #1d4ed8;
      font-weight: 700;
      min-width: 32px;
      text-align: center;
      border-left: 1px solid var(--border);
      padding-left: 8px;
    }}
    .metric-slider {{
      width: 64px;
      accent-color: #1d4ed8;
      cursor: pointer;
    }}
    .chart-card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 12px;
      box-shadow: 0 2px 10px rgba(17, 24, 39, 0.04);
      min-height: 560px;
    }}
    .chart-tools {{
      display: flex;
      justify-content: flex-end;
      margin: 0 0 8px;
    }}
    .reset-btn {{
      border: 1px solid var(--border);
      background: #f9fafb;
      color: #111827;
      font-size: 12px;
      padding: 4px 8px;
      border-radius: 8px;
      cursor: pointer;
    }}
    .reset-btn:hover {{ background: #eef2ff; }}
    canvas {{ width: 100% !important; height: 500px !important; }}
    @media (max-width: 1000px) {{
      .legend-wrap {{ grid-template-columns: 1fr; }}
      .workload-head {{ align-items: flex-start; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <h1>Benchmark Report</h1>
    <div class="sub">
      CSV source:
      <a id="source-download" class="source-link" href="#" download="{source_csv_name}">
        <code>{source_csv}</code>
      </a>
      (click to download raw CSV)
    </div>
    <div class="sub">
      Test tool source code:
      <a class="source-link" href="{TEST_TOOL_SOURCE_URL}" target="_blank" rel="noopener noreferrer">{TEST_TOOL_SOURCE_URL}</a>
      <br />
      {mace_label}: <code>{mace_value}</code>
      <br />
      rocksdb version: <code>{rocksdb_version}</code>
    </div>

    <div class="legend-wrap">
      <div class="card">
        <div class="legend-title">Color: key/value pairs (consistent across engines)</div>
        <div id="kv-legend" class="legend-list"></div>
      </div>
      <div class="card">
        <div class="legend-title">Fill style: engine</div>
        <div id="engine-legend" class="legend-list"></div>
      </div>
    </div>

    <div id="workloads-root"></div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.0/dist/chart.umd.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2.2.0/dist/chartjs-plugin-zoom.min.js"></script>
  <script>
    const REPORT = {payload_json};
    const SOURCE_CSV_B64 = "{source_csv_b64}";
    const SOURCE_CSV_NAME = "{source_csv_name}";

    const valueLabelPlugin = {{
      id: 'valueLabelPlugin',
      afterDatasetsDraw(chart) {{
        const ctx = chart.ctx;
        ctx.save();
        ctx.font = '11px sans-serif';
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';

        chart.data.datasets.forEach((dataset, datasetIndex) => {{
          const meta = chart.getDatasetMeta(datasetIndex);
          if (meta.hidden) return;

          meta.data.forEach((point, i) => {{
            const raw = dataset.data[i];
            const labelText =
              (dataset.valueLabels && dataset.valueLabels[i] !== undefined)
                ? dataset.valueLabels[i]
                : (raw && raw.label !== undefined ? raw.label : null);
            if (labelText === undefined || labelText === null || labelText === '') return;
            const pos = point.tooltipPosition();
            ctx.fillStyle = dataset.borderColor;
            ctx.fillText(String(labelText), pos.x + 5, pos.y - 6);
          }});
        }});

        ctx.restore();
      }}
    }};

    Chart.register(valueLabelPlugin);

    function patternForStyle(ctx, style, color) {{
      if (style === 'solid') {{
        return color;
      }}
      const p = document.createElement('canvas');
      p.width = 10;
      p.height = 10;
      const pctx = p.getContext('2d');
      if (!pctx) {{
        return color;
      }}
      pctx.clearRect(0, 0, p.width, p.height);
      if (style === 'hatch') {{
        pctx.strokeStyle = color;
        pctx.lineWidth = 2;
        pctx.beginPath();
        pctx.moveTo(-2, 8);
        pctx.lineTo(8, -2);
        pctx.moveTo(2, 12);
        pctx.lineTo(12, 2);
        pctx.stroke();
      }} else if (style === 'dot') {{
        pctx.fillStyle = color;
        pctx.beginPath();
        pctx.arc(3, 3, 1.5, 0, Math.PI * 2);
        pctx.fill();
        pctx.beginPath();
        pctx.arc(8, 8, 1.5, 0, Math.PI * 2);
        pctx.fill();
      }} else {{
        return color;
      }}
      return ctx.createPattern(p, 'repeat') || color;
    }}

    function addLegends() {{
      const kvRoot = document.getElementById('kv-legend');
      const engineRoot = document.getElementById('engine-legend');

      REPORT.kvLegend.forEach(item => {{
        const node = document.createElement('div');
        node.className = 'legend-item';
        node.innerHTML = `<span class="swatch" style="border-top-color:${{item.color}}"></span><span>k=${{item.key_size}}, v=${{item.value_size}}</span>`;
        kvRoot.appendChild(node);
      }});

      REPORT.engineLegend.forEach(item => {{
        const node = document.createElement('div');
        node.className = 'legend-item';
        node.innerHTML = `<span class="engine-swatch ${{item.style}}"></span><span>${{item.engine}}</span>`;
        engineRoot.appendChild(node);
      }});
    }}

    function normalizeBarDatasets(rawDatasets) {{
      const labelSet = new Set();
      rawDatasets.forEach(ds => {{
        ds.data.forEach(pt => {{
          labelSet.add(String(pt.x));
        }});
      }});

      const labels = Array.from(labelSet).sort((a, b) => Number(a) - Number(b));
      const datasets = rawDatasets.map(ds => {{
        const pointMap = new Map(ds.data.map(pt => [String(pt.x), pt]));
        const data = labels.map(lbl => {{
          const p = pointMap.get(lbl);
          return p ? p.y : null;
        }});
        const valueLabels = labels.map(lbl => {{
          const p = pointMap.get(lbl);
          return p ? p.label : '';
        }});
        return {{
          ...ds,
          data,
          valueLabels
        }};
      }});
      return {{ labels, datasets }};
    }}

    function createChart(canvas, title, yTitle, datasets) {{
      const normalized = normalizeBarDatasets(datasets);
      const chartCtx = canvas.getContext('2d');
      if (!chartCtx) {{
        return null;
      }}
      normalized.datasets = normalized.datasets.map(ds => {{
        const fill = patternForStyle(chartCtx, ds.engineStyle, ds.backgroundColor);
        return {{
          ...ds,
          backgroundColor: fill,
          borderColor: ds.borderColor,
        }};
      }});
      const chart = new Chart(chartCtx, {{
        type: 'bar',
        data: normalized,
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          interaction: {{ mode: 'nearest', intersect: false }},
          plugins: {{
            title: {{
              display: true,
              text: title,
              color: '#111827',
              font: {{
                size: 20,
                weight: '700'
              }}
            }},
            legend: {{ display: false }},
            zoom: {{
              pan: {{
                enabled: true,
                mode: 'xy',
                modifierKey: 'ctrl'
              }},
              zoom: {{
                wheel: {{
                  enabled: true
                }},
                drag: {{
                  enabled: true
                }},
                pinch: {{
                  enabled: true
                }},
                mode: 'xy'
              }}
            }},
            tooltip: {{
              callbacks: {{
                label: (ctx) => `${{ctx.dataset.label}}: ${{ctx.dataset.valueLabels?.[ctx.dataIndex] ?? ctx.parsed.y}}`
              }}
            }}
          }},
          scales: {{
            x: {{
              type: 'category',
              title: {{
                display: true,
                text: 'threads',
                color: '#111827',
                font: {{
                  size: 16,
                  weight: '700'
                }}
              }},
              ticks: {{
                color: '#111827',
                font: {{
                  size: 14,
                  weight: '600'
                }}
              }}
            }},
            y: {{
              title: {{
                display: true,
                text: yTitle,
                color: '#111827',
                font: {{
                  size: 16,
                  weight: '700'
                }}
              }},
              ticks: {{
                color: '#111827',
                font: {{
                  size: 14,
                  weight: '600'
                }}
              }}
            }}
          }}
        }}
      }});
      return chart;
    }}

    function addWorkloads() {{
      const root = document.getElementById('workloads-root');

      REPORT.workloads.forEach((w, wIdx) => {{
        const section = document.createElement('section');
        section.className = 'workload';

        section.innerHTML = `
          <div class="workload-head">
            <h2>${{w.label}}</h2>
            <div class="metric-toggle">
              <span class="metric-label">ops</span>
              <input id="metric-switch-${{wIdx}}" class="metric-slider" type="range" min="0" max="1" step="1" value="0" />
              <span class="metric-label">p99</span>
              <span id="metric-current-${{wIdx}}" class="metric-current">ops</span>
            </div>
          </div>
          <div class="chart-card">
            <div class="chart-tools"><button type="button" class="reset-btn" id="chart-reset-${{wIdx}}">Reset zoom</button></div>
            <canvas id="metric-${{wIdx}}"></canvas>
          </div>
        `;
        root.appendChild(section);

        const chartCanvas = section.querySelector(`#metric-${{wIdx}}`);
        const chartResetBtn = section.querySelector(`#chart-reset-${{wIdx}}`);
        const metricSwitch = section.querySelector(`#metric-switch-${{wIdx}}`);
        const metricCurrent = section.querySelector(`#metric-current-${{wIdx}}`);

        let currentChart = null;

        function renderMetric(metric) {{
          const yTitle = metric === 'ops' ? 'ops' : 'p99_us';
          metricCurrent.textContent = metric === 'ops' ? 'ops' : 'p99';
          if (currentChart) {{
            currentChart.destroy();
          }}
          currentChart = createChart(chartCanvas, `${{w.label}} - ${{yTitle}}`, yTitle, w.charts[metric]);
        }}

        metricSwitch.addEventListener('input', () => {{
          const nextMetric = metricSwitch.value === '1' ? 'p99_us' : 'ops';
          renderMetric(nextMetric);
        }});
        chartResetBtn.addEventListener('click', () => {{
          if (currentChart) currentChart.resetZoom();
        }});
        chartCanvas.addEventListener('dblclick', () => {{
          if (currentChart) currentChart.resetZoom();
        }});

        renderMetric('ops');
      }});
    }}

    function initSourceDownload() {{
      const link = document.getElementById('source-download');
      link.href = `data:text/csv;base64,${{SOURCE_CSV_B64}}`;
      link.download = SOURCE_CSV_NAME;
    }}

    initSourceDownload();
    addLegends();
    addWorkloads();
  </script>
</body>
</html>
"""


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    output_path = (
        Path(args.output)
        if args.output
        else csv_path.with_suffix(".html")
    )

    rows, engines, kv_pairs = read_and_aggregate(csv_path)
    payload = build_report_payload(rows, engines, kv_pairs)
    csv_bytes = csv_path.read_bytes()
    csv_b64 = base64.b64encode(csv_bytes).decode("ascii")
    repo_root = Path(__file__).resolve().parent.parent
    mace_label, mace_value = infer_mace_identity(repo_root)
    rocksdb_version = infer_rocksdb_version(repo_root)
    html = render_html(
        payload,
        str(csv_path),
        csv_path.name,
        csv_b64,
        mace_label,
        mace_value,
        rocksdb_version,
    )

    output_path.write_text(html, encoding="utf-8")
    print(f"HTML written to: {output_path}")
    print(f"Workloads: {len(payload['workloads'])}, engines: {len(engines)}, kv pairs: {len(kv_pairs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
