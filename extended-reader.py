# extended-reader.py
import os
import time
import io
import base64
from pathlib import Path

import duckdb
import pandas as pd
import matplotlib.pyplot as plt

REPEATS = 5  # run each query multiple times; take median for fair timings

# ----------------------- Helpers: file counting for pruning snapshot -----------------------
def count_files(path: Path) -> int:
    return sum(1 for _ in path.rglob("*.parquet"))

def count_orders_q10_files(base: Path) -> int:
    # partitions like o_orderdate_year=1993/o_orderdate_month=10|11|12
    total = 0
    year_dir = base / "o_orderdate_year=1993"
    for m in (10, 11, 12):
        month_dir = year_dir / f"o_orderdate_month={m}"
        if month_dir.exists():
            total += count_files(month_dir)
    return total

def count_lineitem_q10_files(base: Path) -> int:
    # partition like l_returnflag=R
    r_dir = base / "l_returnflag=R"
    return count_files(r_dir) if r_dir.exists() else 0

# ----------------------- Output dir -----------------------
OUTDIR = Path("./q10_out")
OUTDIR.mkdir(exist_ok=True)

# ----------------------- DuckDB connection -----------------------
con = duckdb.connect()

# Use all cores (or set to 1 for single-thread apples-to-apples)
con.execute(f"PRAGMA threads = {os.cpu_count()};")
# con.execute("PRAGMA threads = 1;")  # <- uncomment to fix to single thread

# Optional: write a profiling JSON to inspect operators later
con.execute("PRAGMA enable_profiling = json;")
con.execute("PRAGMA profile_output = 'q10_out/duckdb_profile.json';")

# ----------------------- Queries -----------------------
Q10_BASELINE = """
WITH
  o AS (
    SELECT * FROM delta_scan('./orders-delta')
    WHERE o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01'
  ),
  l AS (
    SELECT * FROM delta_scan('./lineitem-delta') WHERE l_returnflag = 'R'
  )
SELECT
  c.c_custkey,
  c.c_name,
  SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
  c.c_acctbal,
  n.n_name,
  c.c_address,
  c.c_phone,
  c.c_comment
FROM o
JOIN delta_scan('./customer-delta') c ON c.c_custkey = o.o_custkey
JOIN l                             ON l.l_orderkey = o.o_orderkey
JOIN delta_scan('./nation-delta')   n ON n.n_nationkey = c.c_nationkey
GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment
ORDER BY revenue DESC
LIMIT 20;
"""

Q10_PARTITIONED = """
WITH
  o AS (
    SELECT * FROM delta_scan('./orders-delta-part')
    WHERE o_orderdate_year = 1993 AND o_orderdate_month IN (10,11,12)
  ),
  l AS (
    SELECT * FROM delta_scan('./lineitem-delta-part') WHERE l_returnflag = 'R'
  )
SELECT
  c.c_custkey,
  c.c_name,
  SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
  c.c_acctbal,
  n.n_name,
  c.c_address,
  c.c_phone,
  c.c_comment
FROM o
JOIN delta_scan('./customer-delta') c ON c.c_custkey = o.o_custkey
JOIN l                             ON l.l_orderkey = o.o_orderkey
JOIN delta_scan('./nation-delta')   n ON n.n_nationkey = c.c_nationkey
GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment
ORDER BY revenue DESC
LIMIT 20;
"""

def run_query_repeated(label: str, sql: str):
    times = []
    last_df = None
    for _ in range(REPEATS):
        t0 = time.time()
        df = con.execute(sql).df()
        t1 = time.time()
        times.append(t1 - t0)
        last_df = df
    secs = sorted(times)[REPEATS // 2]  # median
    csv_path = OUTDIR / f"{label.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('-', '')}_top20.csv"
    last_df.to_csv(csv_path, index=False)
    return {"label": label, "rows": len(last_df), "seconds": secs, "csv": csv_path, "df": last_df, "runs": times}

print("Running TPC-H Q10 visualized …")

baseline = run_query_repeated("Baseline (non-partitioned)", Q10_BASELINE)
part     = run_query_repeated("Partitioned (file-skipping)", Q10_PARTITIONED)

# ----------------------- File-skipping snapshot (partition pruning only) -----------------------
orders_part_dir   = Path("./orders-delta-part")
lineitem_part_dir = Path("./lineitem-delta-part")

orders_total  = count_files(orders_part_dir)
orders_q10    = count_orders_q10_files(orders_part_dir)
orders_skipped_pct = 0.0 if orders_total == 0 else (1 - orders_q10 / orders_total) * 100.0

lineitem_total = count_files(lineitem_part_dir)
lineitem_q10   = count_lineitem_q10_files(lineitem_part_dir)
lineitem_skipped_pct = 0.0 if lineitem_total == 0 else (1 - lineitem_q10 / lineitem_total) * 100.0

print("\n--- Partition pruning snapshot ---")
print(f"orders-delta-part:   files matching Q10 = {orders_q10} / {orders_total}  (skipped ~{orders_skipped_pct:.1f}%)")
print(f"lineitem-delta-part: files matching Q10 = {lineitem_q10} / {lineitem_total}  (skipped ~{lineitem_skipped_pct:.1f}%)")

# ----------------------- Summary metrics -----------------------
summary = pd.DataFrame(
    [
        {"Run": baseline["label"], "Rows": baseline["rows"], "Seconds": round(baseline["seconds"], 3), "CSV": str(baseline["csv"])},
        {"Run": part["label"],     "Rows": part["rows"],     "Seconds": round(part["seconds"], 3),     "CSV": str(part["csv"])},
    ]
)

speedup = baseline["seconds"] / part["seconds"] if part["seconds"] > 0 else float("inf")
reduction = (1.0 - (part["seconds"] / baseline["seconds"])) * 100.0 if baseline["seconds"] > 0 else 0.0

print("\n=== Q10 Summary ===")
print(summary.to_string(index=False))
print(f"\nSpeedup: {speedup:.2f}×  |  Time reduced: {reduction:.1f}%")
print(f"Raw times (s) baseline : {[round(x,3) for x in baseline['runs']]}")
print(f"Raw times (s) partition: {[round(x,3) for x in part['runs']]}")

# ----------------------- Runtime bar chart -----------------------
plt.figure(figsize=(6,4))
x = [baseline["label"], part["label"]]
y = [baseline["seconds"], part["seconds"]]
plt.bar(x, y)
plt.ylabel("Seconds")
plt.title("TPC-H Q10 Runtime (Lower is Better)")
plt.xticks(rotation=15, ha="right")
buf = io.BytesIO()
plt.tight_layout()
plt.savefig(buf, format="png")
plt.close()
buf.seek(0)
png_bytes = buf.read()
chart_path = OUTDIR / "q10_timings.png"
with open(chart_path, "wb") as f:
    f.write(png_bytes)

# ----------------------- Pie charts (donut) for files read vs skipped -----------------------
def make_pie(read_count, total_count, title):
    skipped = max(total_count - read_count, 0)
    labels = ["Read", "Skipped"]
    sizes = [read_count, skipped]
    colors = ["#66b3ff", "#cccccc"]
    fig, ax = plt.subplots(figsize=(3,3))
    ax.pie(
        sizes,
        labels=labels,
        autopct="%1.1f%%",
        startangle=90,
        colors=colors,
        wedgeprops={"width": 0.4},   # donut style
        textprops={"fontsize": 8}
    )
    ax.set_title(title, fontsize=10)
    plt.tight_layout()
    b = io.BytesIO()
    plt.savefig(b, format="png", bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(b.getvalue()).decode("ascii")

orders_pie_b64   = make_pie(orders_q10,   orders_total,   "orders-delta-part")
lineitem_pie_b64 = make_pie(lineitem_q10, lineitem_total, "lineitem-delta-part")

# ----------------------- Excel workbook -----------------------
xlsx_path = OUTDIR / "q10_results.xlsx"
with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xw:
    summary.to_excel(xw, sheet_name="Summary", index=False)
    baseline["df"].to_excel(xw, sheet_name="Baseline_Top20", index=False)
    part["df"].to_excel(xw, sheet_name="Partitioned_Top20", index=False)

# ----------------------- HTML report -----------------------
img_b64 = base64.b64encode(png_bytes).decode("ascii")

def df_to_html_table(df: pd.DataFrame, max_rows=10):
    return df.head(max_rows).to_html(index=False, justify="left")

html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>TPC-H Q10 Report</title>
  <style>
    body {{ font-family: Segoe UI, Roboto, Arial, sans-serif; margin: 20px; }}
    h1 {{ margin-bottom: 0; }}
    .sub {{ color: #666; margin-top: 4px; }}
    table {{ border-collapse: collapse; margin-top: 10px; }}
    th, td {{ border: 1px solid #ddd; padding: 6px 8px; }}
    th {{ background: #f4f4f4; }}
    .cards {{ display: flex; gap: 24px; flex-wrap: wrap; margin-top: 12px; }}
    .card {{ border: 1px solid #e6e6e6; border-radius: 10px; padding: 12px 14px; flex: 1 1 360px; box-shadow: 0 1px 4px rgba(0,0,0,0.05); }}
    .kpi {{ font-size: 28px; font-weight: 700; }}
    .kpi-sub {{ color: #666; }}
    .small {{ color: #666; font-size: 12px; }}
    img {{ max-width: 640px; height: auto; }}
    .pies {{ display:flex; gap:40px; margin-top: 20px; align-items: center; }}
    .piecol {{ text-align:center; }}
    .legend {{ font-size: 12px; color: #555; margin-top: 6px; }}
    .legend .swatch {{ display:inline-block; width:12px; height:12px; margin-right:6px; vertical-align:middle; }}
    .swatch.read {{ background:#66b3ff; }}
    .swatch.skipped {{ background:#cccccc; }}
  </style>
</head>
<body>
  <h1>TPC-H Q10: Delta File Skipping Report</h1>
  <div class="sub">Baseline vs Partitioned (Oct–Dec 1993 &amp; l_returnflag='R')</div>

  <div class="cards">
    <div class="card">
      <div class="kpi">{baseline["seconds"]:.3f}s</div>
      <div class="kpi-sub">Baseline (non-partitioned)</div>
    </div>
    <div class="card">
      <div class="kpi">{part["seconds"]:.3f}s</div>
      <div class="kpi-sub">Partitioned (file-skipping)</div>
    </div>
    <div class="card">
      <div class="kpi">{speedup:.2f}×</div>
      <div class="kpi-sub">Speedup</div>
      <div class="small">Time reduced: {reduction:.1f}%</div>
    </div>
  </div>

  <h2>Partition Pruning Snapshot</h2>
  <table>
    <tr><th>Table</th><th>Files matching filter</th><th>Total files</th><th>~Skipped</th></tr>
    <tr><td>orders-delta-part</td><td>{orders_q10}</td><td>{orders_total}</td><td>{orders_skipped_pct:.1f}%</td></tr>
    <tr><td>lineitem-delta-part</td><td>{lineitem_q10}</td><td>{lineitem_total}</td><td>{lineitem_skipped_pct:.1f}%</td></tr>
  </table>

  <div class="pies">
    <div class="piecol">
      <img src="data:image/png;base64,{orders_pie_b64}" alt="Orders pie" />
      <div class="small">orders-delta-part</div>
    </div>
    <div class="piecol">
      <img src="data:image/png;base64,{lineitem_pie_b64}" alt="Lineitem pie" />
      <div class="small">lineitem-delta-part</div>
    </div>
    <div class="legend">
      <div><span class="swatch read"></span>Read</div>
      <div><span class="swatch skipped"></span>Skipped</div>
    </div>
  </div>

  <h2>Runtime Chart</h2>
  <img src="data:image/png;base64,{img_b64}" alt="Runtime chart"/>

  <h2>Summary</h2>
  {summary.to_html(index=False)}

  <h2>Result Preview (top 10)</h2>
  <h3>Baseline</h3>
  {df_to_html_table(baseline["df"], 10)}
  <h3>Partitioned</h3>
  {df_to_html_table(part["df"], 10)}

  <h2>Downloads</h2>
  <ul>
    <li><a href="q10_timings.png">q10_out/q10_timings.png</a></li>
    <li><a href="q10_results.xlsx">q10_out/q10_results.xlsx</a></li>
    <li><a href="{baseline['csv'].as_posix()}">{baseline['csv'].as_posix()}</a></li>
    <li><a href="{part['csv'].as_posix()}">{part['csv'].as_posix()}</a></li>
  </ul>
</body>
</html>
"""

report_path = OUTDIR / "report.html"
with open(report_path, "w", encoding="utf-8") as f:
    f.write(html)

print(f"\nSaved chart   : {OUTDIR / 'q10_timings.png'}")
print(f"Saved Excel   : {OUTDIR / 'q10_results.xlsx'}")
print(f"Saved report  : {report_path}")

# ----------------------- Optional console previews -----------------------
def preview(df: pd.DataFrame, title: str, n=5):
    cols = ["c_custkey", "c_name", "revenue", "c_acctbal", "n_name"]
    cols = [c for c in cols if c in df.columns]
    small = df[cols].head(n).copy()
    if "c_name" in small.columns:
        small["c_name"] = small["c_name"].str.slice(0, 22)
    print(f"\n--- {title} (top {n}) ---")
    print(small.to_string(index=False))

preview(baseline["df"], "Baseline")
preview(part["df"], "Partitioned")

# ----------------------- Auto-open the report in your browser (absolute path) -----------------------
import webbrowser
webbrowser.open(report_path.resolve().as_uri())
