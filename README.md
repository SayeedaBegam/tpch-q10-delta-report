# 🧮 TPC-H Q10 Delta Lake Report

This repository contains a performance and visualization report for **TPC-H Query 10** executed on the **Delta Lake** table format using **DuckDB**.

The project compares **non-partitioned** vs **partitioned (file-skipping)** Delta tables to measure the effect of **partition pruning** on query performance.

---

## 📊 Report Summary

| Metric | Baseline | Partitioned |
|--------|-----------|--------------|
| **Rows returned** | 20 | 20 |
| **Runtime** | ~1.5 s | ~1.0 s |
| **Speedup** | **1.5× faster** |
| **Time reduced** | ~34% |

### 🔍 Partition Pruning Snapshot
| Table | Files matching filter | Total files | % Skipped |
|--------|-----------------------|--------------|------------|
| `orders-delta-part` | 6 | 160 | 96% |
| `lineitem-delta-part` | 12 | 46 | 74% |

---

## 📈 Contents

| File | Description |
|------|--------------|
| [`report.html`](./report.html) | Full interactive report with charts and tables |
| [`q10_timings.png`](./q10_timings.png) | Runtime comparison bar chart |
| [`q10_results.xlsx`](./q10_results.xlsx) | Excel workbook with results and summary |
| [`baseline_nonpartitioned_top20.csv`](./baseline_nonpartitioned_top20.csv) | Baseline query output |
| [`partitioned_fileskipping_top20.csv`](./partitioned_fileskipping_top20.csv) | Partitioned query output |

---

## 🧠 Key Insights

- Partitioning on **year/month (orders)** and **returnflag (lineitem)** enables DuckDB to skip >90% of files.
- Query runtime improved by ~1.5× on 10× scale factor data.
- Visualization shows **runtime bar chart** and **donut charts** of files read vs skipped.

---

## 🧰 Tools Used
- **DuckDB** (in-memory analytical engine)
- **Delta Lake** (ACID table format)
- **Python** for orchestration, reporting, and chart generation
- **Matplotlib / Pandas** for visualizations
- **GitHub Pages** for hosting the report

---

## 🌐 View the Live Report
➡️ **[Click here to view the report](https://SayeedaBegam.github.io/tpch-q10-delta-report/report.html)**

---

## ✍️ Author
**Sayeeda Begam Mohamed Ikbal**  
📅 _UTN Semester 3 – Cloud Database Assignment_  
