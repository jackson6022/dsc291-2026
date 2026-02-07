# Verification: Assignment Requirements vs pivot_all_files.py

Checklist mapping **HOMEWORK_ASSIGNMENT_1.md** (Pipeline Summary + Part 4) to the code.

---

## Pipeline Summary (assignment § Pipeline Summary)

| # | Requirement | Code location | Status |
|---|-------------|----------------|--------|
| 1 | **Discover** Parquet files (local or `s3://`) | `main()`: `discover_parquet_files(args.input_dir, anon=s3_anon)` (lines 571–596) | ✅ |
| 2 | **Check** schemas consistent; define **common schema** (`pickup_datetime`, `pickup_location`); process into schema before aggregation | `check_schemas_consistent(files)` (605–609); `normalize_to_common_schema()` in `process_single_file()` (224) | ✅ |
| 3 | **Group by month**; **process one month at a time**; infer month from path; normalize → aggregate by (date, taxi_type, pickup_place, hour) → pivot → **discard rows with < 50 rides** → write intermediate Parquet; **count and report** month-mismatch (per-file and/or per-month + total) | `group_files_by_month(files)` (616); loop over `month_keys` (636–662); `process_single_file()` does infer month, normalize, pivot, cleanup_low_count_rows, write intermediate, `month_mismatch_count` (176–271); per-month log (661–662); total log (664–667) | ✅ |
| 4 | **Combine** into **single wide table** (all data), **indexed by taxi_type, date, pickup_place**; aggregate by (taxi_type, date, pickup_place), sum hour columns; **store as Parquet**; **upload to S3** | `combine_into_wide_table(intermediate_paths)` (669); `wide.to_parquet(final_path)` (671); `upload_to_s3(...)` if `args.s3_output` (675–679) | ✅ |
| 5 | **Generate report**: input row count, output row count, bad rows ignored (parse failures, invalid data, timestamp mismatch, low-count cleanup), memory use (peak RSS), run time; output to small report file | `generate_report(...)` (698–711) with all fields; writes JSON to `report_path`; optional `.tex` via `--report-tex` | ✅ |

---

## Part 4 — Main Pipeline (assignment § Part 4)

| Requirement | Code location | Status |
|-------------|----------------|--------|
| **process_single_file**: Read Parquet (local/S3) → infer expected month → normalize → aggregate by (date, taxi_type, pickup_place, hour) → pivot → discard < 50 rides → write intermediate Parquet; **count** rows where month ≠ file’s expected month; return metadata | `process_single_file()` (176–271): read (210), infer month (207), normalize (224), pivot (252–256), cleanup (269), write (265–268); month_mismatch_count (243–248); returns dict with all stats | ✅ |
| **Month-at-a-time**: Group by (year, month); process one month at a time; parallelize within month if desired | `group_files_by_month()` (274); `for month_key in month_iter` (636); `process_month_batch(..., workers=args.workers)` (638–645) | ✅ |
| **Month-mismatch reporting**: Aggregate and report total; optionally per-file and/or per-month | Total: `total_month_mismatch` (625, 664–667). Per-month: `logger.info("Month %s: %d row(s) with month mismatch", ...)` (661–662) | ✅ |
| **combine_into_wide_table**: Read intermediates → aggregate by (taxi_type, date, pickup_place), sum hour columns → single wide table → store as Parquet | `combine_into_wide_table()` (330–371); `wide.to_parquet(final_path)` (671) | ✅ |
| **Step 5 report**: Input row count, output row count, bad rows ignored, memory use, run time; emit to file (e.g. JSON/text) | `generate_report()` (386–404); report_path (JSON), optional tex_path | ✅ |
| **CLI main()**: `--input-dir`, `--output-dir`, `--min-rides` (default 50), `--workers`, `--partition-size` / `--skip-partition-optimization`, `--keep-intermediate`; discovery → group by month → optional partition optimization → process one month at a time → report month-mismatch → combine → store Parquet → upload to S3 → generate report; multiprocessing, tqdm, **continue on per-file errors** | All args present (470–541). Flow in `main()` (570–711). Per-file errors: `process_single_file` returns error dict; `process_month_batch` catches executor exceptions and appends error result (314–325); pipeline does not exit on single-file failure | ✅ |

---

## Notes

- **Report output**: Assignment Pipeline Summary says “Output the report to a small tex file”; Part 4 says “Emit to console, log file, or a small report file (e.g. JSON/text)”. The code writes JSON by default (`--report-output`) and supports a `.tex` snippet via `--report-tex report.tex`. To satisfy “small tex file” literally, run with `--report-tex report.tex` or add a default tex path in code.
- **Partition optimization**: Optional; invoked when `--max-memory-usage` is set and `--skip-partition-optimization` is not (612–614). Requires `partition_optimization.py` module.
- **Wide table index**: Final table is indexed by `(taxi_type, date, pickup_place)` with columns `hour_0`…`hour_23` as required.

All required pipeline steps from the assignment are implemented in `pivot_all_files.py`.
