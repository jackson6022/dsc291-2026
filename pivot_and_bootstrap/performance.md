# Pipeline Performance Summary

**Generated:** 2026-02-08 19:27:28
**Output Location:** `s3://291-s3-bucket/wide.parquet`

## Runtime & Memory

### Total Runtime
- **Wall Clock Time:** 87.85 seconds (1.46 minutes)
- **Time Breakdown:**
  - File Discovery: 0.63 seconds
  - Schema Check & Partition Optimization: 0.41 seconds
  - Month-at-a-Time Processing (Parquet read, pivot, cleanup, intermediate write, month-mismatch counting): 86.64 seconds
  - Production of Single Wide Table (combine intermediates): 0.03 seconds
  - Parquet Processing/Output (write wide_table.parquet): 0.02 seconds
  - S3 Upload: 0.12 seconds
  - **Total (Combine & Output):** 0.17 seconds

### Peak Memory Usage
- **Peak RSS:** 192.16 MB (0.188 GB)

## S3 Metrics

### S3 Input
- **Input Path:** `s3://dsc291-ucsd/taxi/Dataset/2023/yellow_taxi`
- **Files Read from S3:** 9
- **Input Size:** 442.35 MB
- **Input Throughput (read):** 5.11 MB/s

### S3 Output
- **Output URI:** `s3://291-s3-bucket/wide.parquet`
- **Upload Size:** 562.37 KB (0.549 MB)
- **Upload Time:** 0.12 seconds
- **Upload Throughput:** 4.61 MB/s
- **Upload Status:** Failed or skipped

## Input vs Output

### Total Input Rows
- **Total Rows Read:** 28,071,659
- **Input File Size:** 442.35 MB

### Discarded Rows Summary
- **Total Discarded:** 41,701 rows (0.15%)
- **Percentage Skipped:** 0.15%
- **Rows Retained:** 28,029,958 (99.85%)

### Output Row Counts

#### Intermediate Pivoted Table
- **Intermediate Rows:** 19,033
- **Format:** (taxi_type, date, pickup_place, hour) aggregation

#### Final Aggregated / Wide Table
- **Final Output Rows:** 19,033
- **Indexed By:** (taxi_type, date, pickup_place)
- **File Size:** 562.37 KB

## Discard Breakdown

| Reason | Count | % of Input |
|--------|-------|------------|
| Cleanup Removed Low Count | 41,701 | 0.15% |
| Month Mismatch Informational | 603 | 0.00% |
| Parse Failures | 0 | 0.00% |
| **Total Discarded** | **41,701** | **0.15%** |

## Date Consistency Issues

### Inconsistent Rows
- **Total Month-Mismatch Rows:** 603
- **Definition:** Rows where pickup_datetime month ≠ file's expected month

### Files Affected
- **Number of Files with Mismatches:** 9

## Row Breakdown by Year and Taxi Type

### Final Wide Table Distribution

| Year | Taxi Type | Row Count | % of Total |
|------|-----------|-----------|------------|
| 2023 | yellow | 19,033 | 100.0% |
| **Total** | | **19,033** | **100.0%** |

## Schema Summary

### Output Table Structure

**Total Columns:** 27

**Index Columns (3):**
1. `taxi_type` - Type of taxi
2. `date` - Date of trip (YYYY-MM-DD format)
3. `pickup_place` - Pickup location / zone

**Value Columns (24):**
Hour-based ride counts:
- `hour_0` - 00:00
- `hour_1` - 01:00
- `hour_2` - 02:00
- `hour_3` - 03:00
- `hour_4` - 04:00
- `hour_5` - 05:00
- `hour_6` - 06:00
- `hour_7` - 07:00
- `hour_8` - 08:00
- `hour_9` - 09:00
- `hour_10` - 10:00
- `hour_11` - 11:00
- `hour_12` - 12:00
- `hour_13` - 13:00
- `hour_14` - 14:00
- `hour_15` - 15:00
- `hour_16` - 16:00
- `hour_17` - 17:00
- `hour_18` - 18:00
- `hour_19` - 19:00
- `hour_20` - 20:00
- `hour_21` - 21:00
- `hour_22` - 22:00
- `hour_23` - 23:00

## Processing Performance

### Throughput Metrics
- **Input Throughput:** 5.11 MB/s
- **Output Throughput:** 3.20 MB/s
- **Compression Ratio:** 805.5×

### Resource Efficiency
- **CPU Utilization:** 4 workers out of 10 cores
- **Memory Usage:** 192.16 MB (allocated)

## Summary Statistics

| Metric | Value |
|--------|-------|
| Total Files Attempted | 9 |
| Files Succeeded | 9 |
| Files Failed | 0 |
| Total Input Rows | 28,071,659 |
| Rows Discarded | 41,701 (0.15%) |
| Intermediate Rows | 19,033 |
| Final Output Rows | 19,033 |
| Output Columns | 27 |
| Peak Memory (MB) | 192.16 |
| Total Runtime (sec) | 87.85 |
| Total Runtime (min) | 1.46 |
| Time: Discovery | 0.63 s |
| Time: Schema & Partition Opt | 0.41 s |
| Time: Month-at-a-Time Processing | 86.64 s |
| Time: Combine Wide Table | 0.03 s |
| Time: Parquet Write | 0.02 s |
| Time: S3 Upload | 0.12 s |
| Parse Failures | 0 |
| Month Mismatches | 603 |
| Files with Mismatches | 9 |
| Workers Used | 4 |
| Input File Size | 442.35 MB |
| Output File Size | 562.37 KB |

## Conclusion

Pipeline completed successfully. Processed 28,071,659 input rows into 19,033 output rows with 0.15% discard rate. Total runtime: 1.46 minutes with peak memory: 192.16 MB.
