# Pipeline Performance Summary

**Generated:** 2026-02-09 23:08:44

**Input Location:** `s3://dsc291-ucsd/taxi`

**Output Location:** `s3://291-s3-bucket/wide.parquet`

## Runtime & Memory

### Total Runtime
- **Wall Clock Time:** 1016.42 seconds (16.94 minutes)
- **Time Breakdown:**
  - File Discovery: 0.25 seconds
  - Schema Check & Partition Optimization: 0.16 seconds
  - Month-at-a-Time Processing (Parquet read, pivot, cleanup, intermediate write, month-mismatch counting): 1010.92 seconds
  - Production of Single Wide Table (combine intermediates): 2.46 seconds
  - Parquet Processing/Output (write wide_table.parquet): 1.52 seconds
  - S3 Upload: 1.10 seconds
  - **Total (Combine & Output):** 5.08 seconds

### Peak Memory Usage
- **Peak RSS:** 28.0 GB/28000 MB(21.9 %MEM)

## Input vs Output

### Total Input Rows
- **Total Rows Read:** 3,410,052,578
- **Input File Size:** 57061.96 MB

### Discarded Rows Summary
- **Total Discarded:** 8,119,886 rows (0.24%)
- **Percentage Skipped:** 0.24%
- **Rows Retained:** 3,401,932,692 (99.76%)

### Output Row Counts

#### Intermediate Pivoted Table
- **Intermediate Rows:** 2,914,893
- **Format:** (taxi_type, date, pickup_place, hour) aggregation

#### Final Aggregated / Wide Table
- **Final Output Rows:** 2,914,892
- **Indexed By:** (taxi_type, date, pickup_place)
- **File Size:** 88383.2 KB

## Discard Breakdown

| Reason | Count | % of Input |
|--------|-------|------------|
| Cleanup Removed Low Count | 8,119,886 | 0.24% |
| Month Mismatch Informational | 17,064 | 0.0005% |
| Parse Failures | 0 | 0.00% |
| **Total Discarded** | **8,119,886** | **0.2405%** |

## Date Consistency Issues

### Inconsistent Rows
- **Total Month-Mismatch Rows:** 17,064
- **Definition:** Rows where pickup_datetime month ≠ file's expected month

### Files Affected
- **Number of Files with Mismatches:** 141

## Row Breakdown by Year and Taxi Type

### Final Wide Table Distribution

| Year | Taxi Type | Row Count | % of Total |
|------|-----------|-----------|------------|
| 2009 | yellow | 742,277 | 25.5% |
| 2010 | yellow | 732,975 | 25.1% |
| 2011 | yellow | 40,758 | 1.4% |
| 2012 | yellow | 39,223 | 1.3% |
| 2013 | yellow | 37,713 | 1.3% |
| 2014 | green | 31,505 | 1.1% |
| 2014 | yellow | 38,316 | 1.3% |
| 2015 | fhv | 66,408 | 2.3% |
| 2015 | green | 33,695 | 1.2% |
| 2015 | yellow | 37,190 | 1.3% |
| 2016 | fhv | 84,776 | 2.9% |
| 2016 | green | 29,323 | 1.0% |
| 2016 | yellow | 36,394 | 1.2% |
| 2017 | fhv | 89,754 | 3.1% |
| 2017 | green | 23,441 | 0.8% |
| 2017 | yellow | 34,589 | 1.2% |
| 2018 | fhv | 92,124 | 3.2% |
| 2018 | green | 26,576 | 0.9% |
| 2018 | yellow | 33,980 | 1.2% |
| 2019 | fhv | 29,749 | 1.0% |
| 2019 | fhvhv | 83,559 | 2.9% |
| 2019 | green | 23,448 | 0.8% |
| 2020 | fhv | 16,977 | 0.6% |
| 2020 | fhvhv | 89,863 | 3.1% |
| 2020 | green | 6,434 | 0.2% |
| 2020 | yellow | 23,631 | 0.8% |
| 2021 | fhv | 15,101 | 0.5% |
| 2021 | fhvhv | 90,499 | 3.1% |
| 2021 | green | 3,901 | 0.1% |
| 2021 | yellow | 25,853 | 0.9% |
| 2022 | fhv | 23,950 | 0.8% |
| 2022 | fhvhv | 90,745 | 3.1% |
| 2022 | green | 4,089 | 0.1% |
| 2022 | yellow | 26,299 | 0.9% |
| 2023 | fhv | 19,550 | 0.7% |
| 2023 | fhvhv | 68,139 | 2.3% |
| 2023 | green | 3,055 | 0.1% |
| 2023 | yellow | 19,033 | 0.7% |
| **Total** | | **2,914,892** | **100.0%** |

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
- **Input Throughput:** 56.45 MB/s
- **Output Throughput:** 17.00 MB/s
- **Compression Ratio:** 661.1×

### Resource Efficiency
- **CPU Used:** 3 workers out of 16 cores (So CPU utilization should be around 18.75%)
- **CPU Utilization from our run:** 14.24%

## Summary Statistics

| Metric | Value |
|--------|-------|
| Total Files Attempted | 443 |
| Files Succeeded | 443 |
| Files Failed | 0 |
| Total Input Rows | 3,410,052,578 |
| Rows Discarded | 8,119,886 (0.24%) |
| Intermediate Rows | 2,914,893 |
| Final Output Rows | 2,914,892 |
| Output Columns | 27 |
| Peak Memory (GB) | 28.0 |
| Total Runtime (sec) | 1016.42 |
| Total Runtime (min) | 16.94 |
| Time: Discovery | 0.25 s |
| Time: Schema & Partition Opt | 0.16 s |
| Time: Month-at-a-Time Processing | 1010.92 s |
| Time: Combine Wide Table | 2.46 s |
| Time: Parquet Write | 1.52 s |
| Time: S3 Upload | 1.10 s |
| Parse Failures | 0 |
| Month Mismatches | 17,064 |
| Files with Mismatches | 141 |
| Workers Used | 3 |
| Input File Size | 57061.96 MB |
| Output File Size | 88383.2 KB |




## Conclusion

Pipeline completed successfully. Processed 3,410,052,578 input rows into 2,914,892 output rows with 0.2405% discard rate. Total runtime: 16.94 minutes with peak memory: 28.0 GB.
