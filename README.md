# kdb-parquet

**Write kdb+ tables to [Parquet](https://parquet.apache.org/) files using Arrow C++**  
Supports flat and Hive-style partitioned Parquet datasets (single or multi-column), with full support for memory-mapped splayed tables, enumerated symbols, and anymaps.

---

## Features

- Export kdb+ tables to Parquet (flat or partitioned)
- Hive-style partitioning by one or more columns
- Works with in-memory and splayed (on-disk) kdb+ tables
- Supports enumerated symbols and anymaps
- Efficient conversion using Arrow C++
- Query the resulting Parquet data with DuckDB, Spark, or PyArrow

---

## Installation

### 1. Clone the repo

```bash
git clone https://github.com/ak8976/kdb-parquet.git
cd kdb-parquet
```
### 2. Set up environment
Install required dependencies using conda:
```bash
conda create -n arrow-cpp -c conda-forge arrow-cpp gxx_linux-64 cmake make python=3.10
conda activate arrow-cpp
```
### 3. Build the shared library
```bash
mkdir -p build && cd build
cmake ..
make
```
This creates the libparquet_writer.so shared library
## Usage from q
```q
q)to_parquet:`libparquet_writer 2:(`write_parquet; 3)
```
### Basic examples
```q
q)to_parquet:`libparquet_writer 2:(`write_parquet; 3)
// Flat file output
q)to_parquet[([]a:1 2 3;b:`a`b`c);`test.parquet;()]

// Single-level partition (e.g. by symbol)
q)to_parquet[([]a:1 2 3;b:`a`b`c);`test;`b]
```
### From partitioned/splayed tables
```q
q)to_parquet:`libparquet_writer 2:(`write_parquet; 3)
q)cd:system"cd"
// Setup a splayed table
q)trade:1000000#("SPFJSNB*"; 1#",") 0: `:trades.csv
q)update date:2025.04.02 from trade
q).Q.dd[`:db;2025.04.02,`trade`] set .Q.en[`:db;trade]
q)\l db
q)to_parquet[select from trade where date=2025.04.02;`$cd,"/trade.parquet";()]
q)to_parquet[select from trade where date=2025.04.02;`$cd,"/trade_date";`date]
q)to_parquet[select from trade where date=2025.04.02;`$cd,"/trade_date_sym";`date`symbol]
```
## Example Queries in DuckDB
DuckDB natively supports Parquet and Hive-style partitioning:
```sql
-- Flat file
SELECT * FROM 'trade.parquet';

-- Single partition
SELECT * FROM 'trade_date/*/*.parquet';

-- Multi-column partition
SELECT * FROM 'trade_date_sym/*/*/*.parquet';
```
## How It Works
 - C++ foreign function interface (write_parquet) exposed via a shared library
 - Uses Apache Arrow to build Arrow Tables from kdb+ input
 - Writes flat or partitioned Parquet using Arrow's parquet::arrow::WriteTable or arrow::dataset::FileSystemDataset::Write

## Supported Types
 - boolean
 - int, long, short, float, real
 - date, timestamp, time, timespan
 - symbol, including enumerated symbols (type 20–76)
 - mixed, anymap with only string values

## Related
[Blog Post — Partitioned Parquet from kdb+](https://medium.com/@alvi.kabir919/kdb-to-parquet-partitioned-tables-fd85603c322f)

[Apache Arrow C++](https://arrow.apache.org/)

[DuckDB](https://duckdb.org/)
