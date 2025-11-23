# DuckDB-Based Reconciliation Engine

## Overview
A lightweight, zero-cost data reconciliation engine built using Python and DuckDB.  
The solution is designed to compare large-scale datasets across cloud and on-premise sources with minimal resource usage, enabling high-speed reconciliation without the need for distributed compute.

---

## Key Features
- 🔍 Supports multi-file, multi-source reconciliation (CSV, Excel, Parquet, Delta)
- ⚙️ Configurable through JSON mapping for primary keys and attribute comparisons
- 🚀 Fast, in-memory processing using DuckDB
- 📊 Generates comprehensive reconciliation reports in Excel (multi-sheet) format
- 💻 Supports both local and cloud (Databricks) execution modes
- ✅ Handles schema drift, data type mismatches, and late-arriving records

---

## Project Structure
```text
├── src/
│   ├── duckdb_utils.py
│   ├── reconciliation.py
│   ├── databricks_utils.py
│   └── config/
│       └── mapping.json
├── sample_data/
├── output/
├── README.md
└── requirements.txt
```

## Usage
1. Clone the repository:
```
git clone https://github.com/souravagasti/reconciliation-engine.git
```
2. Install dependencies:
```
pip install -r requirements.txt
```
3. Run the reconciliation:
```
python reconciliation.py --config src/config/mapping.json --platform local
```
For Databricks execution:
```python reconciliation.py --config src/config/mapping.json --platform databricks```
## Future Enhancements
✅ Add unit tests and CI/CD integration

✅ Build a user-friendly CLI

✅ Add support for streaming reconciliation via Structured Streaming

✅ Improve error handling and logging framework

✅ Extend support for additional file types (JSON, Avro)

✅ Add supportfor authentication via temporary credential vending for Unity Catalog-backed data

## Contact
For queries, improvements, or collaborations, feel free to reach out:
📧 sourav.agasti@gmail.com
