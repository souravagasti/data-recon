DuckDB-Based Reconciliation Engine
Overview
A lightweight, zero-cost data reconciliation engine built using Python and DuckDB. The solution is designed to compare large-scale datasets across cloud and on-premise sources with minimal resource usage, enabling high-speed reconciliation without the need for distributed compute.

Key Features
🔍 Supports multi-file, multi-source reconciliation (CSV, Excel, Parquet, Delta)

⚙️ Configurable through JSON mapping for primary keys and attribute comparisons

🚀 Fast, in-memory processing using DuckDB

📊 Generates comprehensive reconciliation reports in Excel (multi-sheet) format

💾 Supports both local and cloud (Databricks) execution modes

✅ Handles schema drift, data type mismatches, and late-arriving records

Project Structure
text
Copy
Edit
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
Usage
Clone the repository:

bash
Copy
Edit
git clone https://github.com/souravagasti/reconciliation-engine.git
Install dependencies:

bash
Copy
Edit
pip install -r requirements.txt
Run the reconciliation:

bash
Copy
Edit
python reconciliation.py --config config/mapping.json --platform local
Future Enhancements
Add unit tests and CI/CD integration

Build a user-friendly CLI

Add support for streaming reconciliation via Structured Streaming

Contact
For queries, improvements, or collaborations, please reach out to: sourav.agasti@gmail.com
