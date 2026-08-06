# CHARUSAT University
## Department of Information Technology (Data Engineering)
### Practical 2: Source Data Profiling and Automated Data Quality Validation

---

## 1. Objective
Programmatically interface with diverse file-based, API-driven, and relational source database environments. Develop generation engines that simulate transactional workloads, automatically discover schemas, detect data quality violations (such as missing/duplicate primary keys, null values, invalid email formats, and incorrect datatypes), and establish ingestion criteria before loading only clean data into target database systems.

---

## 2. Technology Stack & Tools
- **Language**: Python 3.x
- **Libraries**:
  - `pandas` (For reading, profiling, and handling structured tabular data)
  - `faker` (For generating synthetic customer records and mock data fields)
  - `requests` (Simulating API payloads/HTTP structures)
  - `tabulate` (For formatting data and metadata profiling summaries as beautiful CLI tables)
- **Database**: SQLite3 (Standard relational engine used for target loading)
- **Data Formats**: CSV (delimited text files), JSON (nested API responses), TXT (unstructured configuration files)
- **Monitoring & Auditing**: Standard Python `logging` library (logs system warnings, errors, and execution metrics)

---

## 3. Directory Layout
The project directory is structured as follows:

```
Project/
│
├── data/
│   ├── customers.csv             # Raw CSV dataset with anomalies
│   ├── transactions.json         # Simulated nested JSON API responses
│   └── config.txt                # Unstructured database & parsing parameters
│
├── quarantine/
│   ├── customers_quarantined.csv # Rejected CSV profiles with error reasons
│   └── transactions_quarantined.json # Rejected JSON transactions with error reasons
│
├── logs/
│   ├── execution.log             # Main application logs (pipeline history)
│   └── summary_history.csv       # Audit trail for pipeline performance
│
├── database/
│   └── customers.db              # Target SQLite database (holds clean data)
│
├── reports/
│   └── validation_report.md      # Auto-generated markdown DQ summary report
│
├── main.py                       # Pipeline Orchestrator (entry point)
├── generator.py                  # Faker-based CSV customer generator
├── api_simulator.py              # Nested JSON API workload simulator
├── profiler.py                   # Automated schema profiling engine
├── validator.py                  # DQ validation rules engine
├── database.py                   # SQLite tables initialization & loader
├── logger.py                     # Logging configuration and metadata audit
└── requirements.txt              # Project dependencies list
```

---

## 4. Execution Guide
Follow these steps to run the complete pipeline within your development environment:

### Step 1: Install Dependencies
Run the command below in your terminal/command prompt to install all necessary libraries:
```bash
pip install -r requirements.txt
```

### Step 2: Run the Orchestrator
Execute the main file to run the data pipeline:
```bash
python main.py
```

---

## 5. Expected Output & Pipeline Workflow
When executed, the project performs the following actions:
1. **Directory Setup**: Dynamically creates `data/`, `quarantine/`, `logs/`, `database/`, and `reports/` if they do not exist.
2. **Workload Synthesis**: Generates 200 customer profiles (with 15% messy test data) and 150 nested transactions.
3. **Data Profiling**: Prints tabular statistics to stdout summarizing rows, columns, unique values, missing percentages, and numeric distributions.
4. **Data Validation**: Segregates records failing business rules (invalid emails, underage profiles, negative transaction amounts, missing primary keys) into the `quarantine/` folder.
5. **Database Load**: Loads clean records into SQLite.
6. **Execution Audit**: Writes logs detailing execution times and success rates, and outputs a formatted terminal execution dashboard.

---

## 6. Learning Outcomes
By completing this practical, students will be able to:
- Design modular ETL workflows and ingestion gates in Python.
- Programmatically inject and handle data anomalies (dirty data) in file-based sources.
- Execute automated schema profiling on flat files and nested JSON datasets.
- Implement rule-based data quality checking and quarantining pipelines.
- Load cleansed data into relational SQLite targets using parameterized queries to maintain database integrity.
