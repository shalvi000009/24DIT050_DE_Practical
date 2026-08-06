# Data Quality Validation Report
Generated on: 2026-08-05 21:24:24

This report summarizes the data quality execution logs, profiling results, and quarantined anomalies for the customers dataset and simulated nested transaction logs.

## 1. Customers Dataset Quality Metrics

### Summary Table
| Metric | Count | Percentage |
| :--- | :---: | :---: |
| **Total Processed Records** | 200 | 100.0% |
| **Accepted Records (Clean)** | 126 | 63.00% |
| **Rejected Records (Quarantined)** | 74 | 37.00% |

### Rule Violation Breakdown
* **Missing Primary Key (`Customer_ID`)**: 7
* **Duplicate Primary Key (`Customer_ID`)**: 15
* **Missing Mandatory Fields (Name/Email)**: 23
* **Invalid Email Addresses (Regex match failed)**: 12
* **Age Out of Bounds (Underage < 18 or Negative)**: 17
* **Invalid Datatypes (Age/ID Coercion fails)**: 0

---

## 2. API Transaction Logs Quality Metrics

### Summary Table
| Metric | Count | Percentage |
| :--- | :---: | :---: |
| **Total Processed Transactions** | 150 | 100.0% |
| **Accepted Transactions (Clean)** | 108 | 72.00% |
| **Rejected Transactions (Quarantined)** | 42 | 28.00% |

### Rule Violation Breakdown
* **Missing Primary Key (`transaction_id`)**: 8
* **Duplicate Primary Key (`transaction_id`)**: 4
* **Missing Customer Info (ID/Name)**: 6
* **Invalid Payment Amount (<= 0)**: 14
* **Invalid Payment Datatypes**: 6
* **Missing Status**: 4

---

## 3. Ingestion Decisions
* **Customers File Destination**: Cleansed database table `CustomerProfiles` in SQLite target. Rejected records moved to `quarantine/customers_quarantined.csv`.
* **API Transactions Destination**: Cleansed database table `TransactionLogs` in SQLite target. Rejected records moved to `quarantine/transactions_quarantined.json`.
