import re
import json
import pandas as pd
from pathlib import Path

# Regular expression for email validation
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

def validate_email(email):
    """Returns True if email is valid, False otherwise."""
    if not email or not isinstance(email, str):
        return False
    return bool(EMAIL_REGEX.match(email.strip()))

def validate_customers(csv_path, quarantine_dir):
    """
    Validates customer records from a CSV file.
    Validates:
      - Missing primary key (Customer_ID)
      - Duplicate primary key
      - Null mandatory values (Customer_ID, Name, Email, Age)
      - Wrong datatype for Age or Customer_ID
      - Invalid email format
      - Age below 18 or negative
      
    Saves invalid records with reason of rejection to quarantine directory.
    Returns (clean_df, quarantined_df, stats_dict).
    """
    csv_path = Path(csv_path)
    quarantine_dir = Path(quarantine_dir)
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    
    # Read CSV
    df = pd.read_csv(csv_path)
    df = df.fillna("")  # replace NaN with empty string for parsing
    
    clean_records = []
    quarantined_records = []
    
    seen_ids = set()
    
    stats = {
        "total": len(df),
        "accepted": 0,
        "rejected": 0,
        "violations": {
            "missing_pk": 0,
            "duplicate_pk": 0,
            "null_mandatory": 0,
            "invalid_email": 0,
            "underage": 0,
            "invalid_datatype": 0
        }
    }
    
    for index, row in df.iterrows():
        reasons = []
        cust_id_raw = str(row["Customer_ID"]).strip()
        name = str(row["Name"]).strip()
        email = str(row["Email"]).strip()
        phone = str(row["Phone"]).strip()
        age_raw = str(row["Age"]).strip()
        city = str(row["City"]).strip()
        
        # 1. Primary Key Check (Missing)
        if not cust_id_raw:
            reasons.append("Missing Customer_ID (Primary Key)")
            stats["violations"]["missing_pk"] += 1
        else:
            # Datatype check for ID
            try:
                cust_id = int(float(cust_id_raw))
            except ValueError:
                reasons.append(f"Invalid Datatype for Customer_ID: '{cust_id_raw}' (must be int)")
                stats["violations"]["invalid_datatype"] += 1
                cust_id = cust_id_raw
                
            # Duplicate ID check
            if cust_id in seen_ids:
                reasons.append(f"Duplicate Customer_ID: {cust_id}")
                stats["violations"]["duplicate_pk"] += 1
            else:
                if not reasons:  # only track unique if no datatype error
                    seen_ids.add(cust_id)
                    
        # 2. Mandatory Values Check
        if not name:
            reasons.append("Missing mandatory value: Name")
            stats["violations"]["null_mandatory"] += 1
        if not email:
            reasons.append("Missing mandatory value: Email")
            stats["violations"]["null_mandatory"] += 1
            
        # 3. Email Format Check
        if email and not validate_email(email):
            reasons.append(f"Invalid Email Format: '{email}'")
            stats["violations"]["invalid_email"] += 1
            
        # 4. Age Check (Datatype & Logic)
        if not age_raw:
            reasons.append("Missing mandatory value: Age")
            stats["violations"]["null_mandatory"] += 1
        else:
            try:
                age = int(float(age_raw))
                if age < 0:
                    reasons.append(f"Negative Age: {age}")
                    stats["violations"]["underage"] += 1
                elif age < 18:
                    reasons.append(f"Underage customer: {age} (below 18)")
                    stats["violations"]["underage"] += 1
            except ValueError:
                reasons.append(f"Invalid Datatype for Age: '{age_raw}' (must be int)")
                stats["violations"]["invalid_datatype"] += 1
                
        # Partition record
        record_dict = {
            "Customer_ID": cust_id_raw,
            "Name": row["Name"],
            "Email": row["Email"],
            "Phone": row["Phone"],
            "Age": row["Age"],
            "City": row["City"]
        }
        
        if reasons:
            record_dict["Rejection_Reason"] = " | ".join(reasons)
            quarantined_records.append(record_dict)
        else:
            clean_records.append(record_dict)
            
    # Create DataFrames
    clean_df = pd.DataFrame(clean_records)
    quarantined_df = pd.DataFrame(quarantined_records)
    
    stats["accepted"] = len(clean_df)
    stats["rejected"] = len(quarantined_df)
    
    # Save Quarantine CSV
    quarantine_path = quarantine_dir / "customers_quarantined.csv"
    quarantined_df.to_csv(quarantine_path, index=False, encoding='utf-8')
    
    return clean_df, quarantined_df, stats


def validate_transactions(json_path, quarantine_dir):
    """
    Validates API transaction logs from a JSON file.
    Validates:
      - Missing primary key (transaction_id)
      - Duplicate primary key
      - Null customer details
      - Invalid/negative transaction amounts
      - Wrong datatype for amount
      - Missing status
      
    Saves invalid records with reason of rejection to quarantine directory.
    Returns (clean_list, quarantined_list, stats_dict).
    """
    json_path = Path(json_path)
    quarantine_dir = Path(quarantine_dir)
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    clean_records = []
    quarantined_records = []
    
    seen_tx_ids = set()
    
    stats = {
        "total": len(data),
        "accepted": 0,
        "rejected": 0,
        "violations": {
            "missing_pk": 0,
            "duplicate_pk": 0,
            "null_customer": 0,
            "invalid_amount": 0,
            "invalid_datatype": 0,
            "missing_status": 0
        }
    }
    
    for tx in data:
        reasons = []
        
        tx_id = tx.get("transaction_id")
        
        # 1. Primary Key Checks
        if tx_id is None or tx_id == "":
            reasons.append("Missing transaction_id (Primary Key)")
            stats["violations"]["missing_pk"] += 1
        else:
            try:
                tx_id = int(tx_id)
                if tx_id in seen_tx_ids:
                    reasons.append(f"Duplicate transaction_id: {tx_id}")
                    stats["violations"]["duplicate_pk"] += 1
                else:
                    seen_tx_ids.add(tx_id)
            except ValueError:
                reasons.append(f"Invalid Datatype for transaction_id: '{tx_id}' (must be int)")
                stats["violations"]["invalid_datatype"] += 1
                
        # 2. Customer validation
        customer = tx.get("customer")
        if not customer:
            reasons.append("Missing nested customer details")
            stats["violations"]["null_customer"] += 1
        else:
            cust_id = customer.get("id")
            cust_name = customer.get("name")
            if cust_id is None or cust_id == "":
                reasons.append("Missing customer.id")
                stats["violations"]["null_customer"] += 1
            if not cust_name:
                reasons.append("Missing customer.name")
                stats["violations"]["null_customer"] += 1
                
        # 3. Payment details validation
        payment = tx.get("payment")
        if not payment:
            reasons.append("Missing nested payment details")
            stats["violations"]["invalid_amount"] += 1
        else:
            amount = payment.get("amount")
            mode = payment.get("mode")
            
            if amount is None or amount == "":
                reasons.append("Missing payment.amount")
                stats["violations"]["invalid_amount"] += 1
            else:
                try:
                    amount_val = float(amount)
                    if amount_val <= 0:
                        reasons.append(f"Invalid Transaction Amount: {amount_val} (must be > 0)")
                        stats["violations"]["invalid_amount"] += 1
                except ValueError:
                    reasons.append(f"Invalid Datatype for payment.amount: '{amount}' (must be numeric)")
                    stats["violations"]["invalid_datatype"] += 1
                    
            if not mode:
                reasons.append("Missing payment.mode")
                
        # 4. Status validation
        status = tx.get("status")
        if not status:
            reasons.append("Missing transaction status")
            stats["violations"]["missing_status"] += 1
            
        # Partition
        tx_copy = dict(tx)
        if reasons:
            tx_copy["rejection_reason"] = " | ".join(reasons)
            quarantined_records.append(tx_copy)
        else:
            clean_records.append(tx_copy)
            
    stats["accepted"] = len(clean_records)
    stats["rejected"] = len(quarantined_records)
    
    # Save Quarantine JSON
    quarantine_path = quarantine_dir / "transactions_quarantined.json"
    with open(quarantine_path, 'w', encoding='utf-8') as qf:
        json.dump(quarantined_records, qf, indent=4)
        
    return clean_records, quarantined_records, stats


def generate_validation_report(customer_stats, tx_stats, report_path):
    """
    Generates a Markdown validation report summarizing the data quality metrics.
    
    Parameters:
        customer_stats (dict): Stats dictionary from customer validation.
        tx_stats (dict): Stats dictionary from transaction validation.
        report_path (str or Path): Output file path for validation report.
    """
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    md_content = f"""# Data Quality Validation Report
Generated on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

This report summarizes the data quality execution logs, profiling results, and quarantined anomalies for the customers dataset and simulated nested transaction logs.

## 1. Customers Dataset Quality Metrics

### Summary Table
| Metric | Count | Percentage |
| :--- | :---: | :---: |
| **Total Processed Records** | {customer_stats['total']} | 100.0% |
| **Accepted Records (Clean)** | {customer_stats['accepted']} | {(customer_stats['accepted']/customer_stats['total']*100):.2f}% |
| **Rejected Records (Quarantined)** | {customer_stats['rejected']} | {(customer_stats['rejected']/customer_stats['total']*100):.2f}% |

### Rule Violation Breakdown
* **Missing Primary Key (`Customer_ID`)**: {customer_stats['violations']['missing_pk']}
* **Duplicate Primary Key (`Customer_ID`)**: {customer_stats['violations']['duplicate_pk']}
* **Missing Mandatory Fields (Name/Email)**: {customer_stats['violations']['null_mandatory']}
* **Invalid Email Addresses (Regex match failed)**: {customer_stats['violations']['invalid_email']}
* **Age Out of Bounds (Underage < 18 or Negative)**: {customer_stats['violations']['underage']}
* **Invalid Datatypes (Age/ID Coercion fails)**: {customer_stats['violations']['invalid_datatype']}

---

## 2. API Transaction Logs Quality Metrics

### Summary Table
| Metric | Count | Percentage |
| :--- | :---: | :---: |
| **Total Processed Transactions** | {tx_stats['total']} | 100.0% |
| **Accepted Transactions (Clean)** | {tx_stats['accepted']} | {(tx_stats['accepted']/tx_stats['total']*100):.2f}% |
| **Rejected Transactions (Quarantined)** | {tx_stats['rejected']} | {(tx_stats['rejected']/tx_stats['total']*100):.2f}% |

### Rule Violation Breakdown
* **Missing Primary Key (`transaction_id`)**: {tx_stats['violations']['missing_pk']}
* **Duplicate Primary Key (`transaction_id`)**: {tx_stats['violations']['duplicate_pk']}
* **Missing Customer Info (ID/Name)**: {tx_stats['violations']['null_customer']}
* **Invalid Payment Amount (<= 0)**: {tx_stats['violations']['invalid_amount']}
* **Invalid Payment Datatypes**: {tx_stats['violations']['invalid_datatype']}
* **Missing Status**: {tx_stats['violations']['missing_status']}

---

## 3. Ingestion Decisions
* **Customers File Destination**: Cleansed database table `CustomerProfiles` in SQLite target. Rejected records moved to `quarantine/customers_quarantined.csv`.
* **API Transactions Destination**: Cleansed database table `TransactionLogs` in SQLite target. Rejected records moved to `quarantine/transactions_quarantined.json`.
"""
    try:
        with open(report_path, 'w', encoding='utf-8') as rf:
            rf.write(md_content)
        print(f"Validation report generated at: {report_path.resolve()}")
    except Exception as e:
        print(f"Error generating validation report: {e}")
        raise e

if __name__ == "__main__":
    # Local test
    base_dir = Path(__file__).parent
    q_dir = base_dir / "quarantine"
    c_csv = base_dir / "data" / "customers.csv"
    t_json = base_dir / "data" / "transactions.json"
    rep = base_dir / "reports" / "validation_report.md"
    
    if c_csv.exists() and t_json.exists():
        _, _, c_st = validate_customers(c_csv, q_dir)
        _, _, t_st = validate_transactions(t_json, q_dir)
        generate_validation_report(c_st, t_st, rep)
