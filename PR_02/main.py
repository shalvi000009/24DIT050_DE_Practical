import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from tabulate import tabulate

# Add project root to sys.path for safety
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from logger import setup_logging, log_execution_summary
from generator import generate_customers
from api_simulator import generate_transactions
from profiler import profile_dataset
from validator import validate_customers, validate_transactions, generate_validation_report
from database import initialize_database, insert_clean_customers, insert_clean_transactions

def build_directories(base_path):
    """
    Creates the required directory structure for B.Tech Practical 2.
    """
    dirs = ["data", "quarantine", "logs", "database", "reports"]
    for d in dirs:
        dir_path = base_path / d
        dir_path.mkdir(parents=True, exist_ok=True)
    print("Project directories initialized successfully.")

def run_pipeline():
    """
    Orchestrates the entire Data Engineering pipeline:
    1. Dir Setup
    2. Data Generation
    3. Profiling
    4. Quality Validation
    5. Database Ingestion
    6. Reporting & Logging
    """
    # Start time
    pipeline_start = datetime.now()
    
    # Paths definition
    project_root = Path(__file__).parent
    build_directories(project_root)
    
    # Setup standard logger
    setup_logging()
    
    logging.info("Starting Data Engineering Practical-2 Pipeline execution.")
    
    # Files definition
    customers_csv = project_root / "data" / "customers.csv"
    transactions_json = project_root / "data" / "transactions.json"
    db_file = project_root / "database" / "customers.db"
    report_file = project_root / "reports" / "validation_report.md"
    quarantine_dir = project_root / "quarantine"
    
    errors_count = 0
    warnings_count = 0
    
    try:
        # Step 1: Generate Datasets
        print("\n" + "="*50)
        print(" STEP 1: WORKLOAD & DATA SYNTHESIS")
        print("="*50)
        
        logging.info("Generating customer CSV dataset (200 records).")
        generate_customers(customers_csv, num_records=200)
        
        logging.info("Generating nested API JSON transactions (150 records).")
        generate_transactions(transactions_json, num_records=150)
        
        # Step 2: Source Data Profiling
        print("\n" + "="*50)
        print(" STEP 2: SOURCE DATA PROFILING")
        print("="*50)
        
        customer_profile = profile_dataset(customers_csv)
        tx_profile = profile_dataset(transactions_json)
        
        # Step 3: Database schema creation
        print("\n" + "="*50)
        print(" STEP 3: TARGET DATABASE INITIALIZATION")
        print("="*50)
        initialize_database(db_file)
        
        # Step 4: Automated Data Quality Validation
        print("\n" + "="*50)
        print(" STEP 4: DATA QUALITY VALIDATION & QUARANTINE")
        print("="*50)
        
        logging.info("Running quality validation on Customer Profiles CSV.")
        clean_cust_df, rejected_cust_df, cust_stats = validate_customers(customers_csv, quarantine_dir)
        
        logging.info("Running quality validation on Nested API Transactions JSON.")
        clean_tx_list, rejected_tx_list, tx_stats = validate_transactions(transactions_json, quarantine_dir)
        
        # Write validation report
        generate_validation_report(cust_stats, tx_stats, report_file)
        
        # Step 5: Database Loading (Clean Ingestion)
        print("\n" + "="*50)
        print(" STEP 5: TARGET DATABASE INGESTION")
        print("="*50)
        
        cust_loaded = insert_clean_customers(db_file, clean_cust_df)
        tx_loaded = insert_clean_transactions(db_file, clean_tx_list)
        
        # Calculate Warning and Error counts for summary
        # Violations count acts as warnings in ingestion criteria
        cust_warnings = sum(cust_stats["violations"].values())
        tx_warnings = sum(tx_stats["violations"].values())
        warnings_count = cust_warnings + tx_warnings
        
        # Log individual dataset executions
        pipeline_end = datetime.now()
        log_execution_summary(pipeline_start, pipeline_end, "Customer Profiles (CSV -> SQLite)", cust_stats, errors_count, cust_warnings)
        log_execution_summary(pipeline_start, pipeline_end, "Transaction Logs (JSON -> SQLite)", tx_stats, errors_count, tx_warnings)
        
        # Step 6: Print Final Execution Dashboard
        print("\n" + "+" + "-"*78 + "+")
        print("|" + " "*24 + "PIPELINE EXECUTION DASHBOARD" + " "*26 + "|")
        print("+" + "-"*78 + "+")
        
        dashboard_data = [
            [
                "Customers Dataset", 
                cust_stats["total"], 
                cust_stats["accepted"], 
                cust_stats["rejected"],
                f"{(cust_stats['accepted']/cust_stats['total']*100):.2f}%",
                "Table: CustomerProfiles"
            ],
            [
                "API Transactions", 
                tx_stats["total"], 
                tx_stats["accepted"], 
                tx_stats["rejected"],
                f"{(tx_stats['accepted']/tx_stats['total']*100):.2f}%",
                "Table: TransactionLogs"
            ]
        ]
        
        headers = ["Source Dataset", "Total", "Accepted", "Rejected", "Clean Rate", "Target Destination"]
        print(tabulate(dashboard_data, headers=headers, tablefmt="grid"))
        
        print("\n  [SUCCESS] Database Location     :", db_file.resolve())
        print("  [SUCCESS] Validation Report     :", report_file.resolve())
        print("  [SUCCESS] Log Output File       :", (project_root / "logs" / "execution.log").resolve())
        print("  [SUCCESS] Quarantine Directory  :", quarantine_dir.resolve())
        print("+" + "-"*78 + "+\n")
        
        logging.info("Data Engineering Pipeline run completed successfully.")
        
    except Exception as e:
        errors_count += 1
        logging.error(f"Critical error during pipeline execution: {e}", exc_info=True)
        print(f"\n[ERROR] PIPELINE FAILED: Check logs/execution.log for stacktrace.")

if __name__ == "__main__":
    run_pipeline()
