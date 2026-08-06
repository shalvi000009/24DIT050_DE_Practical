import os
import logging
from datetime import datetime
from pathlib import Path

# Ensure logs directory exists
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Main log file path
LOG_FILE = LOGS_DIR / "execution.log"

def setup_logging():
    """
    Configures the standard Python logging system to write to both
    the console and a file in the logs directory.
    """
    try:
        # Create formatter
        log_format = "%(asctime)s - %(levelname)s - %(message)s"
        
        # Configure root logger
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        logging.info("Logging system initialized successfully.")
    except Exception as e:
        print(f"Error setting up logging: {e}")

def log_execution_summary(start_time, end_time, dataset_name, stats, errors=0, warnings=0):
    """
    Logs a structured execution summary for a specific data pipeline stage.
    
    Parameters:
        start_time (datetime): Time when execution began.
        end_time (datetime): Time when execution ended.
        dataset_name (str): Name of the dataset processed (e.g. 'Customers', 'Transactions').
        stats (dict): Dictionary containing 'total', 'accepted', 'rejected' records.
        errors (int): Number of error messages logged.
        warnings (int): Number of warning messages logged.
    """
    duration = (end_time - start_time).total_seconds()
    
    summary_msg = (
        f"\n=================== EXECUTION SUMMARY: {dataset_name} ===================\n"
        f"Start Time       : {start_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}\n"
        f"End Time         : {end_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}\n"
        f"Duration (sec)   : {duration:.4f}s\n"
        f"Total Records    : {stats.get('total', 0)}\n"
        f"Accepted Records : {stats.get('accepted', 0)}\n"
        f"Rejected Records : {stats.get('rejected', 0)}\n"
        f"Errors Encountered: {errors}\n"
        f"Warnings Logged  : {warnings}\n"
        f"========================================================================="
    )
    logging.info(summary_msg)
    
    # Also write a structured history file for tracking
    summary_history_file = LOGS_DIR / "summary_history.csv"
    file_exists = summary_history_file.exists()
    try:
        with open(summary_history_file, mode="a", encoding="utf-8") as f:
            if not file_exists:
                f.write("timestamp,dataset,duration_seconds,total,accepted,rejected,errors,warnings\n")
            f.write(
                f"{datetime.now().isoformat()},{dataset_name},{duration:.4f},"
                f"{stats.get('total', 0)},{stats.get('accepted', 0)},{stats.get('rejected', 0)},"
                f"{errors},{warnings}\n"
            )
    except Exception as e:
        logging.error(f"Failed to write to summary history CSV: {e}")
