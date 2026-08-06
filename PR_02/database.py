import sqlite3
from pathlib import Path
import logging

def initialize_database(db_path):
    """
    Creates the SQLite database and initializes the tables:
    - CustomerProfiles (for clean customer records)
    - TransactionLogs (for clean nested transaction records)
    
    Parameters:
        db_path (str or Path): Path to the SQLite database file.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Create CustomerProfiles table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS CustomerProfiles (
            Customer_ID INTEGER PRIMARY KEY,
            Name TEXT NOT NULL,
            Email TEXT NOT NULL,
            Phone TEXT,
            Age INTEGER NOT NULL,
            City TEXT
        );
        """)
        
        # 2. Create TransactionLogs table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS TransactionLogs (
            transaction_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            customer_name TEXT,
            payment_amount REAL NOT NULL,
            payment_mode TEXT NOT NULL,
            status TEXT NOT NULL
        );
        """)
        
        conn.commit()
        logging.info(f"SQLite database initialized successfully at {db_path.resolve()}")
    except sqlite3.Error as e:
        logging.error(f"SQLite initialization error: {e}")
        raise e
    finally:
        if conn:
            conn.close()

def insert_clean_customers(db_path, clean_df):
    """
    Inserts clean customer records into the CustomerProfiles table.
    
    Parameters:
        db_path (str or Path): Path to the SQLite database file.
        clean_df (pd.DataFrame): DataFrame containing verified customer records.
    """
    db_path = Path(db_path)
    conn = None
    records_inserted = 0
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Clear old records to make execution idempotent
        cursor.execute("DELETE FROM CustomerProfiles;")
        
        for _, row in clean_df.iterrows():
            cursor.execute("""
                INSERT OR IGNORE INTO CustomerProfiles (Customer_ID, Name, Email, Phone, Age, City)
                VALUES (?, ?, ?, ?, ?, ?);
            """, (
                int(float(row["Customer_ID"])),
                row["Name"],
                row["Email"],
                row["Phone"],
                int(float(row["Age"])),
                row["City"]
            ))
            if cursor.rowcount > 0:
                records_inserted += 1
                
        conn.commit()
        logging.info(f"Successfully loaded {records_inserted} records into CustomerProfiles table.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting customer records: {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()
            
    return records_inserted

def insert_clean_transactions(db_path, clean_txs):
    """
    Inserts clean transaction records into the TransactionLogs table.
    
    Parameters:
        db_path (str or Path): Path to the SQLite database file.
        clean_txs (list): List of clean transaction dictionaries.
    """
    db_path = Path(db_path)
    conn = None
    records_inserted = 0
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Clear old records to make execution idempotent
        cursor.execute("DELETE FROM TransactionLogs;")
        
        for tx in clean_txs:
            cust = tx.get("customer", {})
            cust_id = cust.get("id")
            cust_name = cust.get("name")
            
            payment = tx.get("payment", {})
            amount = payment.get("amount")
            mode = payment.get("mode")
            
            cursor.execute("""
                INSERT OR IGNORE INTO TransactionLogs (transaction_id, customer_id, customer_name, payment_amount, payment_mode, status)
                VALUES (?, ?, ?, ?, ?, ?);
            """, (
                int(tx["transaction_id"]),
                int(cust_id) if cust_id is not None else None,
                cust_name,
                float(amount),
                mode,
                tx["status"]
            ))
            if cursor.rowcount > 0:
                records_inserted += 1
                
        conn.commit()
        logging.info(f"Successfully loaded {records_inserted} records into TransactionLogs table.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting transaction records: {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()
            
    return records_inserted

if __name__ == "__main__":
    # Test initialization
    db = Path(__file__).parent / "database" / "customers.db"
    initialize_database(db)
