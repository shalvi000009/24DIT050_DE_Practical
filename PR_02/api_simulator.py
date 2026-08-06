import os
import json
import random
from pathlib import Path
from faker import Faker

def generate_transactions(file_path, num_records=150):
    """
    Generates synthetic nested JSON API transaction records with intentional anomalies.
    Anomalies include: missing transaction IDs, duplicate transaction IDs, 
    invalid transaction amounts (negative or zero), missing status, or null customer info.
    
    Parameters:
        file_path (str or Path): Path where the JSON file should be saved.
        num_records (int): Total number of transaction logs to generate.
    """
    fake = Faker()
    random.seed(42)
    Faker.seed(42)
    
    transactions = []
    base_tx_id = 10000
    
    # Pre-generate some customers for realistic lookup
    customers_pool = [{"id": random.randint(1, 300), "name": fake.first_name()} for _ in range(50)]
    payment_modes = ["UPI", "Credit Card", "Debit Card", "NetBanking", "Wallet"]
    statuses = ["SUCCESS", "PENDING", "FAILED"]
    
    for i in range(num_records):
        anomaly_roll = random.random()
        
        # 1. Transaction ID with anomalies
        if anomaly_roll < 0.04:  # 4% chance of missing ID
            tx_id = None
        elif 0.04 <= anomaly_roll < 0.08:  # 4% chance of duplicate ID
            if len(transactions) > 0:
                prev_tx = random.choice(transactions)
                tx_id = prev_tx.get("transaction_id")
                # Fallback if selected is None
                if tx_id is None:
                    tx_id = base_tx_id + i
            else:
                tx_id = base_tx_id + i
        else:
            tx_id = base_tx_id + i
            
        # 2. Customer Nested Object
        if anomaly_roll >= 0.08 and anomaly_roll < 0.12:  # 4% chance of null customer ID
            cust = {"id": None, "name": fake.first_name()}
        elif 0.12 <= anomaly_roll < 0.15:  # 3% chance of completely missing customer data
            cust = None
        else:
            cust = random.choice(customers_pool)
            
        # 3. Payment Nested Object
        if anomaly_roll >= 0.15 and anomaly_roll < 0.20:  # 5% chance of negative/zero amount
            amount = random.choice([-500, 0, -2500])
        elif 0.20 <= anomaly_roll < 0.23:  # 3% chance of invalid datatype for amount
            amount = "two-thousand"
        else:
            amount = float(random.randint(100, 15000))
            
        mode = random.choice(payment_modes)
        
        # 4. Status
        if anomaly_roll >= 0.23 and anomaly_roll < 0.26:  # 3% chance of null status
            status = None
        else:
            status = random.choice(statuses)
            
        # Construct Nested Structure
        tx_record = {}
        if tx_id is not None:
            tx_record["transaction_id"] = tx_id
            
        if cust is not None:
            tx_record["customer"] = cust
            
        tx_record["payment"] = {
            "amount": amount,
            "mode": mode
        }
        
        if status is not None:
            tx_record["status"] = status
            
        transactions.append(tx_record)
        
    # Ensure directory exists
    output_path = Path(file_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save as JSON
    try:
        with open(output_path, mode='w', encoding='utf-8') as json_file:
            json.dump(transactions, json_file, indent=4)
        print(f"API Transactions successfully saved to: {output_path.resolve()}")
    except Exception as e:
        print(f"Error saving transactions JSON dataset: {e}")
        raise e

if __name__ == "__main__":
    target = Path(__file__).parent / "data" / "transactions.json"
    generate_transactions(target)
