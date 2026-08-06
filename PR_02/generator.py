import os
import random
import csv
from pathlib import Path
from faker import Faker

def generate_customers(file_path, num_records=200):
    """
    Generates synthetic customer records with intentional anomalies using Faker.
    Anomalies include: missing IDs, duplicate IDs, null values, invalid email formats, 
    and invalid ages (negative and under 18).
    
    Parameters:
        file_path (str or Path): Path where the CSV file should be saved.
        num_records (int): Total number of customer records to generate.
    """
    fake = Faker()
    # Set seed for reproducible anomaly distribution
    random.seed(42)
    Faker.seed(42)
    
    records = []
    
    # Base ID sequence
    base_id = 1000
    
    for i in range(num_records):
        # Determine if we should generate an anomaly
        anomaly_roll = random.random()
        
        # 1. Customer_ID Generation (Normal is base_id + i)
        if anomaly_roll < 0.05:  # 5% chance of missing ID
            cust_id = ""
        elif 0.05 <= anomaly_roll < 0.10:  # 5% chance of duplicate ID
            # Duplicate the ID of a previous record if available, else standard
            if len(records) > 0:
                cust_id = random.choice(records)["Customer_ID"]
                # If chosen record has a blank ID, fallback to base
                if not cust_id:
                    cust_id = str(base_id + i)
            else:
                cust_id = str(base_id + i)
        else:
            cust_id = str(base_id + i)
            
        # 2. Name Generation
        if anomaly_roll >= 0.10 and anomaly_roll < 0.14:  # 4% chance of null name
            name = ""
        else:
            name = fake.name()
            
        # 3. Email Generation
        if anomaly_roll >= 0.14 and anomaly_roll < 0.18:  # 4% chance of null email
            email = ""
        elif 0.18 <= anomaly_roll < 0.23:  # 5% chance of invalid email format
            invalid_formats = [
                f"{fake.user_name()}@",               # Missing domain
                f"@{fake.domain_name()}",             # Missing username
                f"{fake.user_name()}_at_{fake.domain_name()}", # Missing @
                f"{fake.user_name()}@ {fake.domain_name()}"    # Space in email
            ]
            email = random.choice(invalid_formats)
        else:
            email = fake.email()
            
        # 4. Phone Generation
        if anomaly_roll >= 0.23 and anomaly_roll < 0.26:  # 3% chance of null phone
            phone = ""
        else:
            phone = fake.phone_number()
            
        # 5. Age Generation
        if anomaly_roll >= 0.26 and anomaly_roll < 0.30:  # 4% chance of negative age
            age = random.randint(-50, -1)
        elif 0.30 <= anomaly_roll < 0.35:  # 5% chance of underage (under 18)
            age = random.randint(1, 17)
        elif 0.35 <= anomaly_roll < 0.37:  # 2% chance of empty age
            age = ""
        else:
            age = random.randint(18, 85)
            
        # 6. City Generation
        city = fake.city()
        
        records.append({
            "Customer_ID": cust_id,
            "Name": name,
            "Email": email,
            "Phone": phone,
            "Age": age,
            "City": city
        })
        
    # Ensure directory exists
    output_path = Path(file_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save as CSV
    try:
        with open(output_path, mode='w', newline='', encoding='utf-8') as csv_file:
            fieldnames = ["Customer_ID", "Name", "Email", "Phone", "Age", "City"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for record in records:
                writer.writerow(record)
        print(f"Dataset successfully saved to: {output_path.resolve()}")
    except Exception as e:
        print(f"Error saving customer dataset: {e}")
        raise e

if __name__ == "__main__":
    # Test generation run
    target = Path(__file__).parent / "data" / "customers.csv"
    generate_customers(target)
