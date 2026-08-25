import argparse
import os
import random
from datetime import datetime, timedelta
import pandas as pd

def generate_telemetry_data(num_records: int = 2000, output_file: str = "data/user_logs.csv"):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    users = ["Alice", "Bob", "Charlie", "Diana", "Evan", "Fiona", "George", "Hannah", "Ian", "Julia"]
    actions = ["Login", "Logout", "View", "Purchase", "Search", "Upload", "Download"]
    status_codes = [200, 200, 200, 200, 201, 304, 400, 404, 500]
    
    base_time = datetime.now() - timedelta(minutes=30)
    data = []
    
    for i in range(1, num_records + 1):
        # Generate incrementally increasing timestamp with small random intervals
        event_time = base_time + timedelta(milliseconds=i * random.randint(10, 50))
        timestamp_str = event_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        record = {
            "event_id": i,
            "timestamp": timestamp_str,
            "user": random.choice(users),
            "action": random.choice(actions),
            "cpu": round(random.uniform(5.0, 95.0), 2),
            "memory": round(random.uniform(20.0, 90.0), 2),
            "response_time": random.randint(10, 450),
            "status_code": random.choice(status_codes)
        }
        data.append(record)
        
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)
    print(f"[Generator] Successfully generated {num_records} telemetry records -> {output_file}")
    return output_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate web application server telemetry dataset.")
    parser.add_argument("--records", type=int, default=2000, help="Number of records to generate")
    parser.add_argument("--output", type=str, default="data/user_logs.csv", help="Output CSV path")
    args = parser.parse_args()
    
    generate_telemetry_data(num_records=args.records, output_file=args.output)
