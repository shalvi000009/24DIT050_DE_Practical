import json
from pathlib import Path
import pandas as pd
from tabulate import tabulate

def profile_dataset(file_path):
    """
    Auto-detects format (CSV or JSON), profiles the dataset,
    and returns a structured dictionary along with printing an ASCII report.
    
    Parameters:
        file_path (str or Path): Path to the dataset to profile.
        
    Returns:
        dict: Profiling results including row count, col count, null counts, duplicates.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found for profiling: {file_path}")
        
    print(f"\n" + "=" * 80)
    print(f" PROFILING DATASET: {file_path.name}")
    print("=" * 80)
    
    # 1. Load data into DataFrame
    if file_path.suffix.lower() == '.csv':
        df = pd.read_csv(file_path)
    elif file_path.suffix.lower() == '.json':
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Flatten the nested JSON structure
        df = pd.json_normalize(data)
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")
        
    # 2. Basic Metadata
    num_rows = len(df)
    num_cols = len(df.columns)
    column_names = list(df.columns)
    
    # Duplicate rows (entire row matching)
    duplicate_rows = df.duplicated().sum()
    
    print(f"Dataset Shape    : {num_rows} rows x {num_cols} columns")
    print(f"Duplicate Rows   : {duplicate_rows}")
    print("-" * 80)
    
    # 3. Column-wise Profiling Details
    col_summary = []
    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_pct = (null_count / num_rows * 100) if num_rows > 0 else 0.0
        unique_count = df[col].nunique(dropna=True)
        detected_type = str(df[col].dtype)
        
        # Sample value
        non_null_samples = df[col].dropna()
        sample_val = str(non_null_samples.iloc[0]) if not non_null_samples.empty else "N/A"
        if len(sample_val) > 25:
            sample_val = sample_val[:22] + "..."
            
        col_summary.append([
            col,
            detected_type,
            null_count,
            f"{null_pct:.1f}%",
            unique_count,
            sample_val
        ])
        
    headers = ["Column Name", "Data Type", "Nulls", "Null %", "Uniques", "Sample Value"]
    print(tabulate(col_summary, headers=headers, tablefmt="grid"))
    
    # 4. Data Distribution for Numeric Columns
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        print("\nNumeric Columns Data Distribution:")
        print("-" * 80)
        dist_summary = []
        for col in numeric_cols:
            desc = df[col].describe()
            dist_summary.append([
                col,
                f"{desc['min']:.2f}",
                f"{desc['25%']:.2f}",
                f"{desc['50%']:.2f}",
                f"{desc['75%']:.2f}",
                f"{desc['max']:.2f}",
                f"{desc['mean']:.2f}"
            ])
        dist_headers = ["Column Name", "Min", "25%", "50%", "75%", "Max", "Mean"]
        print(tabulate(dist_summary, headers=dist_headers, tablefmt="grid"))
    else:
        print("\nNo numeric columns found for data distribution analysis.")
        
    print("=" * 80 + "\n")
    
    # Return metrics for main execution logging
    return {
        "file_name": file_path.name,
        "rows": num_rows,
        "columns": num_cols,
        "duplicates": int(duplicate_rows),
        "columns_list": column_names
    }

if __name__ == "__main__":
    # Test execution
    base_dir = Path(__file__).parent / "data"
    csv_file = base_dir / "customers.csv"
    json_file = base_dir / "transactions.json"
    
    if csv_file.exists():
        profile_dataset(csv_file)
    if json_file.exists():
        profile_dataset(json_file)
