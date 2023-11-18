import pandas as pd

# Replace this with the path to your Parquet file
parquet_file_path = 'C:/Users/André/Documents/data_engineer/k8s-airflow-EKS/test/raw_output.parquet'

try:
    with open(parquet_file_path, 'r') as file:
        sql_content = file.read()
        print(sql_content)
except FileNotFoundError:
    print("File not found. Please check the file path.")
except Exception as e:
    print(f"An error occurred: {e}")