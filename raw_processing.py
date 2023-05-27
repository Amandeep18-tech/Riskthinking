import os
import pandas as pd
from prefect import task, flow
from prefect_dask.task_runners import DaskTaskRunner
from math import ceil

@task
def process_data(file_path, symbol, mapping_data, columns):
    # Read the file data
    file_data = pd.read_csv(file_path)

    # Filter the mapping data based on the symbol
    matching_symbols = mapping_data[mapping_data["Symbol"] == symbol]

    if not matching_symbols.empty:
        # Get the security name for the symbol
        security_name = matching_symbols["Security Name"].values[0]
    else:
        # Assign a default value if no match found
        security_name = "N/A"

    # Add the symbol and security name to the file data
    file_data["Symbol"] = symbol
    file_data["Security Name"] = security_name

    # Return the relevant columns of the file data
    return file_data[columns]

def file_paths():
    dataset_dir="./stock_market_dataset"
    file_paths = []
    etf_dir = os.path.join(dataset_dir, "etfs")
    for filename in os.listdir(etf_dir):
        file_path = os.path.join(etf_dir, filename)
        if os.path.isfile(file_path):
            file_paths.append((file_path, os.path.splitext(filename)[0]))

    # Collect file paths from Stocks directory
    stock_dir = os.path.join(dataset_dir, "stocks")
    for filename in os.listdir(stock_dir):
        file_path = os.path.join(stock_dir, filename)
        if os.path.isfile(file_path):
            file_paths.append((file_path, os.path.splitext(filename)[0]))
    
    return file_paths

def chunk_into_n(lst, n):
    size = ceil(len(lst) / n)
    return list(
    map(lambda x: lst[x * size:x * size + size],
    list(range(n)))
    )

@task
def combine_data(dataset_dir,file_paths,no_of_files):
    # Define the columns to retain in the final dataset
    columns = ["Symbol", "Security Name", "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]

    # Read the symbol and security name mapping file
    mapping_file = os.path.join(dataset_dir,  "symbols_valid_meta.csv")
    mapping_data = pd.read_csv(mapping_file)
    processed_files = []
    # Create an empty DataFrame to store the combined ETF and stock data
    combined_data = pd.DataFrame(columns=columns)
    for file_path, symbol in file_paths:
        processed_file = process_data.fn(file_path, symbol, mapping_data, columns)
        processed_files.append(processed_file)

    combined_data= pd.concat(processed_files)
    print(len(combined_data))
    
    combined_data.to_csv(r'combined_data-{}.csv'.format(no_of_files))
    
def chunks():
    files=file_paths()
    file_chunks=chunk_into_n(files,8)
    return file_chunks

dataset_dir = "./stock_market_dataset"

@task
def combine_csv_task(no_of_files):
    combined_csv = pd.DataFrame()
    dfs = []
    for i in range(no_of_files):
        filename = f'combined_data-{i}.csv'
        try:
            # Read each CSV file and append its data to the combined DataFrame
            df = pd.read_csv(filename)
            dfs.append(df)
        except FileNotFoundError:
            print(f"File {filename} not found. Skipping...")
    combined_csv = pd.concat(dfs, ignore_index=True)
    print(len(combined_csv))
    combined_csv.to_parquet('combined_data.parquet')

@flow
def combine_data_flow(no_of_files):
    combine_csv_task(no_of_files)

@flow(task_runner=DaskTaskRunner())
def make_csv_flow():
    file_chunks=chunks()
    no_of_files=0
    for chunk in file_chunks:
        combine_data.submit(dataset_dir,chunk,no_of_files)
        no_of_files+=1
    

if __name__ == "__main__":
    make_csv_flow()
    combine_data_flow(8)
