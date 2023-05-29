import pandas as pd

def compute_vol_adj():
    df=pd.read_parquet('combined_data.parquet')
    print(len(df))
    # Convert the 'Date' column to datetime type
    df['Date'] = pd.to_datetime(df['Date'])

    # Sort the dataframe by 'Symbol' and 'Date' columns
    df.sort_values(['Symbol', 'Date'], inplace=True)

    # Calculate the moving average of the trading volume for each stock and ETF
    df['vol_moving_avg'] = df.groupby('Symbol')['Volume'].rolling(window=30).mean().reset_index(0, drop=True)

    # Calculate the rolling median of the 'Adj Close' column for each stock and ETF
    df['adj_close_rolling_med'] = df.groupby('Symbol')['Adj Close'].rolling(window=30).median().reset_index(0, drop=True)

    # Write the resulting dataset to a new Parquet file
    df.to_parquet('result_dataset.parquet')
    
if __name__ == "__main__":
    compute_vol_adj()