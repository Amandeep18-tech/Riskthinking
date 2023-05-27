import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib
import shutil
import zipfile
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor


df=pd.read_parquet('result_dataset.parquet')
# Assume `data` is loaded as a Pandas DataFrame
df['Date'] = pd.to_datetime(df['Date'])
df.set_index('Date', inplace=True)

# Remove rows with NaN values
df.dropna(inplace=True)
df = df.sample(frac=0.01)
print(len(df))
# Select features and target
features = ['vol_moving_avg', 'adj_close_rolling_med']
target = 'Volume'

X = df[features]
y = df[target]

# Split data into train and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Create a RandomForestRegressor model
model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)

# Train the model
model.fit(X_train.values, y_train)

# Make predictions on test data
y_pred = model.predict(X_test.values)

result = model.score(X_test.values, y_test)
print(result)


# Calculate the Mean Absolute Error and Mean Squared Error
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
print(mae, mse)

def compress_file(source_file, zip_file):
    joblib.dump(model, source_file)
    # Create a ZipFile object with write mode
    with zipfile.ZipFile(zip_file, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        # Add the source file to the zip file
        zipf.write(source_file, arcname=source_file)

# Specify the source file and the target zip file
source_file = 'finalized_model.joblib'
zip_file = 'finalized_model.zip'

# Compress the file
num_threads = multiprocessing.cpu_count()
executor = ThreadPoolExecutor(max_workers=num_threads)

# Compress the file using multithreading
executor.submit(compress_file, source_file, zip_file)