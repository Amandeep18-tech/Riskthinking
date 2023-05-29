import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib
import logging
def run_model():
    df=pd.read_parquet('result_dataset.parquet')
    # Assume `data` is loaded as a Pandas DataFrame
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)

    # Remove rows with NaN values
    df.dropna(inplace=True)
    print(len(df))
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


    # Calculate the Mean Absolute Error and Mean Squared Error
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)

    # Specify the source file and the target zip file
    source_file = 'finalized_model.joblib'

    joblib.dump(model, source_file)

    logging.basicConfig(filename='model_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Log the values
    logging.info('Predictions: {}'.format(result))
    logging.info('MSE: {}'.format(mse))
    logging.info('MAE: {}'.format(mae))