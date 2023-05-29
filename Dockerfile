# Use an official Python runtime as the base image
FROM python:3.10

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file and install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt


# Copy all the necessary files to the working directory
COPY app.py model_training.py parquet_processing.py raw_processing.py ./

VOLUME ["/app/stock_market_dataset/stocks","/app/stock_market_dataset/etfs"]

# Run the app.py file
CMD ["python", "app.py"]
