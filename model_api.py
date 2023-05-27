
from flask import Flask, request
import joblib

app = Flask(__name__)

# Load the trained model
model = joblib.load('finalized_model.joblib')

@app.route('/predict', methods=['GET'])
def predict():
    # Get the query parameters from the request
    vol_moving_avg = float(request.args.get('vol_moving_avg'))
    adj_close_rolling_med = float(request.args.get('adj_close_rolling_med'))

    # Perform prediction using the trained model
    prediction = model.predict([[vol_moving_avg, adj_close_rolling_med]])

    # Return the prediction as the response
    return str(int(prediction[0]))

if __name__ == '__main__':
    # Run the Flask app
    app.run(host='0.0.0.0')
