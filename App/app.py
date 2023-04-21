import pickle
import pandas as pd
import math
from flask import Flask, render_template, request, jsonify, redirect

app = Flask(__name__)


# load the regressor model from the pickle file
with open('bike_predictor.pkl', 'rb') as handle:
    regressor = pickle.load(handle)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/predict', methods=['POST'])
def predict():
    # get the input values from the request
    data_dict = {
        'number': request.form['number'],
        'hour': request.form['hour'],
        'minute': request.form['minute'],
        'month': request.form['month'],
        'weather_description': request.form['weather_description'],
        'main_temp': request.form['main_temp'],
        'main_humidity': request.form['main_humidity'],
        'wind_speed': request.form['wind_speed'],
        'dayofweek': request.form['dayofweek']
    }
    data_df = pd.DataFrame.from_dict(data_dict, orient='index').T
    prediction = regressor.predict(data_df)
    output = "{:.0f}".format(prediction[0])
    return output





if __name__ == '__main__':
    app.run(debug=True)
