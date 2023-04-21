#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/home/ubuntu/miniconda3/envs/comp30830


# In[84]:


import requests
import json
import time
import threading
import mysql.connector
import sys


# In[85]:


# Setting up the database connection
try:
    dbbikes = mysql.connector.connect(host="dbbikes.cvwut6jnqsvn.us-east-1.rds.amazonaws.com", user="admin", passwd="password", database="dbbikes")

except mysql.connector.Error as error:
    print("Unable to connect to database: {}".format(error))
    sys.exit(1)


# In[86]:


# connecting to get the bike data from the jcdecaux API
api_endpoint = "https://api.jcdecaux.com/vls/v1/stations"
params = {
    "contract": "dublin",
    "apiKey": "e58b454e0c21037a669b05e177dd6d3e910eb9fb"
}
try:
    bike_response = requests.get(api_endpoint,params=params).json()

except requests.exceptions.RequestException as e:
    print(e)
    sys.exit(1)


# In[87]:


# weather
weatherApiKey = "8989835a2bce4268768202b1dd2b056b"
weather_ID = "2964574"
weather_url = f"https://api.openweathermap.org/data/2.5/weather?id={weather_ID}&appid={weatherApiKey}"
try:
    weather_request = requests.get(weather_url)
    weather_response = weather_request.json()

except requests.exceptions.RequestException as e:
    print(e)
    sys.exit(1)


# In[88]:


# Create MYsql Table
sql_cursor = dbbikes.cursor()
# sql_cursor.execute("DROP TABLE IF EXISTS Bike")
sql_cursor.execute('''CREATE TABLE IF NOT EXISTS Bike (
    number INT,
    contract_name VARCHAR(255),
    name VARCHAR(255),
    bike_stands INT,
    available_bike_stands INT,
    available_bikes INT,
    status VARCHAR(255),
    last_update TIMESTAMP,
    PRIMARY KEY (number, last_update)
)''')
sql_cursor.execute("CREATE TABLE IF NOT EXISTS Weather (coord_lon VARCHAR(255), coord_lat VARCHAR(255), weather_id VARCHAR(255), weather_main VARCHAR(255), weather_description VARCHAR(255),weather_icon VARCHAR(255), weather_base VARCHAR(255) ,main_temp VARCHAR(255),feels_like VARCHAR(255), main_temp_min VARCHAR(255), main_temp_max VARCHAR(255), main_pressure VARCHAR(255), main_humidity VARCHAR(255), main_visibility VARCHAR(255),wind_speed VARCHAR(255), wind_deg VARCHAR(255),  clouds_all VARCHAR(255), dt VARCHAR(255), sys_type VARCHAR(255), sys_id VARCHAR(255), sys_country VARCHAR(255),sys_sunrise VARCHAR(255), sys_sunset VARCHAR(255), city_id VARCHAR(255), city_name VARCHAR(255), cod VARCHAR(255))")


# In[89]:


# Create Tables Queries for inserting data
bike_table = "INSERT IGNORE INTO Bike(number, contract_name, name, bike_stands, available_bike_stands, "              "available_bikes, status, last_update) "              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

weather_table = "INSERT INTO Weather (coord_lon,coord_lat,weather_id,weather_main,weather_description, "                 "weather_icon, weather_base , main_temp, feels_like , main_temp_min, main_temp_max, main_pressure, main_humidity, main_visibility, "                 "wind_speed,clouds_all, dt, sys_type, sys_id, sys_country, "                 "sys_sunrise, sys_sunset,city_id,city_name,cod) "                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)"


# In[90]:


try:
    sql_cursor = dbbikes.cursor()

    # Iterate through the data response object and perform inserts
    for i in range(len(bike_response)):
        Bike_data = (bike_response[i]["number"], bike_response[i]["contract_name"], bike_response[i]["name"],
                     bike_response[i]["bike_stands"], bike_response[i]["available_bike_stands"],
                     bike_response[i]["available_bikes"], bike_response[i]["status"],
                     time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(bike_response[i]["last_update"] / 1000)))
        sql_cursor.execute(bike_table, Bike_data)

    Weather_data =(weather_response['coord']['lon'], weather_response['coord']['lat'], weather_response['weather'][0]['id'],
                   weather_response['weather'][0]['main'], weather_response['weather'][0]['description'],
                   weather_response['weather'][0]['icon'],
                   weather_response['base'], weather_response['main']['temp'], weather_response['main']['feels_like'],
                   weather_response['main']['temp_min'], weather_response['main']['temp_max'], weather_response['main']['pressure'],
                   weather_response['main']['humidity'], weather_response['visibility'], weather_response['wind']['speed'],
                   weather_response['clouds']['all'], time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(weather_response['dt'])), weather_response['sys']['type'],
                   weather_response['sys']['id'], weather_response['sys']['country'], time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(weather_response['sys']['sunrise'])),
                   time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(weather_response['sys']['sunset'])), weather_response['id'], weather_response['name'], weather_response['cod'])

    sql_cursor.execute(weather_table, Weather_data)

    dbbikes.commit()

    # Close the connection
    dbbikes.close()
except mysql.connector.Error as error:
    print("Connection to the database failed: {}".format(error))
    sys.exit(1)


# In[90]:




