# import sys
# import subprocess

# subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'psycopg2'])
# subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'boto3'])

import requests
import schedule
import time
import datetime as dt
import pandas as pd
import warnings
import datetime
import boto3
from botocore.exceptions import NoCredentialsError
import psycopg2
import json

warnings.filterwarnings('ignore')

# Save config information.


# Loop through the list of cities and perform a request for data on each
def job():
    url = "http://api.openweathermap.org/data/2.5/weather?"
    units = "imperial"

    # OpenWeatherMap API Key
    api_key = "2e4308cb04a3e7ff677c66e1e413e691"

    # Build partial query URL
    query_url = f"{url}appid={api_key}&units={units}&q="

    # Lists for holding variables
    city_name = []
    cloudiness = []
    country = []
    date = []
    humidity = []
    lat = []
    lng = []
    max_temp = []
    wind_speed = []

    # Initiate count for city number below
    count = 1

    print(f"Beginning Data Retrieval")
    print("-" * 40)

    cities = ['Dallas']
    for city in cities:
        try:
            response = requests.get(query_url + city).json()
            city_name.append(response["name"])
            cloudiness.append(float(response["clouds"]["all"]))
            country.append(response["sys"]["country"])
            timestamp = datetime.datetime.fromtimestamp(response["dt"])
            date.append(timestamp.strftime('%Y-%m-%d'))
            humidity.append(float(response["main"]["humidity"]))
            max_temp.append(float(response["main"]["temp_max"]))
            lat.append(float(response["coord"]["lat"]))
            lng.append(float(response["coord"]["lon"]))
            wind_speed.append(float(response["wind"]["speed"]))

            print(f"Processing Record {count} | {city}")

            count += 1

        except:
            print(f"City not found. Skipping...")
            continue
        print("-" * 40)
        print(f"Data Retrieval Complete")
        print("-" * 40)

        # build a data frame's titles
        weather_dict = {
            "City": city_name,
            "Cloudiness": cloudiness,
            "Country": country,
            "Date": date,
            "Humidity": humidity,
            "Lat": lat,
            "Lng": lng,
            "Max Temp": max_temp,
            "Wind Speed": wind_speed
        }

# Create df
        weather_data = pd.DataFrame(weather_dict)

        # Save data frame to CSV
        current_date = datetime.datetime.now()
        filename = 'weather_data_' + str(current_date.day) + str(current_date.month) + str(current_date.year)
        weather_data.to_csv(str(filename + '.csv'), index=False)
        return filename

########put the data to s3 bucket now###########
        with open('keyjson.json') as json_file:
            data = json.load(json_file)
        ACCESS_KEY = data[0]['AWSAccessKeyId']
        SECRET_KEY = data[0]['AWSSecretKey']
        current_date = datetime.datetime.now()
        local_file = filename + '.csv'
        s3_file = filename + '.csv'
        bucket = 'data-openweather-api'
        data_from_s3 = 's3_data_' + str(current_date.day) + str(current_date.month) + str(current_date.year) + ".csv"

        s3 = boto3.client('s3', region_name="us-east-1", aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY, verify=False)

        try:
            s3.upload_file(local_file, bucket, 'csvdata/{}'.format(s3_file))
            print("Upload Successful")
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False


schedule.every().days.at("08:00").do(job)
# schedule.every(2).minutes.do(job)
while True:
    schedule.run_pending()
