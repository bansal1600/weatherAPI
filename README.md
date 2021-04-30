# weather

WeatherAPI ETL Job

* Step1 and Step 2 is scheduled for daily at 09:00 AM CST using schedule package. 
# Step1 - 
* weatehr_ETL.py -> 
  * This will call the OpenWeatherAPI to get the weather updates for Dallas and Detroit.
  * Then A csv is generated named as weather_data_3042021.csv
  * Then this generated csv will be stored in S3 bucket.
# Step2
  * As soon as data is uploaded to s3 bucket a lambda function(AWS_Lambda_Function.py).
  * AWS_Lambda_Function.py will create a database(mydb) then create a table weather_data in RDS MYSQL instance to store the data from the csv.

# Step3
  * DataRetrieval.py will connect to the RDS Instance that can run select query to fetch data from the table.
