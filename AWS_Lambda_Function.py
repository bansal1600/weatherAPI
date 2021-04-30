import json
import os
import boto3
import csv
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # TODO: write code...
    bucket = event['Records'][0]['s3']['bucket']['name']
    csv_file = event['Records'][0]['s3']['object']['key']
    csv_file_obj = s3_client.get_object(Bucket=bucket, Key=csv_file)
    lines = csv_file_obj['Body'].read().decode('utf-8').splitlines()
    
    rows = []
    data = csv.reader(lines)
    headers = next(data)
    for line in data:
        rows.append(line)
    print(rows)
    print('headers: %s' %(headers))
    
    
    #connect to the RDS INSTANCE
    connection = mysql.connector.connect(host='app1project1db.c71mgqopnjvt.us-east-2.rds.amazonaws.com',database='mydb',user='admin',password='admin123')
    mysql_empsql_insert_query = "INSERT INTO weather_data (city, Cloudiness, Country, Date, Humidity, Lat, lng, Max_Temp, Wind_Speed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor = connection.cursor()
    cursor.executemany(mysql_empsql_insert_query,rows)
    connection.commit()
    print(cursor.rowcount, "Record inserted successfully into employee table")
    
    if(connection.is_connected()):
        connection.close()
        print("MYSQL connection is closed")
    
    return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
                }