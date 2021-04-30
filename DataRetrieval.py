import pymysql
import pandas as pd
# get_ipython().system('pip install PyMySql')

host = 'app1project1db.c71mgqopnjvt.us-east-2.rds.amazonaws.com'
user = 'admin'
password = 'admin123'
database = 'mydb'

connection = pymysql.connections.Connection(host = host, user = user, password = password, database = database)
with connection:
    cur = connection.cursor()
    cur.execute("SELECT VERSION()")
    version = cur.fetchone()
    print("Database version: {} ".format(version[0]))
    sql = '''select * from mydb.weather_data'''
    cur.execute(sql)
    print(pd.DataFrame(cur.fetchall(), columns = ['city', 'Cloudiness','Country', 'Date', 'Humidity', 'Lat', 'lng',
                                                 'Max Temp', 'Wind Speed']))