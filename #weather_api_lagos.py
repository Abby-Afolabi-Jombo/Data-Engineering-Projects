#weather_api_lagos.py

import requests
import psycopg2
import pandas as pd
import json
from pandas import json_normalize
url = "https://api.openweathermap.org/data/2.5/weather?q=lagos&appid=47722860126fb0ee1b88d67df4d71fc3"


response = requests.get(url)

data = response.json()

print(data)

df = pd.json_normalize(data)

from datetime import date

today = date.today()
df['Date'] = today
df.info()
#print("Today's date:", today)

pd.DataFrame(df['weather'])
df['weather'].apply(pd.Series)
additinal_columns_d = ['Weather Description']
df[additinal_columns_d[-1:4]] = df['weather'].apply(pd.Series)

df.drop(columns = ['weather'],inplace = True)

df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

# Column to be dropped
column_to_drop = ('sys.type','sys.id','icon')
# Check if the column exists in the DataFrame before dropping it
if column_to_drop in df.columns:
    df.drop(columns=[column_to_drop], inplace=True)

pd.DataFrame(df['Weather Description'])
df['Weather Description'].apply(pd.Series)
additinal_columns_d = ['WeatherId','WeatherName','WeatherDescription','icon']
df[additinal_columns_d] = df['Weather Description'].apply(pd.Series)

num_columns = df.shape[1]
valid_indices = [26, 29, 27, 28, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25]
valid_indices = [i for i in valid_indices if i < num_columns]

df2 = df[['Date','name','WeatherName','WeatherDescription','WeatherId','icon','sys.country']]

import requests
import pandas as pd
import json
from pandas import json_normalize
from datetime import date
# Install required libraries
#!pip install pandas sqlalchemy psycopg2-binary
#!pip install jupyter_scheduler

# Import necessary libraries
import pandas as pd
from sqlalchemy import create_engine
import  time

import warnings
from urllib3.exceptions import InsecureRequestWarning

warnings.filterwarnings("ignore", category=InsecureRequestWarning)

# Database connection
conn = psycopg2.connect(
    dbname='Myairflowpostgresql',
    user='airflow_user',
    password='Abbysairflow',
    host='localhost',
    port='5432'
    )


cur = conn.cursor()

# Create table if it does not exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS public."Lagos_Weather"
    (
        "Date" timestamp without time zone,
        name text COLLATE pg_catalog."default",
        "WeatherName" text COLLATE pg_catalog."default",
        "WeatherDescription" text COLLATE pg_catalog."default",
        "WeatherId" bigint,
        icon text COLLATE pg_catalog."default",
        "sys.country" text COLLATE pg_catalog."default"
    )
""")

# Commit the table creation
conn.commit()

# Iterate over the DataFrame and insert each row into the database
for index, row in df2.iterrows():
    try:
        cur.execute("""
            INSERT INTO public."Lagos_Weather" ("Date", name, "WeatherName", "WeatherDescription", "WeatherId", icon, "sys.country")
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (row['Date'], row['name'], row['WeatherName'], row['WeatherDescription'], row['WeatherId'], row['icon'], row['sys.country']))
    except Exception as e:
        print(f"Error inserting row {index}: {e}")

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()