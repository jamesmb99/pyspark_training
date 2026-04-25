print('hello')

!pip install pyspark

from google.colab import drive
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, unix_timestamp, round

drive.mount('/content/drive')

pyspark.__version__

import requests

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
output_file = "taxi_zone_lookup.csv"

response = requests.get(url)
response.raise_for_status()  # raises error if download fails

with open(output_file, "wb") as f:
    f.write(response.content)

print("Downloaded successfully:", output_file)

response1 = requests.get('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet')
response1.raise_for_status()

with open('yellow_tripdata_2025-01.parquet', "wb") as g:
  g.write(response1.content)

print("Downloaded successfully:", "yellow_tripdata_2025-01.parquet")

response2 = requests.get('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-02.parquet')
response2.raise_for_status()

with open('yellow_tripdata_2025-02.parquet', "wb") as h:
  h.write(response2.content)

print("Downloaded successfully:", "yellow_tripdata_2025-02.parquet")

# Commented out IPython magic to ensure Python compatibility.
# %%sh
# pwd
# ls /content/pyspark_training

os.path.isfile('/content/pyspark_training/taxi_zone_lookup.csv')

os.path.isdir('/content/pyspark_training')

spark = SparkSession.builder.getOrCreate()

spark

january_file = '/content/pyspark_training/yellow_tripdata_2025-01.parquet'
df = spark.read.parquet(january_file)

df.show(5)

# now of rows
df.count()

df.schema.names

df.printSchema()

df.describe(['passenger_count', 'trip_distance', 'total_amount']).show()

df.select('passenger_count').show()

df.select(col('passenger_count')).show()

df.sort('total_amount', ascending=False).show()

df.sort(['total_amount', 'passenger_count'], ascending = [False, False]).show()

df.filter('Airport_fee > 0').show()

df.filter('Airport_fee = 0' and 'passenger_count = 1')\
.select(['trip_distance', 'total_amount'])\
.sort('trip_distance', ascending=False)\
.show()

df.filter(isnull(col('fare_amount'))).count()

df.filter(isnull(col('passenger_count'))).count()

df1 = df.fillna({'passenger_count': 1})

df1.show()

df1.filter(isnull(col('passenger_count'))).count()

df1 = df.withColumn('trip_duration_minutes',
                    round((unix_timestamp('tpep_dropoff_datetime') -  unix_timestamp('tpep_pickup_datetime'))/60, 1))\
                    .sort('trip_duration_minutes', ascending=False)\
                    .show()

# withColumnRenamed
# drop('col1', 'col2')
