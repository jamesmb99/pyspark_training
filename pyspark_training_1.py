print('hello')

!pip install pyspark

from google.colab import drive
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, unix_timestamp, round

#drive.mount('/content/drive')

import pyspark
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
# mkdir /content/pyspark_training
# mv /content/taxi_zone_lookup.csv /content/pyspark_training
# mv /content/yellow_tripdata_2025-01.parquet /content/pyspark_training
# mv /content/yellow_tripdata_2025-02.parquet /content/pyspark_training

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

feb_file = '/content/pyspark_training/yellow_tripdata_2025-02.parquet'
df_feb = spark.read.parquet(feb_file)

df_2025 = df.union(df_feb)

df_2025.count()

taxi_zone_lookup_file = '/content/pyspark_training/taxi_zone_lookup.csv'
taxi_zone_lookup = spark.read.option('header','true').csv(taxi_zone_lookup_file)

taxi_zone_lookup.show()

df_joined = df_2025.join(taxi_zone_lookup, df_2025.PULocationID == taxi_zone_lookup.LocationID, how='left')

df_joined.show()

df.groupBy('payment_type').count().sort('payment_type').show()

df.groupBy('payment_type').avg('total_amount').sort('payment_type').show()

taxi = spark.read.parquet('/content/pyspark_training/yellow_tripdata_2025-01.parquet')

taxi.createOrReplaceTempView('taxi')

spark.sql('select * from taxi where total_amount > 50').show();

spark.sql('select * from taxi where total_amount > 50')\
.filter('passenger_count > 2')\
.select('payment_type', 'passenger_count', 'total_amount')\
.show()

query = '''
SELECT
  payment_type,
  passenger_count,
  total_amount
FROM
  taxi
WHERE
  total_amount > 50
  AND passenger_count > 2
'''

spark.sql(query).show()

taxi_jan2025 = spark.read.parquet('/content/pyspark_training/yellow_tripdata_2025-01.parquet')
taxi_zone_lookup = spark.read.option('header', 'true').csv('/content/pyspark_training/taxi_zone_lookup.csv')

taxi_jan2025.createOrReplaceTempView('taxi_jan2025')
taxi_zone_lookup.createOrReplaceTempView('taxi_zone_lookup')

joined_df = spark.sql('select DOLocationID, Borough, total_amount from taxi_jan2025 left join taxi_zone_lookup on DOLocationID = LocationID')
joined_df.show()

from pyspark.sql.functions import avg

joined_df.groupBy('Borough').agg(avg('total_amount').alias('avg_amount')).show()
