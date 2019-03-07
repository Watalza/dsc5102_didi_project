from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf, from_unixtime
from pyspark.sql import functions as F
from math import radians, cos, sin, asin, sqrt, atan2, degrees
from datetime import datetime
from pyspark.sql.window import Window
import os



def spark_pipeline(file):
	sc = SparkContext("local", "Simple App")
	sqlContext = SQLContext(sc)

	spark = SparkSession.builder.master("local").\
		appName("Simple App").\
		config("spark.some.config.option", "some-value").\
		getOrCreate()

	fields = [StructField('driver_id', StringType(), True),
	StructField('order_id', StringType(), True),
	StructField('timestamp', StringType(), True),
	StructField('lon', DoubleType(), True),
	StructField('lat', DoubleType(), True)]
	schema = StructType(fields)

	didi_df = sqlContext.read.csv(file, schema=schema)

	didi_df_1 = didi_df.withColumn('timestamp', F.from_unixtime("timestamp"))

	lat_max = didi_df_1.agg({"lat": "max"}).collect()[0][0]
	lat_min = didi_df_1.agg({"lat": "min"}).collect()[0][0]
	lon_max = didi_df_1.agg({"lon": "max"}).collect()[0][0]
	lon_min = didi_df_1.agg({"lon": "min"}).collect()[0][0]
	time_max = didi_df_1.agg({"timestamp": "max"}).collect()[0][0]
	time_min = didi_df_1.agg({"timestamp": "min"}).collect()[0][0]

	sc.stop()

	current_time = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
	return (file, current_time, lat_max, lat_min, lon_max, lon_min, time_max, time_min)




def main():
	sample_data_1_path = 'xian_sample_1.csv'
	full_data_1_path = 'gps_20161001.csv'
	sample_dir = '../data/'
	full_data_dir = '../data_full/xian/'
	output_dir = '../log/boundary.csv'


	with open(output_dir, 'a') as outf:
		for filename in os.listdir(full_data_dir):
			# if filename.endswith('.csv'):
			print(sample_dir+filename)
			output = spark_pipeline(full_data_dir+filename)
			outf.write(','.join([str(i) for i in output]) + "\n")

if __name__ == '__main__':
	main()


