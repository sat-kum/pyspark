from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()

my_conf.set("spark.app.name", "first conf_file")

my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

spark.stop()

