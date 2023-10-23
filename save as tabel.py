#Table has 2 parts
#1. data - warehouse - spark.sql.warehouse.dir
#2. metadata - catalog metastore - memory

from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf= SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf= my_conf).getOrCreate()

orderdf=spark.read.format("csv").option("header", True).option("inferSchema", True).option("path", "C:\\Users\\sathish\\BIG DATA\\week 12\\orders-201025-223502.csv").load()

orderdf.write.format("csv").mode("overwrite").saveAsTable('order1')