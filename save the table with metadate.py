# metadate will store till the machince is on, once you exit metadate will be remove/delete.

from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf= SparkConf()
my_conf= my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf= my_conf).getOrCreate()

orderdf=spark.read.format("csv").option("header", True).option("inferSchema", True).option("path", "C:\\Users\\sathish\\BIG DATA\\week 12\\orders-201025-223502.csv").load()


spark.sql("create database if not exists retail")

orderdf.write.format("csv").mode("overwrite").saveAsTable('retail.order2')
