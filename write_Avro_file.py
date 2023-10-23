from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")
my_conf.set("spark.jars","C:\\Users\\sathish\\Downloads\\spark-avro_2.12-2.4.4.jar")

spark=SparkSession.builder.config(conf=my_conf).getOrCreate()

orderdf = spark.read.format("csv").option("header", True).option("inferScheme", True).option("path", "C:\\Users\\sathish\\BIG DATA\\week 12\\orders-201025-223502.csv").load()

orderdf.write.format("avro").mode("overwrite").option("path", "C:\\Users\\sathish\\BIG DATA\\week 12\\write_avro").save()