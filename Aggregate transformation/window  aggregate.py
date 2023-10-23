#// Window aggregates


from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

my_conf = SparkConf()

my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
invoiceDF = spark.read.format("csv").option("header",True).option("inferSchema",True).option("path","C:\\Users\\sathish\\BIG DATA\\week 12\\windowdata-201025-223502.csv").load()

myWindow = Window.partitionBy("country")\
.orderBy("weeknum")\
.rowsBetween(Window.unboundedPreceding, Window.currentRow)
mydf = invoiceDF.withColumn("RunningTotal",sum("invoicevalue").over(myWindow))
mydf.show()