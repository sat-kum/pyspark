from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import  regexp_extract

my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf= my_conf).getOrCreate()

myregex = r'^(\S+) (\S+)\t(\S+)\,(\S+)'

linedf=spark.read.text("C:\\Users\\sathish\\BIG DATA\\week 11\\orders_new-201019-002101.csv")

#linedf.printSchema()
#linedf.show()


final_df =linedf.select(regexp_extract('value',myregex,1).alias("order_id"),regexp_extract('value',myregex,2).alias("date"),regexp_extract('value',myregex,3).alias("customer_id"),regexp_extract('value'
,myregex,4).alias("status"))
final_df.printSchema()
final_df.show()

final_df.select("order_id").show()

final_df.groupby("status").count().show()