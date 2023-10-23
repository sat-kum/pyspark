from pyspark import SparkConf
from pyspark.sql import SparkSession

my_con = SparkConf()
my_con.set("spark.app.name", "conf read file")
my_con.set("spark.master", "local[*]")

spark=SparkSession.builder.config(conf=my_con).getOrCreate()

order_df=spark.read.option('header', True).option('inferSchema', True).csv("C:\\Users\\sathish\\BIG DATA\\week 11\\orders-201019-002101.csv")

group_df=order_df.repartition(4).where('order_customer_id > 10000').select('order_id', 'order_customer_id').groupBy('order_customer_id').count()

group_df.show()