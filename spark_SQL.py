from pyspark import SparkConf
from pyspark.sql import SparkSession

my_con=SparkConf()
my_con.set("spark.app.name", "my first application")
my_con.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=my_con).getOrCreate()

orderdf=spark.read.format("csv").option("header", True).option("inferSchema", True).option("path", "C:\\Users\\sathish\\BIG DATA\\week 12\\orders-201025-223502.csv").load()

orderdf.createOrReplaceTempView("orders")

resultdf=spark.sql("select order_status, count(*) as total_order from orders group by order_status")

resultdf.show()

resultdf_where = spark.sql("select order_customer_id, count(*) as total_orders from orders where order_status='CLOSED' group by order_customer_id order by total_orders desc")

resultdf_where.show()