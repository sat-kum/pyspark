#//simple aggregates


from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()

my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
invoiceDF = spark.read.format("csv").option("header",True).option("inferSchema",True).option("path","C:\\Users\\sathish\\BIG DATA\\week 12\\order_data-201025-223502.csv").load()

# column object expression
invoiceDF.select(
count("*").alias("RowCount"),
sum("Quantity").alias("TotalQuantity"),
avg("UnitPrice").alias("AvgPrice"),
countDistinct("InvoiceNo").alias("CountDistinct")).show()

#column string expression.
invoiceDF.selectExpr('count(*) as RowCount', 'sum(Quantity) as TotalQuantity', 'avg(UnitPrice) as AvgPrice', 'count(Distinct(InvoiceNo)) as CountDistinct').show()


#SparkSQL
invoiceDF.createOrReplaceTempView('sales')
spark.sql("select count(*), sum(Quantity), avg(UnitPrice), count(distinct(InvoiceNo)) from sales").show()