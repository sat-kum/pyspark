#// Grouping aggregates


from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()

my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
invoiceDF = spark.read.format("csv").option("header",True).option("inferSchema",True).option("path","C:\\Users\\sathish\\BIG DATA\\week 12\\order_data-201025-223502.csv").load()

#column object expression
summaryDF = invoiceDF.groupBy("Country","InvoiceNo").agg(sum("Quantity").alias("TotalQuantity"),sum(expr("Quantity * UnitPrice")).alias("InvoiceValue"))

summaryDF.show()

#string expression
summaryDf1 = invoiceDF.groupBy("Country","InvoiceNo").agg(expr("sum(Quantity) as TotalQunatity"),expr("sum(Quantity * UnitPrice) as InvoiceValue"))

summaryDf1.show()

#spark SQL
invoiceDF.createOrReplaceTempView("sales")
spark.sql("""select country,InvoiceNo,sum(Quantity) as totQty,sum(Quantity * UnitPrice) as
InvoiceValue from sales group by country,InvoiceNo""").show()
