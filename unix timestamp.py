"""
create the spark session
create a local list
create a dataframe from this local list and give column names
add a new column date1 with unix timestamp
add one more column with monotonically increasing id
drop the duplicates based on combination of 2 columns
drop the orderid column
sort based on order date

"""

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf= SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=my_conf).getOrCreate()

mylist=[(1,"2013-07-25",11599,"CLOSED"),
(2,"2014-07-25",256,"PENDING_PAYMENT"),
(3,"2013-07-25",11599,"COMPLETE"),
(4,"2019-07-25",8827,"CLOSED")]

listdf=spark.createDataFrame(mylist).toDF('orderid', 'orderdate', 'customerid', 'status')

newdf=listdf.withColumn('date1', unix_timestamp(col('orderdate'), 'yyyy-mm-dd')).withColumn('newid',monotonically_increasing_id()).drop_duplicates(['orderdate', 'customerid']).drop('orderid').sort('orderdate')

newdf.printSchema()

newdf.show()