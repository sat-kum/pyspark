# Creating our own user defined function is spark.
# 1. Column object expression -- the function won't be registered in catalog

#problem statment
# if the age is greater than 18 we have to populate the 4th column named Adult with "Y"
# else we need to populated the column with "N"

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, StringType
my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
df = spark.read.format("csv")\
.option("inferSchema",True)\
.option("path","C:\\Users\\sathish\\BIG DATA\\week 12\\201025-223502.dataset1")\
.load()

df1=df.toDF("Name", "Age", "City")

def ageCheck(age):
    if (age>18):
        return "Y"
    else:
        return "N"

parseAgeFunction= udf(ageCheck, StringType())

df2=df1.withColumn("adult", parseAgeFunction('Age'))

df2.printSchema()
df2.show()