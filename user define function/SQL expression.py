#Creating our own user defined function is spark.

#SQL expression -- the function will be registered in catalog.
#So in this case we can even use it with spark SQL.

#problem statment
# if the age is greater than 18 we have to populate the 4th column named Adult with "Y"
# else we need to populated the column with "N"

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, StringType, expr

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

spark.udf.register("parseAgeFunction", ageCheck, StringType())

for x in spark.catalog.listFunctions():
    print(x)

df2 = df1.withColumn("adult", expr("parseAgeFunction(Age)"))

df2.show()