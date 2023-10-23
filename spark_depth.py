from pyspark import SparkContext

def loadBoringWords():
    boring_words= set(line.strip() for line in open ("C:\\Users\\sathish\\BIG DATA\\week 10\\Boringwords.txt"))
    return boring_words

sc=SparkContext("local[*]", "spark_depth")
name_set=sc.broadcast(loadBoringWords())

initial_rdd= sc.textFile("C:\\Users\\sathish\\BIG DATA\\week 10\\bigdatacampaigndata-201014-183159.csv")

mapped_input =initial_rdd.map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))

words=mapped_input.flatMapValues(lambda x: x.split(" "))

final_mapped= words.map(lambda x: (x[1].lower(), x[0]))

filtered_rdd=final_mapped.filter(lambda x: x[0] not in name_set.value)

total= filtered_rdd.reduceByKey(lambda x,y: x+y)

sorted = total.sortBy(lambda x: x[1], False)

result = sorted.take(20)
for x in result:
    print(x)