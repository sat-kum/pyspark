from pyspark import SparkContext

sc = SparkContext("local[*]", "wordcount")
sc.setLogLevel("ERROR")

input =sc.textFile("C:\\Users\\sathish\\Documents\\search_data.txt")

words= input.flatMap(lambda  x:x.split(" "))

# word_counts= words.map(lambda x: (x,1))
#
# final_count= word_counts.reduceByKey(lambda x, y: x+y)
#
# result = final_count.collect()
#
# for a in result:
#     print(a)


# improve the code to count as lower case and sort
word_counts=words.map(lambda x: (x.lower(),1))
#
# final_count=word_counts.countByValue()
#
# print(final_count)

#ReduceByKey in sortByKey

final_count=word_counts.reduceByKey(lambda x, y: x+y).map(lambda x:(x[1], x[0]))

result= final_count.sortByKey(False).map(lambda x: (x[1], x[0])).collect()

for i in result:
    print(i)