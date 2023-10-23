from pyspark import SparkContext

def blankLineChecker(line):
    if (len(line)==0):
        myaccum.add(1)

sc = SparkContext("local[*]","Accumulator")

myrdd= sc.textFile("C:\\Users\\sathish\\BIG DATA\\week 10\\simplefile.txt")

myaccum= sc.accumulator(0.0)

myrdd.foreach(blankLineChecker)

print(myaccum.value)