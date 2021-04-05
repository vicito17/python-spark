from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
def normalizeWords(text):
    normalizada= re.compile(r'\W+', re.UNICODE).split(text.lower())
    return normalizada


input = sc.textFile("data/book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x + y)
wordCountsSorted = wordCounts.map(lambda x : (x[1],x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    word = result[1].encode('ascii', 'ignore')
    count = str(result[0])
    if (word):
        print(word.decode() + " " + count)
