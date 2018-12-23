from pyspark import SparkConf, SparkContext
from operator import add
import re
from nltk.corpus import stopwords

conf = SparkConf()
conf.setAppName("Wordcount")
conf.set("spark.ui.port", "4096")
conf.set("spark.executor.memory", "2g")
sc= SparkContext(conf = conf)

text=sc.textFile("/cosc6339_hw2/large-dataset/")
words = text.flatMap(lambda line:line.lower().split())
word = words.map(lambda x: re.sub('\W+','',x))
stops = set(stopwords.words('english'))
wordt = word.map(lambda x: ''.join([w1 for w1 in x.split() if w1 not in (stops)]))
wcounts= wordt.map(lambda w: (w, 1) )
counts = wcounts.reduceByKey(add, numPartitions=1)
count1 = counts.map(lambda (a,b) : (b,a))
count2 = count1.sortByKey(False)
count = count2.map(lambda (a,b) : (b,a))
count3 = count.map(lambda (b,a): b)
count4 = count3.take(1000)
count5 = sc.parallelize(count4,1)
count5.saveAsTextFile("/bigd45/output27")