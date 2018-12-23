from pyspark import SparkConf,SparkContext
from operator import add
import string
import nltk
from nltk.corpus import stopwords
import re

conf=SparkConf()
conf.setAppName("Inverted Index")
conf.set("spark.executor.memory","2g")
conf.set("spark.ui.port","4097")
sc=SparkContext(conf=conf)

path="/cosc6339_hw2/gutenberg-500/"

#popular words
text=sc.textFile(path)
words = text.flatMap(lambda line:line.lower().split())
word = words.map(lambda x: re.sub('\W+','',x))
stops = set(stopwords.words('english'))
wordt = word.map(lambda x: ''.join([w1 for w1 in x.split() if w1 not in (stops)]))
wcounts= wordt.map(lambda w: (w, 1) )
counts = wcounts.reduceByKey(add, numPartitions=1)
count1 = counts.map(lambda (a,b) : (b,a))
count2 = count1.sortByKey(False)
count = count2.map(lambda (a,b) : (b,a))
count3 = count.take(1000)
count4 = sc.parallelize(count3,1)
removePunct=(lambda x:x not in string.punctuation)
finalWords=[]
out=count4.collect()
for(count,word) in out:
        out1 = count
        finalWords.append(out1)
		
#inverted index
rdd=sc.wholeTextFiles(path)
inverted1=rdd.map(lambda(x,y):(y,x))
inverted2=inverted1.map(lambda (x,y):(filter(removePunct,x),y))
def checkWords(c):
        if c in finalWords:
                return True
        else:
                return False
inverted3=inverted2.flatMap(lambda (x,y):(((i,y),float(1.0/(float(len(x.split()))))) for i in x.lower().split() if check
Words(i)))
inverted4=inverted3.reduceByKey(add,numPartitions=1)
inverted5=inverted4.map(lambda ((x,y),z):(x,(y,z)))
inverted6=inverted5.groupByKey()
inverted7=inverted6.mapValues(list)
inverted7.saveAsTextFile("/bigd45/output97")