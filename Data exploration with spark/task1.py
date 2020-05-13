import os
import sys

import pyspark
from pyspark import SparkContext, SparkConf
import json

conf = SparkConf().set("spark.ui.port", "4050")

sc = pyspark.SparkContext(conf=conf)
rdd=sc.textFile(sys.argv[1])


js=rdd.map(lambda x : json.loads(x))

qa=js.count()

y=str(sys.argv[4])
qb=js.filter(lambda x : x['date'].find(y) != -1 ).count()

qc=js.groupBy(lambda x : x['user_id']).distinct().count()

m=int(sys.argv[5])
qd=js.map(lambda x:(x['user_id'],1)).reduceByKey(lambda x,u : x+u).sortBy(lambda x: (-x[1],x[0])).take(m)

def transformed(x):
  chars='([,.!?:;])'
  lc = x.lower()
  for ch in chars:
    lc = lc.replace(ch, '')
  return lc

n=int(sys.argv[6])
with open(sys.argv[3], 'r') as file:
    stopwords = file.read()

qe=js.flatMap(lambda x: x['text'].split()).map(transformed).filter(lambda x : x not in stopwords).map(lambda x:(x,1)).reduceByKey(lambda x,u : x+u).sortBy(lambda x: (-x[1],x[0])).keys().take(n)

d={"A":qa,"B":qb,"C":qc,"D":qd,"E":qe}


with open(sys.argv[2], "w") as outf:
    outf.write(json.dumps(d))
