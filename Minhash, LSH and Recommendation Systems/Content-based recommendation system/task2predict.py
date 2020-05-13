from pyspark import SparkContext, SparkConf
import pyspark
from itertools import combinations
import math
import operator
import sys
import json

conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('master', 'local'), ('appName', 'Vignesh_hw3_task2_preds')])
sc = pyspark.SparkContext(conf=conf)

rdd_test=sc.textFile(sys.argv[1])
js_test = rdd_test.map(lambda x : json.loads(x))
test_set = js_test.map(lambda x : (x['user_id'],x['business_id']))

with open(sys.argv[2]) as fp:
    s1 = json.load(fp)

def cosine(uid,bid):
    t1= s1['user_profiles'][uid]
    t2= s1['business_profiles'].get(bid,[1])
    sumxx, sumxy, sumyy = 0, 0, 0
    for i in range(len(t1)):
     x = t1[i] 
     sumxx += 1*1
     sumyy += 1*1
     if(x in t2):
        sumxy += 1*1
     else:
        sumxy += 0
        
    return sumxy/math.sqrt(sumxx*sumyy)
    


#t=test_set.map(lambda x : (x[0],x[1])).take(10)    
#print(t)
preds = test_set.map(lambda x : (x[0],x[1],cosine(x[0],x[1]))).filter(lambda x : x[2] > 0.01)
content=preds.map(lambda x : {"user_id": x[0], "business_id":x[1],"sim": x[2]}).collect()

with open(sys.argv[3],'w') as fw:
    for i in content:
        json.dump(i,fw)
        fw.write('\n')
