import pyspark
from pyspark import SparkContext, SparkConf
import json
import csv
conf = SparkConf().set("spark.ui.port", "4050")
sc = pyspark.SparkContext(conf=conf)

rdd1=sc.textFile('review.json')
rdd2=sc.textFile('business.json')


js=rdd1.map(lambda x : json.loads(x))
js1=rdd2.map(lambda x: json.loads(x))

nevada_RDD=js1.filter(lambda x : x['state']=="NV").map(lambda x : (x['business_id'],x['state']))
review_RDD=js.map(lambda x : (x['business_id'],x['user_id']))

r3=nevada_RDD.join(review_RDD)
req_fields=r3.map(lambda x : (x[1][1],x[0]))


lst1=req_fields.map(lambda x : x[0]).collect()
lst2=req_fields.map(lambda x : x[1]).collect()


header=["user_id","business_id"]
with open('user_business.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(zip(lst1,lst2))
