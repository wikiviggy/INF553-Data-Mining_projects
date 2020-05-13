import pyspark
from pyspark import SparkContext, SparkConf
import json
import sys

if(sys.argv[3]=="default"):
    conf = SparkConf().set("spark.ui.port", "4050")
    sc = pyspark.SparkContext(conf=conf)

    rdd=sc.textFile(sys.argv[1])
    js=rdd.map(lambda x : json.loads(x))

    n=int(sys.argv[5])
    resrdd=js.map(lambda x : (x['business_id'],1)).reduceByKey(lambda u,v : u+v).filter(lambda x: x[1] > n)
    result=resrdd.collect()
    n_items=resrdd.mapPartitions(lambda x: [sum(1 for _ in x)]).collect()
    n_partitions=resrdd.getNumPartitions()
    d={"n_partitions":n_partitions,"n_items":n_items,"result":result}

    with open(sys.argv[2], "w") as outf:
        outf.write(json.dumps(d))
        
elif(sys.argv[3]=="customized"):
    
    conf = SparkConf().set("spark.ui.port", "4050")
    sc = pyspark.SparkContext(conf=conf)

    rdd=sc.textFile(sys.argv[1])
    js=rdd.map(lambda x : json.loads(x))

    n=int(sys.argv[5])
    n_partitions=int(sys.argv[4])
    resrdd=js.map(lambda x : (x['business_id'],1)).coalesce(n_partitions).persist()
    resrdd1=resrdd.reduceByKey(lambda u,v : u+v).filter(lambda x: x[1] > n)
    
    result=resrdd1.collect()
    n_items=resrdd1.mapPartitions(lambda x: [sum(1 for _ in x)]).collect()

    d={"n_partitions":n_partitions,"n_items":n_items,"result":result}

    with open(sys.argv[2], "w") as outf:
        outf.write(json.dumps(d))
    
