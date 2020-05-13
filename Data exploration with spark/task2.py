import pyspark
from pyspark import SparkContext, SparkConf
import sys
import json


if(sys.argv[4] =="spark"):
    conf = SparkConf().set("spark.ui.port", "4050")
    sc = pyspark.SparkContext(conf=conf)

    rdd=sc.textFile(sys.argv[1])
    rdd1=sc.textFile(sys.argv[2])

    js=rdd.map(lambda x : json.loads(x))
    js1=rdd1.map(lambda x: json.loads(x))

    r1=js1.map(lambda x:(x['business_id'],x['categories'])).filter(lambda x: x[1] is not None).map(lambda x :(x[0],x[1].split(","))).flatMapValues(lambda x : x).map(lambda x: (x[0],x[1].strip()))

    r2=js.map(lambda x:(x['business_id'],x['stars']))
    r3=r1.join(r2)
    rdd_aggregation=r3.map(lambda x: x[1]).aggregateByKey((0,0), lambda u,v: (u[0] + v,u[1] + 1),lambda u,v: (u[0] + v[0], u[1] + v[1]))
    n=sys.argv[5]
    res=rdd_aggregation.mapValues(lambda x: x[0]/x[1]).map(lambda x: (str(x[0]),float(x[1]))).takeOrdered(int(n), key = lambda x: (-x[1],x[0]))
  
    d={"result":res}
    with open(sys.argv[3], "w") as outf:
        outf.write(json.dumps(d))
        
elif(sys.argv[4]=="no_spark"):
    r1 = []
    for line in open(sys.argv[1], 'r'):
        r1.append(json.loads(line))
    r2=[]
    for line in open(sys.argv[2], 'r'):
        r2.append(json.loads(line)) 
    lst=[]
    for i in range(0,len(r2)):
      if(r2[i]['categories'] is not None):
        lst.append((r2[i]['business_id'],r2[i]['categories'].split(",")))
    
    flatten={}
    for i in range(0,len(lst)):
      for j in range(1,len(lst[i])):
        for k in range(0,len(lst[i][j])):
          lst[i][j][k]=lst[i][j][k].strip()
          flatten.setdefault(lst[i][0], []).append(lst[i][j][k])
            
            
    lst_rv={}
    for i in range(0,len(r1)):
        lst_rv.setdefault(r1[i]['business_id'], []).append(r1[i]['stars'])        
     
    joined = {}
    for i in (flatten.keys() and lst_rv.keys()):
        if i in flatten and i in lst_rv: 
          for j in flatten[i]:
            for k in lst_rv[i]:
              joined.setdefault(j, []).append(k)
    
    dic1={}
    for k,v in joined.items():
        dic1[k] = sum(v)/len(v)
    
    res=sorted(dic1.items(), key=lambda x: (-x[1],x[0]))
    
    n=int(sys.argv[5])
    final_res=res[0:n]
    
    d={"result":final_res}
    with open(sys.argv[3], "w") as outf:
        outf.write(json.dumps(d))
    
