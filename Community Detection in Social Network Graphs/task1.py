from pyspark import SparkContext, SparkConf
import pyspark
from graphframes import *
import sys
from pyspark.sql import SQLContext
import itertools
from collections import OrderedDict
from pyspark.sql import Row

conf = SparkConf().set("spark.ui.port", "4050").setMaster("local[3]")
sc = pyspark.SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

rdd = sc.textFile(sys.argv[2])
data = rdd.map(lambda x: x.split(','))
header = data.first()

df = data.filter(lambda x: x != header)

users = df.groupByKey().map(lambda x: (x[0], set(x[1])))
userslst = users.collect() 
users_dict = users.collectAsMap()
threshold = int(sys.argv[1])
def edges(x):
  nodes = set() 
  edge_lst = list() 
  ub = users_dict[x[0]] 
  i = userslst.index((x[0], ub)) 
  for k in range(i+1, len(users_dict)): 
    corated_business = ub.intersection(userslst[k][1]) 
    if(len(corated_business) >= threshold):
  
      nodes.add(x[0]) 
      nodes.add(userslst[k][0])
      edge_lst.append((x[0], userslst[k][0]))
      edge_lst.append((userslst[k][0], x[0]))
  
  return (edge_lst, nodes)


edgelist = users.map(edges)

vertices = edgelist.flatMap(lambda x: x[1]).distinct()
edges = edgelist.flatMap(lambda x: x[0]).collect()

v1 = vertices.map(lambda x: Row(id = x))
v = sqlContext.createDataFrame(v1)
e = sqlContext.createDataFrame(edges, ["src", "dst"])
#vertices = edges.select('src').distinct()
#vertices = vertices.withColumnRenamed('src', 'id')

g = GraphFrame(v,e)

communities = g.labelPropagation(maxIter=5)
res=communities.collect()
t1=sc.parallelize(res)
result=t1.map(lambda x : (str(x[1]),x[0]) ).groupByKey().mapValues(set).mapValues(list).collectAsMap()

for i in result:
    result[i]=sorted(result[i])

ordered_d = OrderedDict(sorted(result.items(), key=lambda x: (len(x[1]),x[1][0])))
            
last = next(reversed(ordered_d.keys()))

with open(sys.argv[3],'w') as fw:
    for i in ordered_d:
        s=""
        if(len(ordered_d[i])== 1):
            for j in ordered_d[i]:
                s=s+"'"+j+"'"
             
        else:
            for j in ordered_d[i]:
                s=s+"'"+j+"', "
        
        s=s.rstrip(', ')
        fw.write(s)
        if(i!=last):
            fw.write("\n")        
