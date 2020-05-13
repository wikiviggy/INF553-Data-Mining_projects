from pyspark import SparkContext
from pyspark import SparkConf
import time
from operator import add
import os
import json
import sys
import pyspark
import random
import math 
import operator

Conf = pyspark.SparkConf().setAppName('hw 5').setMaster('local[*]')

input_path = sys.argv[1]
sc = pyspark.SparkContext(conf = Conf)
sc.setLogLevel("ERROR")

n_cluster = int(sys.argv[2])
intermediate_output = sys.argv[4]
output_cluster = sys.argv[3]

t1=time.time()

file_lst = list()
f1 = input_path + "/"
counter = 0
for f in os.listdir(input_path):
  if(counter == 0):
    f1 += f
  else:
    file_lst.append(input_path + "/" + f)
  counter +=  1

def adder(x, y):
  
  a = x
  b = y  
  lst = [element1 + element2 for element1, element2 in zip(a, b)]
  return lst


def change_dist(new, old):
  distance = 0
  for (cID, pt) in new:
    distance += sum([math.pow((operator.sub(oldC, newP)),2) for oldC, newP in zip(old[cID], pt)])
  
  return distance

#citation : Prof. Wensheng Wu's slides for computing K-means in spark 

def euclidian_dist(pt, all_centers):
  closest_center = (0, float('+inf'))

  for cID, cPt in all_centers.items():
    sum = 0
  
    for i in range(dimensions):
      sum += math.pow((operator.sub(pt[1][i] , cPt[i])),2)
    
    eucDist = sum**0.5

    cc1 = closest_center[1]
    if(eucDist < cc1):
      closest_center = (cID, eucDist)
  
  cc = closest_center[0]
  return cc

load1 = open(f1, "r")
all_lines = [line.strip() for line in load1.readlines()]
lines = list()
for l1 in all_lines:
  columns = l1.split(',')
  line = list()
  for c1 in columns:
    c1=float(c1)
    line.append(c1)
  lines.append(line)

length = lines[0][1:]
dimensions = len(length)

perc = 0.2
fraction = math.ceil(float(len(lines))*perc)

sample = lines[:fraction]
random.seed(a=0) 
initialCenters = random.sample(sample, n_cluster) 

centers_DS = dict() 
count = 0
for center in initialCenters: 
  centers_DS[count] = center[1:]
  count += 1

initial_sample = sc.parallelize(sample)
initial_sample = initial_sample.map(lambda x: (str(int(x[0])), x[1:])).persist()
DScount = 0
tempDist = 1.0

while(tempDist > 0.0001):
  closestCluster_DS = initial_sample.map(lambda x: (euclidian_dist(x, centers_DS), x)).persist() 

  datapoint_cluster_DS = closestCluster_DS.map(lambda x: (str(int(x[1][0])), x[0])) 
  centersPoints_DS = closestCluster_DS.map(lambda x: (x[0], (1, x[1][1], [i ** 2 for i in x[1][1]]))).persist()
  centersPoints_DS = centersPoints_DS.reduceByKey(lambda x, y: (x[0] + y[0], adder(x[1], y[1]), adder(x[2], y[2]))) 

  newCenters_DS = centersPoints_DS.mapValues(lambda x: [operator.truediv(val,x[0]) for val in x[1]]).persist()
  newCenters_DS = newCenters_DS.collect()
  tempDist = change_dist(newCenters_DS, centers_DS)

  for (i, p) in newCenters_DS:
    centers_DS[i] = p
  DScount += 1
  if(DScount == 100):
    break

dsOutput = datapoint_cluster_DS
dsOutput = dsOutput.collectAsMap()
summaryStats_DS = centersPoints_DS
summaryStats_DS = summaryStats_DS.collectAsMap()

remainingSample = lines[fraction:]
random.seed(a=5)
initial_csCenters = random.sample(remainingSample, operator.mul(2,n_cluster)) 

centers_CS = dict() 
clusterIndex_CS = n_cluster 
for csCenter in initial_csCenters: 
  centers_CS[clusterIndex_CS] = csCenter[1:]
  clusterIndex_CS += 1

remaining_sample = sc.parallelize(remainingSample)
remaining_sample = remaining_sample.map(lambda x: (str(int(x[0])), x[1:])).persist()
tempDist_CS = 1.0
CScount = 0


while(tempDist_CS > 0.0001):
 
  closestCluster_CS = remaining_sample.map(lambda x: (euclidian_dist(x, centers_CS), x)).persist() 

  datapoint_cluster_CS = closestCluster_CS.map(lambda x: (str(int(x[1][0])), x[0])).persist()

  cluster_datapoint_CS = closestCluster_CS.map(lambda x: (x[0], [x[1][0]])).persist()
  cluster_datapoint_CS = cluster_datapoint_CS.reduceByKey(lambda a,b: a+b)  

  centersPoints_CS = closestCluster_CS.map(lambda x: (x[0], (1, x[1][1], [i ** 2 for i in x[1][1]]))).reduceByKey(lambda x, y: (x[0] + y[0], adder(x[1], y[1]), adder(x[2], y[2]))) 
  
 
  newCenters_CS = centersPoints_CS.mapValues(lambda x: [operator.truediv(val,x[0]) for val in x[1]]).persist()
  newCenters_CS = newCenters_CS.collect() 

 
  tempDist_CS = change_dist(newCenters_CS, centers_CS)

  closestCluster_CS.unpersist()
  for (i, p) in newCenters_CS: 
    centers_CS[i] = p

  CScount +=  1
  if(CScount == 100):
    break


summaryStats_CS = centersPoints_CS.collectAsMap()
CSdict = datapoint_cluster_CS.collectAsMap()
rsOutput = dict()
cluster_point_dict = cluster_datapoint_CS.collectAsMap()
for key in centers_CS:
  if(len(centers_CS[key]) == 1):
    rsOutput[centers_CS[key]] = key
    del centers_CS[key]
    del cluster_point_dict[key]
    del CSdict[centers_CS[key]]
    del summaryStats_CS[key]

csOutput = CSdict

output = "round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained\n"
intermed_output= output + "1," + str(n_cluster) + "," + str(len(dsOutput)) + "," + str(len(centers_CS)) + "," + str(len(csOutput)) + "," + str(len(rsOutput)) + "\n"
    
def reassign_CS_clusters(x):
  for pt in cluster_point_dict[x[0]]:
    yield (pt, x[1])


def update_stats(new, old):
  for cid, stats in old.items():
    if(cid in new.keys()):
      N = stats[0] + new[cid][0] 
      SUM = adder(new[cid][1], stats[1]) 
      SUMSQ = adder(new[cid][2], stats[2]) 
      old[cid] = (N, SUM, SUMSQ)      
      
  return old

def mahalanobis(x, center_stats, center_coordinates):
  closestCenter = (0, float('+inf'))
  for cid, cpt in center_coordinates.items():
    N = center_stats[cid][0]
    SUM = center_stats[cid][1]
    SUMSQ = center_stats[cid][2]
    
    
    sum = 0
    for i in range(0,dimensions,1):
      diff = operator.sub(x[1][i] , cpt[i])
      var = operator.sub(float(SUMSQ[i]/N) , float(SUM[i]/N)**2)
      std = math.sqrt(var)
      if(std != 0):
        divide = float(operator.truediv(diff,std))
        sum += divide**2
      else:
        sum += diff**2
    
    md = math.sqrt(sum)

    
    if(md < closestCenter[1]):
      closestCenter = (cid, md)
  
  if(closestCenter[1] < float(3*math.sqrt(dimensions))):
    return closestCenter[0] 
  else:
    return -1

round_num = 1
for eachFile in range(counter-1):
  round_num += 1
  next_load = open(file_lst[eachFile], "r")
  all_lines = [line.strip() for line in next_load.readlines()]
  lines = list()
  for l1 in all_lines:
    c = l1.split(',')
    line = list()
    for c1 in c:
      line.append(float(c1))
    lines.append(line)

  length = lines[0][1:]
  dimensions = len(length) 
  entire_load = sc.parallelize(lines).map(lambda x: (str(int(x[0])), x[1:])).persist()

  closest_DS = entire_load.map(lambda x: (mahalanobis(x,summaryStats_DS, centers_DS), x)).persist() 
  new_DS = closest_DS.filter(lambda x: x[0] != -1).persist() 
  
  add_DS = new_DS.map(lambda x: (str(int(x[1][0])), x[0])).persist() 

  temp_DS_Output = add_DS.collectAsMap()
  for k, v in temp_DS_Output.items():
    dsOutput[k] = v
  
  addStats_DS = new_DS.map(lambda x: (x[0], (1, x[1][1], [i ** 2 for i in x[1][1]]))).persist()
  addStats_DS = addStats_DS.reduceByKey(lambda x, y: (x[0] + y[0], adder(x[1], y[1]), adder(x[2], y[2]))).collectAsMap()

  summaryStats_DS = update_stats(addStats_DS, summaryStats_DS)

  test_CS = closest_DS.filter(lambda x: x[0] == -1).persist() 
  CS_assign = test_CS.map(lambda x: x[1]).map(lambda x: (mahalanobis(x,summaryStats_CS, centers_CS), x)).persist() 
  new_CS = CS_assign.filter(lambda x: x[0] != -1).persist() 

  add_CS = new_CS.map(lambda x: (str(int(x[1][0])), x[0])).persist()
  add_CS = add_CS.collectAsMap() 
  temp_CS_Output = new_CS.map(lambda x: (x[0], [x[1][0]])).reduceByKey(lambda x,y: x+y)
  temp_CS_Output = temp_CS_Output.collectAsMap() 

  csOutput.update(add_CS)
  for k, v in temp_CS_Output.items():
    cluster_point_dict.extend(v)

  addStats_CS = new_CS.map(lambda x: (x[0], (1, x[1][1], [i ** 2 for i in x[1][1]]))).reduceByKey(lambda a, b: (a[0] + b[0], adder(a[1], b[1]), adder(a[2], b[2]))).collectAsMap() 

  summaryStats_CS = update_stats(addStats_CS,summaryStats_CS)

  RS = CS_assign.filter(lambda x: x[0] == -1).map(lambda x: x[1]).persist()
  RS_datapoints = RS.collect()
  rs_dic = RS.collectAsMap()
  rsOutput.update(rs_dic)
  RS_length = len(RS_datapoints)

  if(RS_length > 0):
    random.seed (a=5)
    if(RS_length >= 2*n_cluster):
      initial_CS_centers = random.sample(RSpoints, 2*n_cluster) 
    else:
      initial_CS_centers = RS_datapoints 

    temp_centers_CS_clust = dict() 
    for csCenter in initial_CS_centers: 
      temp_centers_CS_clust[clusterIndex_CS] = csCenter[1]
      clusterIndex_CS += 1
    tempDist_CS = 1.0
    count_CS = 0
    
    
    while(tempDist_CS > 0.0001):
     
      closest_Cluster_CS = RS.map(lambda x: (euclidian_dist(x, temp_centers_CS_clust), x)).persist()      
      datapoint_cluster_CS = closest_Cluster_CS.map(lambda x: (str(int(x[1][0])), x[0])).persist()
     
      cluster_datapoint_CS = closest_Cluster_CS.map(lambda x: (x[0], [x[1][0]])).persist()
      cluster_datapoint_CS = cluster_datapoint_CS.reduceByKey(lambda a,b: a+b)
        
      centersPoints_CS = closest_Cluster_CS.map(lambda x: (x[0], (1, x[1][1], [i ** 2 for i in x[1][1]]))).persist()
      centersPoints_CS  = centersPoints_CS.reduceByKey(lambda x, y: (x[0] + y[0], adder(x[1], y[1]), adder(x[2], y[2])))
           
      newCenters_CS = centersPoints_CS.mapValues(lambda x: [value/x[0] for value in x[1]]).persist()
      newCenters_CS = newCenters_CS.collect()      
      tempDist_CS = change_dist(newCenters_CS, temp_centers_CS_clust)

      for (i, p) in newCenters_CS: 
        temp_centers_CS_clust[i] = p

      count_CS += 1
      if(count_CS == 100):
        break

    temp_CS_Output = cluster_datapoint_CS.collectAsMap()
    new_CSpoints = datapoint_cluster_CS.collectAsMap()
    temp_CS_stats_summary = centersPoints_CS.collectAsMap()
    
    delete_lst = list()
    for k in temp_CS_Output:
      if(len(temp_CS_Output[k]) == 1):
        rsOutput[temp_CS_Output[k][0]] = k
        del temp_centers_CS_clust[k]
        delete_lst.append(k)
        del temp_CS_stats_summary[k]
        del new_CSpoints[temp_CS_Output[k][0]]
      else:
        del rsOutput[temp_CS_Output[k][0]]
    
    for k in delete_lst:
      del temp_CS_Output[k]

    centers_CS.update(temp_centers_CS_clust)
    csOutput.update(new_CSpoints)

    for k in temp_CS_Output:
      cluster_points[k].extend(temp_CS_Output[k])

    summaryStats_CS.update(temp_CS_stats_summary)

  
  if(eachFile < counter-2):
    output = str(round_num) + "," + str(n_cluster) + "," + str(len(dsOutput)) + "," + str(len(centers_CS)) + "," + str(len(csOutput)) + "," + str(len(rsOutput)) + "\n"
    intermed_output += output

csCentersList = list()
for c, pt in centers_CS.items():
  csCentersList.append((c, pt))

all_CS = sc.parallelize(csCentersList)
all_CS = all_CS.map(lambda x: (x[0], mahalanobis(x, summaryStats_DS, centers_DS))).persist()

temp_CS = all_CS.filter(lambda x: x[1] != -1).persist()
remove_CS = temp_CS.collectAsMap()
for k, v in remove_CS.items():
  del centers_CS[k]

move_CS = temp_CS.flatMap(lambda x: reassign_CS_clusters(x))
move_CS = move_CS.collectAsMap() 

for k, v in move_CS.items():
  del csOutput[k]
  dsOutput[k] = v

centerID_clust = all_CS.filter(lambda x: x[1] == -1)
centerID_clust = centerID_clust.collectAsMap() 
last_RS = all_CS.filter(lambda x: x[1] == -1).flatMap(lambda x: reassign_CS_clusters(x)).persist()
last_RS = last_RS.collectAsMap() 

for key in last_RS:
  rsOutput[key] = last_RS[key]
  del csOutput[key]

for k in centerID_clust.keys():
  del centers_CS[k]

output = str(round_num) + "," + str(n_cluster) + "," + str(len(dsOutput)) + "," + str(len(centers_CS)) + "," + str(len(csOutput)) + "," + str(len(rsOutput))
intermed_output += output
dsOutput.update(rsOutput)

dsOutput = dict(sorted(dsOutput.items(), key=lambda x: int(x[0])))
cluster_assignment = dsOutput

with open(intermediate_output, 'w') as f:
    f.write(intermed_output)

with open(output_cluster, 'w') as g:
	g.write(json.dumps(cluster_assignment))

t2 = time.time()

print(t2-t1)

