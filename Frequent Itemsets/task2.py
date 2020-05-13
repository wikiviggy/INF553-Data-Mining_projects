from pyspark import SparkContext, SparkConf
import pyspark
from itertools import combinations
import math
import sys
import time

conf = SparkConf().set("spark.ui.port", "4050")
sc = pyspark.SparkContext(conf=conf)
start_time = time.time()
k1=int(sys.argv[1])
def ph1(lst,baskets,k):
  freq_singles= sorted(set(sum(lst, ()))) if k>2 else lst
  counter =dict()
  for b in baskets:
    b_singles=list()
    for i in freq_singles:
      if i in b:
        b_singles.append(i)
    comb = combinations(b_singles,k)
    for c in comb:
        prev_count_val=counter[c] if c in counter else 0 
        counter[c] = prev_count_val + 1 
       
  returned_freqs=list()
  for i in counter:
    if counter[i] >= subset_support:
      returned_freqs.append(i)
  
  return returned_freqs

def singleton_gen(bk):
  emp=dict()
  baskets=bk
  for i in baskets:
    for j in i:
      if j not in emp:
        emp[j]= 1

      else :
        emp[j]=emp[j] + 1 
       
  phase1_keys=list()
  for i in emp:
    if emp[i] >= subset_support :
      phase1_keys.append(i)
  candidate_itemset=list()
  items=list()
  for i in phase1_keys:
    candidate_itemset.append((i,1))

  for i in phase1_keys:
    items.append(i) 

  return items     

def get_freq(val):
  sample= list(val)
  freq=list()

  singleton = singleton_gen(sample)
  if not singleton:
    return freq

  freq_singletons = sorted(singleton)
  k=2
  while freq_singletons:
    freq.extend(freq_singletons)
    combos = ph1(freq_singletons,sample,k)
    k= k+1
    freq_singletons = combos
  
  return freq 

def ph2(val,candidate_itemset):
    baskets = list(val)
    c = list()
    for i in candidate_itemset:
        t = 0
        if(type(i) != tuple):
            i = (i,)
        for b in baskets:
            flag =  all(j in b  for j in i)
            if(flag == True):
                t = t + 1
        c.append((i,t))
    
    return(c)



rdd=sc.textFile(sys.argv[3]).filter(lambda x: not x.startswith('user_id')).map(lambda x:x.split(',')).map(lambda x: (str(x[0]),str(x[1]))).groupByKey().map(lambda x: tuple(set(x[1]))).filter(lambda x : len(x) > k1)

support = int(sys.argv[2])
subset_support = math.floor(float(support/rdd.getNumPartitions()))
candidate_itemset = rdd.mapPartitions(get_freq).persist().distinct().collect()
result_map_phase2 = rdd.mapPartitions(lambda x: ph2(x,candidate_itemset)).persist().reduceByKey(lambda u,v: u+v).collect()

result=list()
for i in result_map_phase2:
  if (i[1] >= support):
    result.append(i[0])
    
candidate_result=list()
for i in result_map_phase2:
    candidate_result.append(i[0])    

maximal_length = 0
for i in result:
    if len(i) > maximal_length:
        maximal_length = len(i)
        
out_d = {i:list() for i in range(1,maximal_length+1)}
for i in result:
    out_d[len(i)].append(tuple(sorted(i)))

maximal_length_candidate = 0
for i in candidate_result:
    if len(i) > maximal_length_candidate:
        maximal_length_candidate = len(i)
        
out_d_candidate = {i:list() for i in range(1,maximal_length_candidate+1)}
for i in candidate_result:
    out_d_candidate[len(i)].append(tuple(sorted(i)))    

last=sorted(out_d.keys())[-1]    
with open(sys.argv[4],'w') as fw:
    fw.write('Candidates:\n')
    for k1 in out_d_candidate:
        out_d_candidate[k1].sort()
        fw.write(str(out_d_candidate[k1])[1:-1].replace(',)',')').replace(", (",",("))
        fw.write('\n\n')
    fw.write('Frequent Itemsets:\n')
    for k in out_d:
        out_d[k].sort()
        fw.write(str(out_d[k])[1:-1].replace(',)',')').replace(", (",",("))
        if(k!=last):
            fw.write('\n\n')
fw.close()
    
end_time = time.time()
print("Duration:",end_time - start_time)

