from pyspark import SparkContext, SparkConf
import pyspark
import json
import sys
import math
from itertools import combinations

conf = SparkConf().set("spark.ui.port", "4050")
sc = pyspark.SparkContext(conf=conf)

rdd=sc.textFile(sys.argv[1])
js=rdd.map(lambda x : json.loads(x))
rdd1=js.map(lambda x : ((x['business_id'],x['user_id']),x['stars']))
required_rdd = rdd1.map(lambda x : x[0] )
num_users = required_rdd.map(lambda x: x[1]).distinct().count()
indices=required_rdd.map(lambda x : x[1]).zipWithIndex().collectAsMap()
business_user_map = required_rdd.map(lambda x: (x[0], indices.get(x[1]))).groupByKey().sortByKey().mapValues(lambda x: set(x)).persist()
business_char_matrix = business_user_map.collectAsMap()

def minhash(users, num_hashes, num_users):
    max_val = float("inf")
    hashed_users = [max_val for i in range(0,num_hashes)]

    for user in users:
        for i in range(0, num_hashes,2):
            current_hash_code = ((i+1)*user + (5*(i+1)*13)) % num_users

            if current_hash_code < hashed_users[i]:
                hashed_users[i] = current_hash_code
    return hashed_users

num_hashes = 80
min_hash = business_user_map.mapValues(lambda x: minhash(list(x), num_hashes, num_users))

b = 40
r = 2

def generate_pairs(x): 
    return sorted(list(combinations(sorted(x),2)))

def LSH(business_id, signatures, n_bands, n_rows):
    signature_tuples = []
    for band in range(0,n_bands):
        final_signature = [band]
        final_signature.extend(signatures[band*n_rows:(band*n_rows)+n_rows])
        signature_tuple = (tuple(final_signature), business_id)
        signature_tuples.append(signature_tuple)
    return signature_tuples

candidates = min_hash.flatMap(lambda x : LSH(x[0], list(x[1]), b, r)).groupByKey().filter(lambda x: len(list(x[1])) > 1).flatMap(lambda x : generate_pairs(sorted(x[1])))

lsh_candidates=candidates.collect()

c_pairs=sc.parallelize(lsh_candidates)

def JS(x,business_char):

    c1= set(business_char.get(x[0]))
    c2= set(business_char.get(x[1]))
    jaccard_intersect = len(c1.intersection(c2))
    jaccard_union = len(c1.union(c2))

    similarity = float(jaccard_intersect)/float(jaccard_union)

    return x,similarity

final_required  = candidates.map(lambda x : JS(x, business_char_matrix)).filter(lambda x: x[1] >= 0.05).sortByKey()
l = final_required.distinct().sortBy(lambda x: (x[0][0], x[0][1])).collect()

write=sc.parallelize(l)

content=write.map(lambda x : {"b1": x[0][0], "b2":x[0][1] , "sim": x[1]}).collect()
with open(sys.argv[2],'w') as fw:
   for i in content:
     json.dump(i,fw)
     fw.write('\n')   
