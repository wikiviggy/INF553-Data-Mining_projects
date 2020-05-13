from pyspark import SparkContext, SparkConf
import pyspark
from itertools import combinations
import json
import math
import sys

conf = SparkConf().set("spark.ui.port", "4050")
sc = pyspark.SparkContext(conf=conf)
sc.setLogLevel("ERROR")
if(sys.argv[3]=='item_based'):
    rdd=sc.textFile(sys.argv[1])
    js=rdd.map(lambda x : json.loads(x))

    rdd_training=js.map(lambda x : ((x['business_id'],x['user_id']),x['stars']))
    rdd_training=rdd_training.map(lambda x : (x[0][1],(x[0][0],x[1])))
    user_matrix=rdd_training.groupByKey().sortByKey().mapValues(list).collectAsMap()
    item_matrix=rdd_training.map(lambda x : (x[1][0], (x[0], x[1][1]))).groupByKey().sortByKey().mapValues(list).collectAsMap()
    user_items = rdd_training.map(lambda x : ((x[0], x[1][0]), x[1][1])).sortByKey().collectAsMap()

    rdd_train=rdd_training.map(lambda x: ((x[1][0],x[0]),x[1][1]))
    required_rdd_train = rdd_train.map(lambda x : x[0])
    indices_tr=required_rdd_train.map(lambda x : x[1]).zipWithIndex().collectAsMap()
    business_user_map_tr = required_rdd_train.map(lambda x: (x[0], indices_tr.get(x[1]))).groupByKey().sortByKey().mapValues(lambda x: set(x)).persist()
    business_char_matrix_tr = business_user_map_tr.collectAsMap()

    def minhash(users, num_hashes, num_users):
        max_val = float("inf")
        hashed_users = [max_val for i in range(0,num_hashes)]
        lst1=[ 499, 503, 509, 521, 523, 541, 547, 557, 563, 569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887]
        for user in users:
            for i in range(0, num_hashes):
                current_hash_code = (17*user + 43) % lst1[i]

                if current_hash_code < hashed_users[i]:
                    hashed_users[i] = current_hash_code
        return hashed_users

    num_hashes=60
    num_users = required_rdd_train.map(lambda x: x[1]).distinct().count()
    min_hash = business_user_map_tr.mapValues(lambda x: minhash(list(x), num_hashes, num_users))

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

    b=30
    r=2

    candidates = min_hash.flatMap(lambda x : LSH(x[0], list(x[1]), b, r)).groupByKey().filter(lambda x: len(list(x[1])) > 1).flatMap(lambda x : generate_pairs(sorted(x[1])))

    fp=candidates.collect()

    sp1 = sc.parallelize(fp).map(lambda x : (x[1], x[0])).groupByKey().sortByKey().mapValues(list).collectAsMap()
    sp2 = sc.parallelize(fp).map(lambda x : (x[0], x[1])).groupByKey().sortByKey().mapValues(list).collectAsMap()

    sp3 = sc.parallelize(fp).flatMap(lambda x : set(x)).map(lambda x : (1,x))
    sp4=sc.parallelize(fp)

    def pearson(active_item, other_item):
      corated_items = []
      i = 0
      j = 0
      active_item.sort()
      other_item.sort()
      while (i<len(active_item) and j< len(other_item)):
        if active_item[i][0] == other_item[j][0]:
          corated_items.append((active_item[i][0], (active_item[i][1], other_item[j][1])))
          i = i+1
          j = j+1

        elif active_item[i][0] < other_item[j][0]:
          i = i+1

        else:
          j = j+1

      if len(corated_items) == 0 or len(corated_items) == 1 or len(corated_items)==2 :
        return 1.0

      active = [x[1][0] for x in corated_items]
      a_mean = sum(active)/len(active)

      other = [x[1][1] for x in corated_items]
      o_mean = sum(active)/len(active)

      a_list = active
      o_list = other

      num = 0.0
      d1 = 0.0
      d2 = 0.0
      for i in range(len(a_list)):
        a = a_list[i] - a_mean
        o = o_list[i] - o_mean
        num = num + a*o
        d1 = d1 + (a*a)
        d2 = d2 + (o*o)

      den = math.sqrt(d1) * math.sqrt(d2)
      if den == 0 or num == 0:
        return 1.0
      else:
        return num/den
    
    def getitems(user,item):
      top_items = []
      if(item not in sp1 and item not in sp2):
        top_items.append((1, item))
        return top_items

      if item not in item_matrix:
        top_items.append((1, item))
        return top_items

      active_item_data = item_matrix[item]
      other_items = []
      if item in sp1:
        other_items = other_items + sp1[item]
      if item in sp2:
        other_items = other_items + sp2[item]

      other_items = list(set(other_items))

      for others in other_items:
        if item != others:
          other_data = item_matrix[others]
          similarity = pearson(active_item_data, other_data)
          if similarity != -2.0:
            top_items.append((similarity,(others,item)))
              
      top_items=set(top_items)
      similar_ones = sorted(top_items,reverse=True)
      return similar_ones

    #rdd_train_1=rdd_training.map(lambda x : (x[0],x[1][0]))
    #t_items = rdd_training.flatMap(lambda x : getitems(x[0],x[1][0]))
    #t_items = sp3.flatMap(lambda x : getitems(x[0],x[1]))
    sii=sp4.map(lambda x : (x[0],x[1],pearson(item_matrix[x[0]],item_matrix[x[1]])))
    c=sii.map(lambda x : {"b1": x[0], "b2":x[1],"sim": x[2]}).collect()

    with open(sys.argv[2],'w') as fw:
       for i in c:
         json.dump(i,fw)
         fw.write('\n')   
        
elif(sys.argv[3]=='user_based'):
    rdd=sc.textFile(sys.argv[1])
    js=rdd.map(lambda x : json.loads(x))

    rdd_training=js.map(lambda x : ((x['business_id'],x['user_id']),x['stars']))
    rdd_training=rdd_training.map(lambda x : (x[0][1],(x[0][0],x[1])))
    user_matrix=rdd_training.groupByKey().sortByKey().mapValues(list).collectAsMap()
    item_matrix=rdd_training.map(lambda x : (x[1][0], (x[0], x[1][1]))).groupByKey().sortByKey().mapValues(list).collectAsMap()
    user_items = rdd_training.map(lambda x : ((x[0], x[1][0]), x[1][1])).sortByKey().collectAsMap()

    rdd_train=rdd_training.map(lambda x: ((x[1][0],x[0]),x[1][1]))
    required_rdd_train = rdd_train.map(lambda x : x[0])
    indices_tr=required_rdd_train.map(lambda x : x[0]).zipWithIndex().collectAsMap()
    user_business_map_tr = required_rdd_train.map(lambda x: (x[1], indices_tr.get(x[0]))).groupByKey().sortByKey().mapValues(lambda x: set(x)).persist()
    user_char_matrix_tr = user_business_map_tr.collectAsMap()


    def minhash(users, num_hashes, num_business):
        max_val = float("inf")
        hashed_users = [max_val for i in range(0,num_hashes)]
        for user in users:
            for i in range(0, num_hashes,2):
                current_hash_code = ((i+1)*user + 65*(i+1)) % num_business

                if current_hash_code < hashed_users[i]:
                    hashed_users[i] = current_hash_code
        return hashed_users

    num_hashes=14
    num_business = required_rdd_train.map(lambda x: x[1]).distinct().count()
    min_hash = user_business_map_tr.mapValues(lambda x: minhash(list(x), num_hashes, num_business))

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

    b=7
    r=2

    candidates = min_hash.flatMap(lambda x : LSH(x[0], list(x[1]), b, r)).groupByKey().filter(lambda x: len(list(x[1])) > 1).flatMap(lambda x : generate_pairs(sorted(x[1])))

    fp=candidates.collect()

    sp1 = sc.parallelize(fp).map(lambda x : (x[1], x[0])).groupByKey().sortByKey().mapValues(list).collectAsMap()
    sp2 = sc.parallelize(fp).map(lambda x : (x[0], x[1])).groupByKey().sortByKey().mapValues(list).collectAsMap()

    sp3 = sc.parallelize(fp).flatMap(lambda x : set(x)).map(lambda x : (1,x))
    sp4=sc.parallelize(fp)

    def pearson(active_item, other_item):
      corated_items = []
      i = 0
      j = 0
      active_item.sort()
      other_item.sort()
      while (i<len(active_item) and j< len(other_item)):
        if active_item[i][0] == other_item[j][0]:
          corated_items.append((active_item[i][0], (active_item[i][1], other_item[j][1])))
          i = i+1
          j = j+1

        elif active_item[i][0] < other_item[j][0]:
          i = i+1

        else:
          j = j+1

      if len(corated_items) == 0 or len(corated_items) == 1 or len(corated_items)==2 :
        return 1.0

      active = [x[1][0] for x in corated_items]
      a_mean = sum(active)/len(active)

      other = [x[1][1] for x in corated_items]
      o_mean = sum(active)/len(active)

      a_list = active
      o_list = other

      num = 0.0
      d1 = 0.0
      d2 = 0.0
      for i in range(len(a_list)):
        a = a_list[i] - a_mean
        o = o_list[i] - o_mean
        num = num + a*o
        d1 = d1 + (a*a)
        d2 = d2 + (o*o)

      den = math.sqrt(d1) * math.sqrt(d2)
      if den == 0 or num == 0:
        return 1.0
      else:
        return num/den
    
    def getitems(user,item):
      top_items = []
      if(item not in sp1 and item not in sp2):
        top_items.append((1, item))
        return top_items

      if item not in user_matrix:
        top_items.append((1, item))
        return top_items

      active_item_data = user_matrix[item]
      other_items = []
      if item in sp1:
        other_items = other_items + sp1[item]
      if item in sp2:
        other_items = other_items + sp2[item]

      other_items = list(set(other_items))

      for others in other_items:
        if item != others:
          other_data = user_matrix[others]
          similarity = pearson(active_item_data, other_data)
          if similarity != -2.0:
            top_items.append((similarity,others,item))
               
      top_items=set(top_items)
      similar_ones = sorted(top_items,reverse=True)
      return similar_ones
    
    #rdd_train_1=rdd_training.map(lambda x : (x[0],x[1][0]))
    #top_items = rdd_train_1.flatMap(lambda x : getitems(x[1],x[0])).filter(lambda x : x[0] >= 0.01 )
    #top_items = sp3.flatMap(lambda x : getitems(x[0],x[1])).filter(lambda x : x[0] >= 0.01 )
    sii=sp4.map(lambda x : (x[0],x[1],pearson(user_matrix[x[0]],user_matrix[x[1]]))).filter(lambda x : x[2] >= 0.01 )
    content=sii.map(lambda x : {"u1": x[0], "u2":x[1] ,"sim": x[2]}).collect()

    with open(sys.argv[2],'w') as fw:
       for i in content:
         json.dump(i,fw)
         fw.write('\n')   
            
