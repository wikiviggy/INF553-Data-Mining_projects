from pyspark import SparkContext, SparkConf
import pyspark
from itertools import combinations
import json
import math
import sys

conf = SparkConf().set("spark.ui.port", "4050")
sc = pyspark.SparkContext(conf=conf)

if(sys.argv[5] == 'item_based'):
    rdd=sc.textFile(sys.argv[1])
    js=rdd.map(lambda x : json.loads(x))
    rdd1=js.map(lambda x : ((x['user_id'],x['business_id']),x['stars']))
    rdd2=sc.textFile(sys.argv[2])
    js1=rdd2.map(lambda x : json.loads(x))
    rdd_testing=js1.map(lambda x : ((x['business_id'],x['user_id']),1)).map(lambda x : (x[0][1],x[0][0])).sortByKey()

    rdd_train = rdd1.collectAsMap()
    user_business_train = rdd1.map(lambda x: (x[0][0], x[0][1])).groupByKey().mapValues(lambda x: set(x)).sortByKey().collectAsMap()
    business_user_train = rdd1.map(lambda x: (x[0][1], x[0][0])).groupByKey().mapValues(lambda x: set(x)).sortByKey().collectAsMap()

    biz_avg = rdd1.map(lambda x: (x[0][1],x[1])).mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0] , x[1] + y[1])).map(lambda x : (x[0],x[1][0]/x[1][1])).collectAsMap()
    user_avg = rdd1.map(lambda x: (x[0][0], x[1])).mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0] , x[1] + y[1])).map(lambda x : (x[0],x[1][0]/x[1][1])).collectAsMap()


    def predict(user, business):
        preds = 0
        pearson_sim_weights = {}
 
    # rating if no other user rated (First Rater issue)
        if business in biz_avg.keys():
          default = biz_avg.get(business)
        else: 
          if user in user_avg.keys():
            default = user_avg.get(user)
          else:
            default = 0

        if(business_user_train.get(business)):
            corated_users = set(business_user_train[business])
        else:
            corated_users = set()
  
        #if(user_business_train.get(user)):
        user_business_interests = set(user_business_train[user])
        #else:
        #return ((user,business),1.0)
    
        bsn_not_req = set()

        if len(corated_users) >= 30:
          for bn in user_business_interests:
            c_users = set()

            bn_users = set(business_user_train[bn])
        
            c_users = corated_users.intersection(bn_users)
            
            if(len(c_users)>=3):
              c_users = set(list(c_users)[:5])

            if ((business,bn)) not in pearson_sim_weights.keys() and len(c_users)>=3:
              b1_ratings = 0
              b2_ratings  = 0
              pearson_W_ij = 0

              for usr in c_users:
                b1_ratings += rdd_train[(usr,business)]
                b2_ratings += rdd_train[(usr,bn)]
          
              b1_avg = b1_ratings / len(c_users)
              b2_avg = b2_ratings / len(c_users)
          
              W_num = 0
              W_den = 0
              b1_den = 0
              b2_den = 0
  
              for usr in c_users:
                b1_rating  =  rdd_train[(usr,business)] - b1_avg
                b2_rating  =  rdd_train[(usr,bn)] - b2_avg

                W_num = W_num + b1_rating * b2_rating

                b1_den += b1_den + b1_rating ** 2
                b2_den += b2_den + b2_rating ** 2
          
              W_den = math.sqrt(b1_den) * math.sqrt(b2_den)
          
              if(W_den != 0 and W_num != 0):
            #Taking Absolute weights only
                pearson_W_ij = abs(W_num / W_den)

              pearson_sim_weights[(business,bn)] = pearson_W_ij
            else:
              bsn_not_req.add(bn)
      
          user_business_interests = user_business_interests -  bsn_not_req


      #PEARSON_PREDICTION
          Pearson_num = 0
          Pearson_den = 0

          for bn in user_business_interests:
            bn_W_in = pearson_sim_weights[(business,bn)]

            if bn_W_in != 0:
              bn_rating_un = rdd_train[(user, bn)]

              Pearson_num += bn_W_in * bn_rating_un 
              Pearson_den += abs(bn_W_in)

          if(Pearson_den != 0):
            preds = float(Pearson_num / Pearson_den)
          if preds == 0:
            preds = default
      
        else:
          preds = default
    

        return ((user,business),preds)

    def check_pred(x):
     if x > 5:
      return 5
     if x < 0:
      return 0
     return x

    predictions = rdd_testing.map(lambda x: predict(x[0], x[1])).mapValues(lambda x : check_pred(x))
    content=predictions.map(lambda x : {"user_id": x[0][0], "business_id":x[0][1] ,"stars": x[1]}).collect()

    with open(sys.argv[4],'w') as fw:
       for i in content:
         json.dump(i,fw)
         fw.write('\n')

elif( sys.argv[5] =='user_based'):
    rdd=sc.textFile(sys.argv[1])
    js=rdd.map(lambda x : json.loads(x))
    rdd1=js.map(lambda x : ((x['user_id'],x['business_id']),x['stars']))
    rdd2=sc.textFile(sys.argv[2])
    js1=rdd2.map(lambda x : json.loads(x))
    rdd_testing=js1.map(lambda x : ((x['business_id'],x['user_id']),1)).map(lambda x : (x[0][1],x[0][0])).sortByKey()

    rdd_train = rdd1.collectAsMap()
    user_business_train = rdd1.map(lambda x: (x[0][0], x[0][1])).groupByKey().mapValues(lambda x: set(x)).sortByKey().collectAsMap()
    business_user_train = rdd1.map(lambda x: (x[0][1], x[0][0])).groupByKey().mapValues(lambda x: set(x)).sortByKey().collectAsMap()

    biz_avg = rdd1.map(lambda x: (x[0][1],x[1])).mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0] , x[1] + y[1])).map(lambda x : (x[0],x[1][0]/x[1][1])).collectAsMap()
    user_avg = rdd1.map(lambda x: (x[0][0], x[1])).mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0] , x[1] + y[1])).map(lambda x : (x[0],x[1][0]/x[1][1])).collectAsMap()


    def predict(user, business):
        preds = 0
        pearson_sim_weights = {}
 
    # rating if no other user rated (First Rater issue)
        if business in biz_avg.keys():
          default = biz_avg.get(business)
        else: 
          if user in user_avg.keys():
            default = user_avg.get(user)
          else:
            default = 0

        if(user_business_train.get(user)):
            corated_items = set(user_business_train[user])
        else:
            corated_items = set()
  
        if(business_user_train.get(business)):
         business_user_interests = set(business_user_train[business])
        else:
          return ((user,business),3.0)
    
        usr_not_req = set()

        if len(corated_items) >= 50:
          for bn in business_user_interests:
            c_item = set()

            bn_items = set(user_business_train[bn])
        
            c_items = corated_items.intersection(bn_items)
            
            if(len(c_items)>=3):
              c_items = set(list(c_items)[:5])

            if ((bn,user)) not in pearson_sim_weights.keys() and len(c_items)>=3:
              b1_ratings = 0
              b2_ratings  = 0
              pearson_W_ij = 0

              for it in c_items:
                b1_ratings += rdd_train[(user,it)]
                b2_ratings += rdd_train[(bn,it)]
          
              b1_avg = b1_ratings / len(c_items)
              b2_avg = b2_ratings / len(c_items)
          
              W_num = 0
              W_den = 0
              b1_den = 0
              b2_den = 0
  
              for it in c_items:
                b1_rating  =  rdd_train[(user,it)] - b1_avg
                b2_rating  =  rdd_train[(bn,it)] - b2_avg

                W_num = W_num + b1_rating * b2_rating

                b1_den += b1_den + b1_rating ** 2
                b2_den += b2_den + b2_rating ** 2
          
              W_den = math.sqrt(b1_den) * math.sqrt(b2_den)
          
              if(W_den != 0 and W_num != 0):
            #Taking Absolute weights only
                pearson_W_ij = abs(W_num / W_den)

              pearson_sim_weights[(bn,user)] = pearson_W_ij
            else:
              usr_not_req.add(bn)
      
          business_user_interests = business_user_interests -  usr_not_req


      #PEARSON_PREDICTION
          Pearson_num = 0
          Pearson_den = 0

          for bn in business_user_interests:
            bn_W_in = pearson_sim_weights[(bn,user)]

            if bn_W_in != 0:
              bn_rating_un = rdd_train[(bn, business)]

              Pearson_num += bn_W_in * bn_rating_un
              Pearson_den += abs(bn_W_in)

          if(Pearson_den != 0):
            preds = float(Pearson_num / Pearson_den)
          if preds == 0:
            preds = default
      
        else:
          preds = default
    

        return ((user,business),preds)


    predictions = rdd_testing.map(lambda x: predict(x[0], x[1]))
    content=predictions.map(lambda x : {"user_id": x[0][0], "business_id":x[0][1] ,"stars": x[1]}).collect()

    with open(sys.argv[4],'w') as fw:
       for i in content:
         json.dump(i,fw)
         fw.write('\n')
    
     
