import pandas as pd
from tqdm import tqdm
import json
import numpy as np
import time
from copy import deepcopy
from pyspark import SparkContext, SparkConf
import pyspark
from itertools import combinations
import json
import math
import sys


conf = SparkConf().set("spark.ui.port", "4050")
sc = pyspark.SparkContext(conf=conf)

start = time.time()

line_count = len(open("resource/asnlib/publicdata/train_review.json").readlines())
user_ids, business_ids, stars, dates, texts = [], [], [], [], []

with open("resource/asnlib/publicdata/train_review.json") as f:
    for line in tqdm(f, total=line_count):
        blob = json.loads(line)
        user_ids += [blob["user_id"]]
        business_ids += [blob["business_id"]]
        stars += [blob["stars"]]
        dates += [blob["date"]]
        texts += [blob["text"]]

ratings = pd.DataFrame({"user_id": user_ids, "business_id": business_ids, "rating": stars, "date": dates, "text": texts})

user_counts = ratings["user_id"].value_counts()
active_users = user_counts.loc[user_counts >= 1].index.tolist()
ratings = ratings.loc[ratings.user_id.isin(active_users)]

n_users = len(ratings.user_id.unique())
n_restaurants = len(ratings.business_id.unique())

line_count_tst = len(open(sys.argv[1]).readlines())
user_ids_tst, business_ids_tst= [], []
with open(sys.argv[1]) as f:
    for line in tqdm(f, total=line_count):
        blob = json.loads(line)
        user_ids_tst += [blob["user_id"]]
        business_ids_tst += [blob["business_id"]]
        
        
        
test_ratings = pd.DataFrame({"user_id": user_ids_tst, "business_id": business_ids_tst})
test_ratings['ratings'] = 0

trainset = ratings.loc[:,['user_id', 'business_id', 'rating']]
testset = test_ratings.loc[:, ['user_id', 'business_id','ratings']]

from surprise import Dataset
from surprise import Reader
from surprise import SVD

reader = Reader(rating_scale = (0.0, 5.0))
train_data = Dataset.load_from_df(trainset[['user_id','business_id','rating']], reader)
test_data = Dataset.load_from_df(testset[['user_id','business_id','ratings']], reader)

train_sr = train_data.build_full_trainset()
test_sr_before = test_data.build_full_trainset()
test_sr = test_sr_before.build_testset()

algo = SVD(n_epochs = 27, lr_all = 0.005, reg_all = 0.1) #parameter selection done by 10 fold cross validation

algo.fit(train_sr)

predictions = algo.test(test_sr)

lst=[]
for t in predictions:
  lst.append((t[0],t[1],t[3]))

rdd = sc.parallelize(lst)
content=rdd.map(lambda x : {"user_id": x[0], "business_id":x[1] ,"stars": x[2]}).collect()

with open(sys.argv[2],'w') as fw:
    for i in content:
        json.dump(i,fw)
        fw.write('\n')

end = time.time()

print(end - start)
