from pyspark import SparkContext, SparkConf
import pyspark
from itertools import combinations
import math
import operator
import sys
import json

conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('master', 'local'), ('appName', 'Vignesh_hw3_task2')])
sc = pyspark.SparkContext(conf=conf)

rdd=sc.textFile(sys.argv[1])
js=rdd.map(lambda x : json.loads(x))
N=js.map(lambda x : x['business_id']).distinct().count()

with open(sys.argv[3], 'r') as fp:
    stopwords = fp.read().splitlines()
    
def refine_words(row):
    js = json.loads(str(row))
    if(js['business_id'] is not None and js['text'] is not None):
        text = ''.join(x for x in js['text'].lower() if x.isalpha() or x.isspace())
        words = text.split()
        refined_words = [w for w in words if w not in stopwords]
        return (js['business_id'],refined_words)
    
def combine(items):
    combined_lst = list()
    item_lst= list(items)
    for words in item_lst:
        if(words is not None and len(words) > 0):
            combined_lst.extend(words)
    return combined_lst


business_reviews = rdd.map(refine_words).groupByKey().mapValues(combine).filter(lambda x: x[1])

def business_word_mapping(row):
    bid = row[0]
    w = row[1]
    mapping = [(word, bid) for word in w]
    return mapping

def make_dic(val):
    b_id = list(val)
    count = len(b_id)
    unique_count = len(set(b_id))
    return (count, unique_count)

word_count=business_reviews.flatMap(business_word_mapping).groupByKey().mapValues(make_dic).collectAsMap()

twc = 0
for pair in word_count.values():
    twc += pair[0]
rare_words = twc*0.0001

doc_count_dict = {}
for word, pair in word_count.items():
    if pair[0] >= rare_words:
        doc_count_dict[word] = pair[1]
del word_count

q4=business_reviews.mapValues(lambda x : [(i,1) for i in x])

dic={}
def tf(l):  
  for tupl in l:
    if tupl[0] in doc_count_dict :
      w=tupl[0]
      b =dic[w] if w in dic else 0 
      dic[w] = b+1

  return dic

def tf1(d1):
  ma=max(d1.keys(), key=(lambda k: d1[k]))
  for i in d1.keys():
    d1[i] = d1[i]/d1[ma]

  return d1 

qtf=q4.mapValues(tf).mapValues(tf1)
qidf=q4.map(lambda x: set(x[1])).map(lambda x : list(x)).flatMap(lambda x : (x[0],x[1])).countByKey()

for k in qidf:
  qidf[k] = math.log(float(N/qidf[k]) ,2 )

def tfidf(d3):
  for i in d3:
    for j in qidf:
      if(i==j):
        d3[i]=d3[i]*qidf[j]
   
  return d3

def srt(d4):
  d4_sorted = sorted(d4.items(), key=operator.itemgetter(1), reverse=True)
  return d4_sorted

bv={}
def vect(x):
  for tupl in x:
    w=tupl[0]
    b =bv[w] if w in bv else 0 
    bv[w] = tupl[1]

  return bv

qtfidf=qtf.mapValues(tfidf).mapValues(srt).mapValues(vect).persist()

def flatmap_top_words(row):
    tf_idf_dict = row[1]
    top_pairs = sorted(tf_idf_dict.items(), key=lambda x: x[1], reverse=True)[:200]
    top_words = [pair[0] for pair in top_pairs]
    return top_words

top_words_lst=qtfidf.flatMap(flatmap_top_words).distinct().collect()

top_words_dic = dict()
for ids, word in enumerate(top_words_lst):
    top_words_dic[word] = ids
    
def business_profile(b_iter):
    b_profile = list()
    for w in b_iter.keys():
        if w in top_words_dic:
            b_profile.append(top_words_dic[w])
    return b_profile


bp = qtfidf.mapValues(business_profile).filter(lambda x: x[1])

def pair(r):
    js = json.loads(str(r))
    return (js['business_id'], js['user_id'])

def getuser_profile(b_profiles):
    biz_profiles = list(b_profiles)
    prof_union = set().union(*biz_profiles)
    user_profile = list(prof_union)
    return user_profile


users = rdd.map(pair)
up = bp.join(users).map(lambda x: (x[1][1], x[1][0])).groupByKey().mapValues(getuser_profile).filter(lambda x: x[1])

model = {}
model['business_profiles'] = bp.collectAsMap()
model['user_profiles'] = up.collectAsMap()

with open(sys.argv[2], 'w') as f:
    json.dump(model, f)
