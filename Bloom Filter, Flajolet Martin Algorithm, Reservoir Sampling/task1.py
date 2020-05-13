import os
import json
import sys
import pyspark
import random
import math
from pyspark import SparkContext
from pyspark import SparkConf
import time
import binascii

inp1 = sys.argv[1]
inp2 = sys.argv[2]
op_path = sys.argv[3]

Conf = pyspark.SparkConf().setAppName('hw 6 task1').setMaster('local[*]')

sc = pyspark.SparkContext(conf = Conf)
sc.setLogLevel("ERROR")
t1 = time.time()


def isPrime(n):
    if (n <= 1):
        return False
    if (n <= 3):
        return True

    if (n % 2 == 0 or n % 3 == 0):
        return False

    i = 5
    while (i * i <= n):
        if (n % i == 0 or n % (i + 2) == 0):
            return False
        i = i + 6
    return True

def generate_primes(n):
  found = False
  while(not found):
    n = n + 1
    if(isPrime(n) == True):
      found = True
  return n

def bf(f,stream_items):
  global array
  lst1 = list()
  for i in stream_items:
    flag = 1
    if(i == ""):
      lst1.append(str(0))
    else:
      city_hash = int(binascii.hexlify(i.encode('utf8')), 16)
      for z in range(0,len(hash_vals)):
        A = hash_vals[z][0]
        B = hash_vals[z][1]
        Prime = hash_vals[z][2]
        h = ((A*city_hash + B) % Prime) % length

        if(array[h] == 0):
          array[h] = 1
          flag = 0
      
      if(f == inp2):
        if(flag == 0):
          lst1.append(str(0))
        elif(flag !=0 ):
          lst1.append(str(1))
  
  return lst1

business_first = sc.textFile(inp1)
js1 = business_first.map(lambda x : json.loads(x))
business_first = js1.map(lambda x: x['city']).collect()

business_second = sc.textFile(inp2)
js2 = business_second.map(lambda x : json.loads(x))
business_second = js2.map(lambda x: x['city']).collect()

length =  js1.count()
array = [0] * length
hash_vals = list()
n = length
noofhashes = int(round((n/length) * math.log(2)))
for i in range(0,noofhashes):
  x1 = random.choices([x for x in range(1, 65534) if isPrime(x)], k=2)[0]
  x2 = random.choices([x for x in range(1, 65534) if isPrime(x)], k=2)[0]
  p1 = random.choices([x for x in range(121, 65535) if isPrime(x)], k=2)[0]
  prime_generator = generate_primes(p1)
  hash_vals.append((x1,x2,p1))

generate_bitarr = bf(inp1,business_first)
bitarr_checker = bf(inp2,business_second)

with open(op_path, "w" ) as fw:
  for i in bitarr_checker:
    t=0
    if(t < len(bitarr_checker)-1):
      fw.write(i + " ")
    else:
      fw.write(i)

t2 = time.time()

print(t2-t1)

