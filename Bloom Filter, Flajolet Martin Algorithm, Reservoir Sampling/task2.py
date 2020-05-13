from pyspark import SparkContext, StorageLevel
from pyspark.streaming import StreamingContext
import sys
import json
import math
import binascii
from datetime import datetime
from statistics import mean, median
import random


def prime_checker(num):
    if (num <= 1):
        return False
    if (num <= 3):
        return True    
    if (num % 2 == 0 or num % 3 == 0):
        return False
    i = 5
    while (i **2 <= num):
        if (num % i == 0 or num % (i + 2) == 0):
            return False
        i = i + 6
    
    return True

prime = random.choices([x for x in range(1000000000, 1000000100) if prime_checker(x)],k=1)[0]
def Flajolet_martin(stream_items):
    global visited_cities
    global noofcities
    global out_file
    global num_hashes
    global estimate
    global size
    
    cities = stream_items.collect()
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    true_count = len(set(cities))
    final_est = 0
    max_trailing_zeroes = -math.inf

    all_estimates = list()
    for i in range(1, num_hashes+1):
        for city in cities:
            city_hashes = int(binascii.hexlify(city.encode('utf8')), 16)
            hash_val = (((a[i] * city_hashes) + b[i]) % prime) % estimate
            hash_bin = bin(hash_val)[2:]
            hash_bin_without_zeroes = hash_bin.rstrip("0")
            trailing_zeroes_count = len(hash_bin) - len(hash_bin_without_zeroes)
        
            if(trailing_zeroes_count > max_trailing_zeroes):
                 max_trailing_zeroes = trailing_zeroes_count
        
        all_estimates.append((2 ** max_trailing_zeroes))
        max_trailing_zeroes = -math.inf

 
    chunk_avgs = list()
    start = 0
    for end in range(grp_size, num_hashes,grp_size):
        chunk_avgs.append(mean(all_estimates[start:end]))
        start = end
    
    final_est = median(chunk_avgs)
    out = str(current_timestamp) + "," + str(true_count) + "," + str(final_est) + "\n"
    out_file.write(out)
    out_file.flush()

    return



 
pnumber = int(sys.argv[1])
out_file_path = sys.argv[2]

batch_size = 5 
window_length = 30 
sliding_interval = 10 
num_hashes = 9 
grp_size = 3
estimate = 2 ** num_hashes
random.seed(7)
a = random.choices([x for x in range(1000, 30000) if prime_checker(x)], k=num_hashes+1)
b = random.choices([x for x in range(1000, 30000) if prime_checker(x)], k=num_hashes+1)
sc = SparkContext("local[*]", "hw6_task2")
sc.setLogLevel(logLevel="OFF")
ssc = StreamingContext(sc, batch_size)
out_file = open(out_file_path, "w", encoding="utf-8")
out = "Time,Ground Truth,Estimation" + "\n"
out_file.write(out)
data = ssc.socketTextStream("localhost", pnumber)
res = data.map(json.loads).map(lambda x: x['city']).window(windowDuration = window_length, slideDuration = sliding_interval).foreachRDD(Flajolet_martin)

ssc.start()
ssc.awaitTermination()
