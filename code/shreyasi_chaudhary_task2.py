import time
from pyspark import SparkContext
import sys
from collections import defaultdict
import itertools
import random
# import pickle
def defaultvalue():
    return 0

def countSubsets(basket_list):
    d = defaultdict(int)
    basket_list = list(basket_list)
    for basket in basket_list:
        for item in candidate_items_sets:
            if(item.issubset(basket)):
                d[tuple(item)] += 1
    #print("PHASE 2 DICT: ", d)
    return d.items()

def Apriori(basket_list):
    counts = defaultdict(int)
    basket_list = list(basket_list)
    # print("BASKET LIST", basket_list)
    all_freq_items = []
    freq_items = set()
    for basket in basket_list:
        for item in basket:
            #print(item)
            counts[str(item)] += 1
    #print(counts)
    for item in counts:
        if(counts[item] >= support_partitions):
            freq_items.add(item)
    freq_items = list(freq_items)
    all_freq_items.append(freq_items)
    counts = defaultdict(int)
    #k items set 
    
    item_set_size = 2
    #while(len(freq_items) > 0):
    while(item_set_size <= 2):
        counts_k = defaultdict(int)
        candidate_items = list(map(lambda x:list(x), list(itertools.combinations(freq_items, item_set_size))))
        #print("CANDIDATE ITEMS: ", candidate_items)
        
        freq_items = set()

        for basket in basket_list:
            for item in candidate_items:
                item = set(item)
                if item.issubset(basket):
                    counts_k[tuple(item)] += 1
        # print("DICT: ", counts_k)
        for item in counts_k:
            if(counts_k[item] >= support_partitions):
                freq_items.add(item)
        freq_items = list(freq_items)
        
        all_freq_items.append(freq_items)
        freq_items = list(set([item for sublist in freq_items for item in sublist]))
        # print("FREQ ITEMS: ", freq_items)
        item_set_size += 1
        
    
    return all_freq_items


start_time = time.time()
sc = SparkContext(appName="HW2")
filter_value = int(sys.argv[1])
support = int(sys.argv[2])
# print("filer value: ", filter_value)
# print("support: ", support)

numOfPartitions = 2

input_file = sys.argv[3]

output_file = sys.argv[4]

support_partitions = support/numOfPartitions
data = sc.textFile(input_file)

header = data.first() #extract header
data = data.filter(lambda row:  row != header).map(lambda x: x.split(","))
# print("Number of partitions: ", data.getNumPartitions())
# print("Length of partitions: ", data.glom().map(len).collect())

baskets = data.groupByKey().filter(lambda x: len(x[1]) >= filter_value).map(lambda x: set(x[1]))
#print(baskets.collect())
baskets = baskets.repartition(numOfPartitions)
baskets_res = baskets.mapPartitions(Apriori)
#print(baskets.collect())

candidate_items_old = baskets_res.collect()
# print("FINAL RESULT PHASE 1: ", candidate_items_old)

# print("PHASE 1")




candidate_items_old = [item for sublist in candidate_items_old for item in sublist]
# print(candidate_items_old)
candidate_items_all = []
for i in candidate_items_old:
    if(type(i) is tuple):
        candidate_items_all.append(set(i))
    else:
        s = set()
        s.add(i)
        candidate_items_all.append(s)


# pickle.dump( candidate_items_all, open( "abc_task1.p", "wb" ) )
f = open(output_file, "w")
# candidate_items_all = pickle.load(open("abc_task1.p", "rb" ))
f.write("Candidates: \n")
#f.write(str(candidate_items_all) + '\n')
candidate_items_sets = candidate_items_all
candidate_items_all = list(set(map(lambda x: tuple(x), candidate_items_all)))
# print(candidate_items_all)
one_itemset = []
for my_itemset in candidate_items_all:
    if(len(my_itemset) == 1):
        s = "('"+ my_itemset[0]+"')"
        if s not in one_itemset:
            one_itemset.append(s)
# print(one_itemset)
 
max_len = len(max(candidate_items_all, key=len))
one_itemset = sorted(one_itemset)
f.write(','.join(item for item in one_itemset) + '\n')
f.write('\n')
# print("MAX LEN", max_len)
res_list = []
for i in range(2, max_len+1):
   
    x = []
    for val in candidate_items_all:
        if(len(val) == i):
            x.append(str(val))
    x = sorted(x)
    f.write(','.join(item for item in x) + '\n')
    f.write('\n')
    #f.write(str(x) + '\n')


    
#PHASE 2 started
map_phase2 = baskets.mapPartitions(countSubsets).filter(lambda x: x[1] >= support_partitions).reduceByKey(lambda x, y: x+y).filter(lambda x: x[1] >= support)
#print(map_phase2.collect())
map_phase2 = map_phase2.map(lambda x: x[0])
f.write("Frequent Itemsets: \n")

results = map_phase2.collect()


#results = list(set(map(lambda x: tuple(x), result)))
# print("FINAL RESULTS: ", results)
one_itemset = []
for my_itemset in results:
    if(len(my_itemset) == 1):
        s = "('"+ my_itemset[0]+"')"
        if s not in one_itemset:
            one_itemset.append(s)
#print(one_itemset)
 
max_len = len(max(results, key=len))
one_itemset = sorted(one_itemset)
f.write(','.join(item for item in one_itemset) + '\n')
f.write('\n')
# print("MAX LEN", max_len+1)
res_list = []
for i in range(2, max_len+1):
    
    x = []
    for val in results:
        if(len(val) == i):
            x.append(str(val))
    x = sorted(x)
    f.write(','.join(item for item in x) + '\n')
    f.write('\n')





print("Duration: %s " % (time.time() - start_time))


#../spark-2.3.2-bin-hadoop2.7/bin/spark-submit shreyasi_chaudhary_task2.py 70 50 dataset_task2.txt res_4_final.txt