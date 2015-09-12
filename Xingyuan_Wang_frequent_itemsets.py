import MapReduce
import sys

"""
Problem 2 (frequent itemsets)
"""

mr = MapReduce.MapReduce()


# emit [2-itemset tuples, 1] pairs
def mapper(record):
    # key: 2-itemset tuples
    # value: count
    if(len(record) > 1):
        for i in range(0, len(record)-1):
            for j in range(i+1, len(record)):
                mr.emit_intermediate((record[i],record[j]), 1)

def reducer(key, list_of_values):
    # key: 2-itemsets that appear at least 100 times
    # value: none
    total = 0
    for v in list_of_values:
        total += v
    if(total >= 100):
        mr.emit(list(key))

 
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
