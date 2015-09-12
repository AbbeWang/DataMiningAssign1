import MapReduce
import sys

"""
Problem 4 (squaring matrix, two-phase)-2
"""

mr = MapReduce.MapReduce()


def mapper(record):
    # emit ((i,k), value)
    
    mr.emit_intermediate((record[0], record[1]), record[2])


def reducer(key, list_of_values):
    # emit [i,k, sum(value)]

    total = 0
    for v in list_of_values:
        total += v

    mr.emit([key[0], key[1], total])
    


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
