import MapReduce
import sys

"""
Problem 3 (squaring matrix, one-phase)
"""

mr = MapReduce.MapReduce()


def mapper(record):
    # for A: ((i,k), ['A', j, A[i,j]])
    # for B: ((i,k), ['B', j, B[j,k]])
    
    for k in range(0, 5):
        mr.emit_intermediate((record[0], k), ['A', record[1], record[2]])
    for i in range(0, 5):
        mr.emit_intermediate((i, record[1]), ['B', record[0], record[2]])


def reducer(key, list_of_values):
    # emit [i, k, value]

    total = 0
    A = {}
    B = {}
    for v in list_of_values:
        if(v[0] == 'A'):
            A.setdefault(v[1], v[2])
        else:
            B.setdefault(v[1], v[2])
    for j in A:
        if(B.has_key(j)):
            total += A.get(j) * B.get(j)
        
    mr.emit([key[0], key[1], total])


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
