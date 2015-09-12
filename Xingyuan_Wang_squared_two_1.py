import MapReduce
import sys

"""
Problem 4 (squaring matrix, two-phase)-1
"""

mr = MapReduce.MapReduce()


def mapper(record):
    # for A: (j, ['A', i, A[i,j]])
    # for B: (j, ['B', k, B[j,k]])
    
    mr.emit_intermediate(record[1], ['A', record[0], record[2]])
    mr.emit_intermediate(record[0], ['B', record[1], record[2]])


def reducer(key, list_of_values):
    # emit [i,k, A[i,j] * B[j,k]]
    
    A = {}
    B = {}
    for v in list_of_values:
        if(v[0] == 'A'):
            A.setdefault(v[1], v[2])
        else:
            B.setdefault(v[1], v[2])
    for i in A:
        for k in B:
            mr.emit([i, k, A.get(i) * B.get(k)])
    


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
