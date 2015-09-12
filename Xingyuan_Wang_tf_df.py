import MapReduce
import sys
import re

"""
Problem 1 (tf-df)
"""

mr = MapReduce.MapReduce()


# make input file into [word, [document id, 1]] lists
def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1].lower()
    words = re.findall(r'[\w\_\d]+', value)
    for w in words:
      mr.emit_intermediate(w, [key, 1])

# calculate df and tf
def reducer(key, list_of_values):
    # key: word
    # value: list of [document ID , tf]
    df = 0
    count_df = {}

    for v in list_of_values:
        if(count_df.has_key(v[0])):
            count_df[v[0]] += v[1]
        else:
            count_df.setdefault(v[0],1)
            df += 1

    tf = []
    for document_title in count_df:
        tf.append([document_title, count_df.get(document_title)])
    
    mr.emit((key, df, tf))


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

