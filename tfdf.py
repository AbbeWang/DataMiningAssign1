import MapReduce
import sys
import re

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1].lower()
    words = re.findall(r'[\w\_\d]+', value)
    for w in words:
      mr.emit_intermediate(w, [key, 1])

def reducer(key, list_of_values):
    # key: word
    # value: list of [document ID , tf]
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
