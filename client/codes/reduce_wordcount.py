from pyedgecompute import deserialize_partitions
from collections import Counter

def task(bytes_list):
    parts = deserialize_partitions(bytes_list)
    final_counter = Counter()
    for p in parts:
       final_counter.update(p)
    return json.dumps(final_counter)