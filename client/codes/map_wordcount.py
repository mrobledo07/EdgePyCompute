from pyedgecompute import deserialize_input_string, serialize_partition
from collections import Counter

def task(bytes):
    text = deserialize_input_string(bytes)
    words = text.split()
    counter = Counter(words)
    return serialize_partition(counter)