from pyedgecompute import deserialize_partitions, concat_partitions, sort_dataframe, serialize_partition

def task(bytes):
    parts = deserialize_partitions(bytes)
    concatenated_data = concat_partitions(parts)
    sorted_data = sort_dataframe(concatenated_data)
    serialized_partition = serialize_partition(sorted_data)
    return serialized_partition