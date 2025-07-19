from pyedgecompute import deserialize_input_terasort, partition_data, serialize_partition

def task(bytes):
    parsed_data = deserialize_input_terasort(bytes)
    partitioned_data = partition_data(
        data=parsed_data,
        num_partitions=5  # puedes parametrizarlo si lo deseas
    )
    serialized_partitions = [serialize_partition(p) for p in partitioned_data.values()]
    return json.dumps(serialized_partitions)