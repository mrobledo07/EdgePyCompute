from pyedgecompute import deserialize_input_terasort, partition_data, serialize_partition

def task(bytes):
    print("✅ Start map_terasort")
    parsed_data = deserialize_input_terasort(bytes)
    print(f"✅ Parsed data: {len(parsed_data)} elements")
    total_memory_usage_bytes = parsed_data.memory_usage(deep=True).sum()

    print(f"The absolute size of the DataFrame is: {total_memory_usage_bytes} bytes")

    partitioned_data = partition_data(
        data=parsed_data,
        num_partitions=num_partitions  # puedes parametrizarlo si lo deseas
    )
    print("✅ Partitioning done")

    serialized_partitions = [serialize_partition(p) for p in partitioned_data.values()]
    print("✅ Serialization done")

    return json.dumps(serialized_partitions)