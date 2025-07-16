import os
from typing import (
    Dict,
    List,
    Tuple
)

import pandas as pd
import numpy as np
import pickle

MIN_CHAR_ASCII = 32  # ' '
MAX_CHAR_ASCII = 126 # '~'
range_per_char = MAX_CHAR_ASCII - MIN_CHAR_ASCII
base = range_per_char + 1


def get_read_range(
    data_size: int,
    partition_id: int,
    num_partitions: int
) -> Tuple[int, int]:

    total_registers = data_size / 100

    registers_per_worker = total_registers // num_partitions

    lower_bound = partition_id * registers_per_worker * 100

    if partition_id == (num_partitions - 1):
        upper_bound = data_size - 1
    else:
        upper_bound = lower_bound + registers_per_worker * 100 - 1

    return int(lower_bound), int(upper_bound)


def read_input(
    bucket: str,
    key: str,
    lower_bound: int,
    upper_bound: int
) -> bytes:

    # Read a specific byte range from a file in a bucket.
    # This is a placeholder for actual S3/MinIO read operation.
    # In Minio/s3, you would use the `get_object` method with byte range.
    with open(os.path.join(bucket, key), "rb") as f:
        f.seek(lower_bound)
        data = f.read(upper_bound - lower_bound + 1)
        return data


def parse_input(
    data: bytes
) -> pd.DataFrame:

    lines = data.split(b'\n')
    result = {}
    for line in lines:
        if len(line) >= 98:
            key = line[:10].decode('utf-8', errors='ignore')
            value = line[12:98].decode('utf-8', errors='ignore')
            result[key] = value
    df = pd.DataFrame(list(result.items()), columns=["0", "1"])
    result = df
    return result


def get_partition(line: str, num_partitions: int) -> int:

    numerical_value = 0
    max_numerical_value = 0
    for i in range(8):
        if i < len(line):
            normalized_char_val = ord(line[i]) - MIN_CHAR_ASCII
            numerical_value = numerical_value * base + normalized_char_val
        else:
            normalized_char_val = 0
            numerical_value = numerical_value * base + normalized_char_val

    for i in range(8):
        max_numerical_value = max_numerical_value * base + range_per_char

    if max_numerical_value == 0:
        return 0

    normalized_value = numerical_value / max_numerical_value
    partition = int(normalized_value * num_partitions)

    if partition >= num_partitions:
        partition = num_partitions - 1

    return partition


def partition_data(
    data: pd.DataFrame,
    num_partitions: int
) -> Dict[int, pd.DataFrame]:

    partitions = {i: None for i in range(num_partitions)}
    partition_indices = np.empty(len(data), dtype=np.int32)

    for idx, key in enumerate(data["0"]):
        partition_indices[idx] = get_partition(key, num_partitions)

    for i in range(num_partitions):
        indices = np.where(partition_indices == i)[0]
        if indices.size > 0:
            partitions[i] = data.iloc[indices].reset_index(drop=True)
        else:
            partitions[i] = pd.DataFrame(columns=data.columns)

    return partitions


def write_partitions(
    bucket: str,
    partition_prefix: str,
    partitions: Dict[int, pd.DataFrame]
):
    for partition_id, df in partitions.items():
        pickle_bytes = pickle.dumps(df)

        # This is a placeholder for actual S3/MinIO read operation.
        partition_path = os.path.join(
            bucket,
            f"{partition_prefix}_part_{partition_id}.pkl"
        )
        with open(partition_path, "wb") as f:
            f.write(pickle_bytes)


def mapper(
    bucket: str,
    key: str,
    data_size: int,
    mapper_id: int,
    num_mappers: int,
    num_reducers: int,
    partition_prefix: str = "part_"
):
    lower_bound, upper_bound = get_read_range(
        data_size=data_size,
        partition_id=mapper_id,
        num_partitions=num_mappers
    )

    chunk = read_input(
        bucket=bucket,
        key=key,
        lower_bound=lower_bound,
        upper_bound=upper_bound
    )

    parsed_data = parse_input(chunk)

    partitioned_data = partition_data(
        data=parsed_data,
        num_partitions=num_reducers
    )

    write_partitions(
        bucket=bucket,
        partition_prefix=f"{partition_prefix}_mapper_{mapper_id}",
        partitions=partitioned_data
    )


def read_partitions(
    bucket: str,
    partition_prefix: str,
    num_mappers: int,
    reducer_id: int
) -> List[pd.DataFrame]:

    partition_list = []
    for mapper_id in range(num_mappers):
        partition_path = os.path.join(
            bucket,
            f"{partition_prefix}_mapper_{mapper_id}_part_{reducer_id}.pkl"
        )
        if os.path.exists(partition_path):
            with open(partition_path, "rb") as f:
                partition_df = pickle.load(f)
                partition_list.append(partition_df)
    return partition_list


def concat_partitions(
    partition_list: List[pd.DataFrame]
) -> pd.DataFrame:

    if not partition_list:
        return pd.DataFrame(columns=["0", "1"])

    return pd.concat(partition_list, ignore_index=True)


def sort_dataframe(
    df: pd.DataFrame
) -> pd.DataFrame:

    return df.sort_values(by=["0"]).reset_index(drop=True)


def write_output(
    bucket: str,
    output_key: str,
    df: pd.DataFrame
):

    # This is a placeholder for actual S3/MinIO write operation.
    output_path = os.path.join(bucket, output_key)
    with open(output_path, "wb") as f:
        pickle.dump(df, f)
    print(f"Output written to {output_path}")


def reducer(
    bucket: str,
    partition_prefix: str,
    num_mappers: int,
    reducer_id: int,
    out_prefix: str
):

    partition_list = read_partitions(
        bucket=bucket,
        partition_prefix=partition_prefix,
        num_mappers=num_mappers,
        reducer_id=reducer_id
    )

    concatenated_data = concat_partitions(partition_list)

    sorted_data = sort_dataframe(concatenated_data)

    output_key = f"{out_prefix}_reducer_{reducer_id}.pkl"
    write_output(
        bucket=bucket,
        output_key=output_key,
        df=sorted_data
    )

    return output_key


if __name__ == "__main__":

    # Example usage
    BUCKET = "../data"
    FILE_NAME = "terasort-20m"
    PARTITION_PREFIX = "part_"
    OUTPUT_PREFIX = "sort_out_"
    NUM_MAPPERS = 3
    NUM_REDUCERS = 5

    # Substitute this with a minio/s3 head request
    # to get the size of the file
    # (Could be done from the client before execution)
    data_path = "../data/terasort-20m"
    data_size = os.path.getsize(data_path)
    print(f"Total bytes in file: {data_size}")

    for mapper_id in range(NUM_MAPPERS):
        mapper(
            bucket=BUCKET,
            key=FILE_NAME,
            data_size=data_size,
            mapper_id=mapper_id,
            num_mappers=NUM_MAPPERS,
            num_reducers=NUM_REDUCERS,
            partition_prefix=PARTITION_PREFIX
        )

    output_keys = []
    for reducer_id in range(NUM_REDUCERS):
        k = reducer(
            bucket=BUCKET,
            partition_prefix=PARTITION_PREFIX,
            num_mappers=NUM_MAPPERS,
            reducer_id=reducer_id,
            out_prefix=OUTPUT_PREFIX
        )
        output_keys.append(k)

    print(f"Output keys: {output_keys}")
