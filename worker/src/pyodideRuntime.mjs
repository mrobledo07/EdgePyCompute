import { loadPyodide } from "pyodide";

let pyodideInstance;

export async function initPyodide() {
  console.log("⏳ Loading Pyodide...");
  pyodideInstance = await loadPyodide();
  await pyodideInstance.loadPackage("pandas");
  await pyodideInstance.runPythonAsync(`
# pyedgecompute.py
import pickle, base64, json
import pandas as pd
import numpy as np


# CODE MAPREDUCE
def serialize_partition(result):
    raw = pickle.dumps(result)
    return base64.b64encode(raw).decode('utf-8')

def deserialize_partitions(b64_list):
    parts = []
    for b64 in b64_list:
        raw_bytes = base64.b64decode(b64)
        part = pickle.loads(raw_bytes)
        parts.append(part)
    return parts

def deserialize_input_string(bytes_string):
    string_data = bytes_string.decode('utf-8')
    return string_data

# CODE TERASORT
def deserialize_input_terasort(data):
    lines = data.split(b'\\n')
    result = {}
    for line in lines:
        if len(line) >= 98:
            key = line[:10].decode('utf-8', errors='ignore')
            value = line[12:98].decode('utf-8', errors='ignore')
            result[key] = value
    df = pd.DataFrame(list(result.items()), columns=["0", "1"])
    result = df
    return result

MIN_CHAR_ASCII = 32  # ' '
MAX_CHAR_ASCII = 126 # '~'
range_per_char = MAX_CHAR_ASCII - MIN_CHAR_ASCII
base = range_per_char + 1

def get_partition(line, num_partitions):

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

def partition_data(data, num_partitions):
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

def concat_partitions(partition_list):

    if not partition_list:
        return pd.DataFrame(columns=["0", "1"])

    return pd.concat(partition_list, ignore_index=True)

def sort_dataframe(df):

    return df.sort_values(by=["0"]).reset_index(drop=True)

# Inject module
import types
pyedgecompute = types.ModuleType("pyedgecompute")
pyedgecompute.serialize_partition = serialize_partition
pyedgecompute.deserialize_partitions = deserialize_partitions
pyedgecompute.deserialize_input_string = deserialize_input_string
pyedgecompute.deserialize_input_terasort = deserialize_input_terasort
pyedgecompute.partition_data = partition_data
pyedgecompute.concat_partitions = concat_partitions
pyedgecompute.sort_dataframe = sort_dataframe


import sys
sys.modules["pyedgecompute"] = pyedgecompute
`);
  console.log("✅ Pyodide ready");
  return pyodideInstance;
}

export function getPyodide() {
  if (!pyodideInstance) throw new Error("Pyodide not initialized");
  return pyodideInstance;
}
