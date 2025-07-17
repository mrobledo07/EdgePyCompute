import axios from "axios";
import WebSocket from "ws"; // Using ws client

const HTTP_ORCH = "http://orchestrator:3000";
const ORCHESTRATOR = "ws://orchestrator:3000";
const STORAGE = "http://localhost:9000";

const NUMMAPPERS = 5;
const NUMREDUCERS = 5;

// Code
const codeMapWordcount = `
from pyedgecompute import deserialize_input_string, serialize_partition
from collections import Counter
def task(bytes):
    text = deserialize_input_string(bytes)
    words = text.split()
    counter = Counter(words)
    return serialize_partition(counter)
`;

const codeReduceWordcount = `
from pyedgecompute import deserialize_partitions
from collections import Counter
def task(bytes_list):
    parts = deserialize_partitions(bytes_list)
    final_counter = Counter()
    for p in parts:
       final_counter.update(p)
    return json.dumps(final_counter)
`;

const codeMapTerasort = `
from pyedgecompute import deserialize_input_terasort, partition_data, serialize_partition
def task(bytes):
    parsed_data = deserialize_input_terasort(bytes)
    partitioned_data = partition_data(
        data=parsed_data,
        num_partitions=${NUMREDUCERS}
    )
    serialized_partitions = [serialize_partition(p) for p in partitioned_data]
    return serialized_partitions
`;

const codeReduceTerasort = `
from pyedgecompute import deserialize_partitions, concat_partitions, sort_dataframe, serialize_partition
def task(bytes):
    parts = deserialize_partitions(byte_list)
    concatenated_data = concat_partitions(parts)
    sorted_data = sort_dataframe(concatenated_data)
    return serialize_partition(sorted_data)
`;

const code = "";

const codeWordcount = [codeMapWordcount, codeReduceWordcount];
const codeTerasort = [codeMapTerasort, codeReduceTerasort];

// Parameters
const args = [
  `${STORAGE}/test/example1.txt`,
  //`${STORAGE}/test/example2.txt`,
  //`${STORAGE}/test/example3.txt`,
  //`${STORAGE}/test/example4.txt`,
];

const argsMapReduceWordcount = [
  [`${STORAGE}/test/example1.txt`, NUMMAPPERS, NUMREDUCERS],
  //`${STORAGE}/test/example2.txt`,
  //`${STORAGE}/test/example3.txt`,
  //`${STORAGE}/test/example4.txt`,
];

const argsMapReduceTerasort = [
  [`${STORAGE}/test/terasort-20m`, NUMMAPPERS, NUMREDUCERS],
  //`${STORAGE}/test/example2.txt`,
  //`${STORAGE}/test/example3.txt`,
  //`${STORAGE}/test/example4.txt`,
];

const typeNoneTask = "nonetask";
const typeWordcount = "mapreducewordcount";
const typeTerasort = "mapreduceterasort";

const noneTask = {
  code,
  args,
  type: typeNoneTask,
};

const mapReduceWordcount = {
  code: codeWordcount,
  args: argsMapReduceWordcount,
  type: typeWordcount,
};

const mapReduceTerasort = {
  code: codeTerasort,
  args: argsMapReduceTerasort,
  type: typeTerasort,
};

const maxTasksWordcount = argsMapReduceWordcount.length;
const maxTasksTerasort = argsMapReduceTerasort.length;
const maxTasks = maxTasksTerasort; // Change to maxTasksWordcount for wordcount
let tasksExecuted = 0;
let ws;

async function start() {
  try {
    const res = await axios.post(
      `${HTTP_ORCH}/register_task`,
      mapReduceTerasort
    );

    const taskId = res.data.task_id;
    console.log("🚀 Task submitted. ID:", taskId);
    ws = new WebSocket(`${ORCHESTRATOR}?task_id=${taskId}`);

    ws.on("open", () =>
      console.log("🔌 Connected to ORCHESTRATOR via WebSocket. TASKID:", taskId)
    );

    ws.on("message", (data) => {
      tasksExecuted++;

      try {
        const { arg, status, result } = JSON.parse(data.toString());
        console.log(
          `📦 Task ${taskId}:[${arg}] executed. Status: ${status}. Result: ${result}`
        );

        if (tasksExecuted >= maxTasks) {
          console.log("✅ All tasks executed.");
        }
      } catch (err) {
        console.error(
          "❌ Error parsing message from ORCHESTRATOR:",
          err.message
        );
      }
    });

    ws.on("close", () => {
      console.log(
        "🔌 WebSocket connection to ORCHESTRATOR closed. TASKID:",
        taskId
      );
    });

    ws.on("error", (err) => {
      console.error("❌ WebSocket ORCHESTRATOR error:", err.message);
    });
  } catch (err) {
    if (err.response) {
      console.error(
        "❌ Error response from ORCHESTRATOR server:",
        err.response.data || err.message
      );
    } else if (err.request) {
      console.error(
        "❌ No response received from the ORCHESTRATOR server. The server may be down."
      );
    } else {
      console.error("❌ Error:", err.message);
    }
  }
}

start();
