// Nodo de la cola
class TaskNode {
  constructor(taskId, clientId) {
    this.taskId = taskId;
    this.clientId = clientId;
    this.next = null;
    this.prev = null;
  }
}

class TaskQueue {
  constructor() {
    this.head = null;
    this.tail = null;
    this.taskMap = new Map(); // taskId -> TaskNode
    this.taskCount = 0;
  }

  /** Add task(s) to the end of the queue */
  push(taskOrTasks) {
    const tasks = Array.isArray(taskOrTasks) ? taskOrTasks : [taskOrTasks];

    for (const { taskId, clientId } of tasks) {
      const node = new TaskNode(taskId, clientId);
      this.taskMap.set(taskId, node);

      if (!this.tail) {
        this.head = this.tail = node;
      } else {
        this.tail.next = node;
        node.prev = this.tail;
        this.tail = node;
      }

      this.taskCount++;
    }
  }

  /** Remove and return the first task from the queue */
  shift() {
    if (!this.head) return null;

    const node = this.head;
    this.taskMap.delete(node.taskId);

    this.head = node.next;
    if (this.head) this.head.prev = null;
    else this.tail = null;

    this.taskCount--;

    return { taskId: node.taskId, clientId: node.clientId };
  }

  /** Remove a task by taskId in O(1) */
  remove(taskIds) {
    const ids = Array.isArray(taskIds) ? taskIds : [taskIds];
    let removed = false;

    for (const taskId of ids) {
      const node = this.taskMap.get(taskId);
      if (!node) continue;

      if (node.prev) node.prev.next = node.next;
      else this.head = node.next;

      if (node.next) node.next.prev = node.prev;
      else this.tail = node.prev;

      this.taskMap.delete(taskId);
      this.taskCount--;
      removed = true;
    }

    return removed;
  }

  /** Get the total number of pending tasks */
  size() {
    return this.taskCount;
  }

  /** Check if the queue is empty */
  isEmpty() {
    return this.head === null;
  }

  /** Peek the first task (without removing it) */
  peek() {
    if (!this.head) return null;
    return { taskId: this.head.taskId, clientId: this.head.clientId };
  }
}

const instance = new TaskQueue();
export default instance;
