// Nodo de la cola
class TaskNode {
  constructor(task) {
    this.task = task;
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

  /** Add task to the end (enqueue) */
  push(taskOrTasks) {
    const tasks = Array.isArray(taskOrTasks) ? taskOrTasks : [taskOrTasks];

    for (const task of tasks) {
      const node = new TaskNode(task);
      this.taskMap.set(task.taskId, node);

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

  /** Remove task from front (dequeue) */
  shift() {
    if (!this.head) return null;

    const node = this.head;
    this.taskMap.delete(node.task.taskId);

    this.head = this.head.next;
    if (this.head) this.head.prev = null;
    else this.tail = null;

    this.taskCount--;

    return node.task;
  }

  /** Remove a task by taskId in O(1) */
  remove(taskId) {
    const node = this.taskMap.get(taskId);
    if (!node) return false;

    if (node.prev) node.prev.next = node.next;
    else this.head = node.next;

    if (node.next) node.next.prev = node.prev;
    else this.tail = node.prev;

    this.taskMap.delete(taskId);
    this.taskCount--;
    return true;
  }

  /** Get the total number of pending tasks */
  size() {
    return this.taskCount;
  }

  /** Check if the queue is empty */
  isEmpty() {
    return this.head === null;
  }

  /** Peek the first task */
  peek() {
    return this.head?.task ?? null;
  }
}

const instance = new TaskQueue();
export default instance;
