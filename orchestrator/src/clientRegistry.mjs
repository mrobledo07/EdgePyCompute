class ClientRegistry {
  constructor() {
    if (ClientRegistry.instance) return ClientRegistry.instance;
    this.clients = new Map(); // clientId -> { ws, numTasks, tasks: Map }
    this.isLocked = false;
    ClientRegistry.instance = this;
  }

  lock() {
    if (this.isLocked) throw new Error("ClientRegistry is locked.");
    this.isLocked = true;
  }

  unlock() {
    this.isLocked = false;
  }

  registerClient(clientId, ws, numTasks) {
    this.lock();
    try {
      this.clients.set(clientId, {
        ws,
        numTasks,
        tasks: new Map(),
      });
    } finally {
      this.unlock();
    }
  }

  removeClient(clientId) {
    this.lock();
    try {
      this.clients.delete(clientId);
    } finally {
      this.unlock();
    }
  }

  getAllClients() {
    if (this.isLocked) throw new Error("ClientRegistry is locked.");
    return Array.from(this.clients.entries()).map(([clientId, client]) => ({
      clientId,
      numTasks: client.numTasks,
      tasks: Array.from(client.tasks.entries()).map(([taskId, info]) => ({
        taskId,
        code: info.code,
        args: info.arg,
        type: info.type,
        state: info.state,
        assignedWorkers: Array.from(info.assignedWorkers.keys()),
        error: info.error || null,
      })),
    }));
  }

  getClient(clientId) {
    if (this.isLocked) throw new Error("ClientRegistry is locked.");
    return this.clients.get(clientId);
  }

  getClientTasks(clientId) {
    if (this.isLocked) throw new Error("ClientRegistry is locked.");
    const client = this.clients.get(clientId);
    return Array.from(client.tasks.entries()).map(([taskId, info]) => ({
      taskId,
      clientId,
    }));
  }

  addTask(clientId, task) {
    this.lock();
    try {
      const client = this.clients.get(clientId);
      client.numTasks++;
      client.tasks.set(task.taskId, {
        code: task.code,
        arg: task.arg,
        type: task.type,
        state: "pending",
        assignedWorkers: new Map(),
      });
    } finally {
      this.unlock();
    }
  }

  markTaskRunning(clientId, taskId, workerId) {
    this.lock();
    try {
      const task = this.clients.get(clientId).tasks.get(taskId);
      if (!task) return;
      task.state = "running";
      task.assignedWorkers.set(workerId, Date.now());
    } finally {
      this.unlock();
    }
  }

  markTaskDone(clientId, taskId) {
    this.lock();
    try {
      const task = this.clients.get(clientId).tasks.get(taskId);
      if (task) task.state = "done";
    } finally {
      this.unlock();
    }
  }

  markTaskError(clientId, taskId, errorMsg) {
    this.lock();
    try {
      const task = this.clients.get(clientId).tasks.get(taskId);
      if (!task) return;
      task.state = "error";
      task.error = errorMsg;
    } finally {
      this.unlock();
    }
  }

  getClientStatus(clientId) {
    if (this.isLocked) throw new Error("ClientRegistry is locked.");
    const client = this.clients.get(clientId);
    return {
      numTasks: client.numTasks,
      tasks: Array.from(client.tasks.entries()).map(([taskId, info]) => ({
        taskId,
        state: info.state,
        assignedWorkers: Array.from(info.assignedWorkers.keys()),
        error: info.error || null,
      })),
    };
  }

  getClientTask(clientId, taskId) {
    if (this.isLocked) throw new Error("ClientRegistry is locked.");
    const client = this.clients.get(clientId);
    return client.tasks.get(taskId);
  }
}

const instance = new ClientRegistry();
export default instance;
