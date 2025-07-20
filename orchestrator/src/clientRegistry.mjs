class ClientRegistry {
  constructor() {
    if (ClientRegistry.instance) return ClientRegistry.instance;
    this.clients = new Map(); // clientId -> { ws, numTasks, tasks: Map }
    ClientRegistry.instance = this;
  }

  registerClient(clientId, ws, numTasks) {
    this.clients.set(clientId, {
      ws,
      numTasks,
      tasks: new Map(), // taskId -> { code, args, type, state, assignedWorkers: Map }
    });
  }

  removeClient(clientId) {
    this.clients.delete(clientId);
  }

  getAllClients() {
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
    return this.clients.get(clientId);
  }

  getClientTasks(clientId) {
    const client = this.clients.get(clientId);
    return Array.from(client.tasks.entries()).map(([taskId, info]) => ({
      taskId,
      clientId,
    }));
  }

  addTask(clientId, task) {
    const client = this.clients.get(clientId);
    client.numTasks++;
    client.tasks.set(task.taskId, {
      code: task.code,
      arg: task.arg,
      type: task.type,
      state: "pending",
      assignedWorkers: new Map(), // workerId -> timestamp
    });
  }

  markTaskRunning(clientId, taskId, workerId) {
    const task = this.clients.get(clientId).tasks.get(taskId);
    if (!task) return;
    task.state = "running";
    task.assignedWorkers.set(workerId, Date.now());
  }

  markTaskDone(clientId, taskId) {
    const task = this.clients.get(clientId).tasks.get(taskId);
    if (task) task.state = "done";
  }

  markTaskError(clientId, taskId, errorMsg) {
    const task = this.clients.get(clientId).tasks.get(taskId);
    if (!task) return;
    task.state = "error";
    task.error = errorMsg;
  }

  getClientStatus(clientId) {
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
    const client = this.clients.get(clientId);
    return client.tasks.get(taskId);
  }

  // Puedes añadir más métodos: cleanup, reassignStaleTasks, deleteClient, etc.
}

const instance = new ClientRegistry();
export default instance;
