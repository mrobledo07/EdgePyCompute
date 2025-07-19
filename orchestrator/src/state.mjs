// shared/state.js

// Clients connected via WebSocket, keyed by task ID
export const taskClients = new Map();

// Registered workers (sorted externally when needed)
export let workers = [];

// Task queue for tasks waiting for a worker
export let taskQueue = [];

// MapReduce state (number of mappers/reducers left, intermediate results, etc.)
export const mapreduceTasks = new Map();

/**
 * Allows updating the 'workers' array in-place (needed for sorting and reassigning).
 * This function is used to mutate the reference safely from external modules.
 */
export function setWorkers(newWorkers) {
  workers.length = 0;
  workers.push(...newWorkers);
}

/**
 * Allows updating the 'taskQueue' array in-place.
 */
export function setTaskQueue(newQueue) {
  taskQueue.length = 0;
  taskQueue.push(...newQueue);
}
