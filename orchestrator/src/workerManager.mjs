// workers/manager.js

import { workers, setWorkers } from "./state.mjs";

/**
 * Sorts the workers in-place by number of available processors (descending).
 */
export function sortWorkers() {
  workers.sort((a, b) => b.availableWorkers - a.availableWorkers);
}

/**
 * Returns an array of available worker references,
 * duplicating workers based on how many processors they have available.
 */
export function getAvailableWorkerSlots() {
  let available = [];
  let globalWorkerIndex = 0;

  for (const worker of workers) {
    if (worker.availableWorkers === 0) break;
    for (let i = 0; i < worker.availableWorkers; i++) {
      worker.worker_num = globalWorkerIndex++;
      available.push(worker);
    }
  }

  return available;
}

/**
 * Registers a new worker and adds it to the global workers list.
 * @param {number} numWorkers Number of logical processors
 * @returns {object} New worker object
 */
export function registerWorker(worker_id, numWorkers = 1) {
  const newWorker = {
    worker_id,
    ws: null, // WebSocket connection will be added later
    maxWorkers: numWorkers,
    availableWorkers: numWorkers,
    tasksAssignated: [],
  };

  workers.push(newWorker);
  sortWorkers(); // Keep list sorted

  return newWorker;
}

/**
 * Finds a worker by its ID.
 */
export function findWorkerById(worker_id) {
  return workers.find((w) => w.worker_id === worker_id);
}

/**
 * Removes a worker from the list (e.g. on disconnection).
 */
export function removeWorker(worker_id) {
  const remaining = workers.filter((w) => w.worker_id !== worker_id);
  setWorkers(remaining);
  sortWorkers();
}
