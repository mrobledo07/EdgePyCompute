// workerRegistry.mjs

class WorkerRegistry {
  constructor() {
    if (WorkerRegistry.instance) return WorkerRegistry.instance;
    this.workersById = new Map(); // worker_id => Worker object
    this.availabilityMap = new Map();
    WorkerRegistry.instance = this;
  }

  /** Añade un nuevo worker */
  addWorker(worker) {
    // worker should have: worker_id, availableWorkers, tasksAssignated
    worker.tasksAssignated = {
      map: new Map(),
      len: 0,
    };
    this.workersById.set(worker.worker_id, worker);
    this._addToAvailability(worker.worker_id, worker.availableWorkers);
  }

  /** Elimina un worker */
  removeWorker(worker_id) {
    const worker = this.workersById.get(worker_id);
    if (!worker) return false;
    this._removeFromAvailability(worker_id, worker.availableWorkers);
    this.workersById.delete(worker_id);
    return true;
  }

  /** Recupera un worker por ID en O(1) */
  getWorkerById(worker_id) {
    return this.workersById.get(worker_id) || null;
  }

  /**
   * Cambia la disponibilidad de un worker:
   * - Lo quita de la bucket antigua
   * - Actualiza su availableWorkers
   * - Lo añade a la bucket nueva
   */
  updateAvailability(worker_id, newAvailability) {
    const worker = this.workersById.get(worker_id);
    if (!worker) return false;
    this._removeFromAvailability(worker_id, worker.availableWorkers);
    worker.availableWorkers = newAvailability;
    this._addToAvailability(worker_id, newAvailability);
    return true;
  }

  /** Asigna una tarea a un worker (tarea y decremento de hilos) */
  assignTaskToWorker(worker_id, taskId, task) {
    const worker = this.workersById.get(worker_id);
    if (!worker) return false;
    worker.tasksAssignated.map.set(taskId, task);
    worker.tasksAssignated.len++;

    this.updateAvailability(worker_id, worker.availableWorkers - 1);
    return true;
  }

  /** Marca la tarea como completada (elimina del map y libera hilo) */
  completeTaskOnWorker(worker_id, taskId) {
    const worker = this.workersById.get(worker_id);
    if (!worker) return false;
    worker.tasksAssignated.map.delete(taskId);
    worker.tasksAssignated.len--;
    this.updateAvailability(worker_id, worker.availableWorkers + 1);
    return true;
  }

  /**
   * Devuelve hasta n workers con mayor disponibilidad.
   * Si n es Infinity, devuelve todos.
   */
  getBestWorkers(n) {
    const result = [];
    // Iterar keys ordenadas desc
    const keys = [...this.availabilityMap.keys()].sort((a, b) => b - a);
    for (const avail of keys) {
      const bucket = this.availabilityMap.get(avail);
      for (const workerId of bucket.workers.keys()) {
        result.push(this.workersById.get(workerId));
        if (result.length >= n) return result;
      }
    }
    return result;
  }

  /** Suma el len de todas las buckets para obtener el total disponible */
  getTotalAvailableWorkers() {
    let total = 0;
    for (const [availability, bucket] of this.availabilityMap.entries()) {
      if (Number(availability) > 0) {
        total += bucket.len;
      }
    }
    return total;
  }

  /** Interno: añade workerId a la bucket de disponibilidad */
  _addToAvailability(workerId, availKey) {
    if (!this.availabilityMap.has(availKey)) {
      this.availabilityMap.set(availKey, { workers: new Map(), len: 0 });
    }
    const bucket = this.availabilityMap.get(availKey);
    if (!bucket.workers.has(workerId)) {
      bucket.workers.set(workerId, true);
      bucket.len++;
    }
  }

  /** Interno: quita workerId de la bucket de disponibilidad */
  _removeFromAvailability(workerId, availKey) {
    const bucket = this.availabilityMap.get(availKey);
    if (!bucket) return;
    if (bucket.workers.delete(workerId)) {
      bucket.len--;
      if (bucket.len === 0) this.availabilityMap.delete(availKey);
    }
  }
}

// Exportar instancia singleton
const instance = new WorkerRegistry();
export default instance;
