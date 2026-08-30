/**
 * Warm worker pool for Hillsborough transform execution.
 * Eliminates OS subprocess fork thrashing by reusing persistent worker processes.
 *
 * @module scripts/hillsborough/transform-pool
 */

import { fork } from "node:child_process";
import os from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "../..");
const WORKER_PATH = resolve(__dirname, "transform-worker.cjs");
const NODE_MODULES = resolve(ROOT, "node_modules");

/**
 * @typedef {object} WorkerState
 * @property {import("node:child_process").ChildProcess} process
 * @property {boolean} busy
 * @property {number} id
 */

/**
 * @typedef {object} PendingTask
 * @property {number} id
 * @property {string} parcelDir
 * @property {(value: void) => void} resolve
 * @property {(reason: Error) => void} reject
 */

export class TransformPool {
  /**
   * @param {number} [size]
   */
  constructor(size) {
    const totalCpus = os.cpus().length || 4;
    this.size = size || Math.max(2, Math.min(totalCpus - 1, 8));
    /** @type {WorkerState[]} */
    this.workers = [];
    /** @type {PendingTask[]} */
    this.queue = [];
    /** @type {Map<number, PendingTask>} */
    this.inFlight = new Map();
    this.taskIdCounter = 0;
    this.closed = false;

    this.initWorkers();
  }

  initWorkers() {
    for (let i = 0; i < this.size; i += 1) {
      this.spawnWorker(i);
    }
  }

  /**
   * @param {number} workerIndex
   */
  spawnWorker(workerIndex) {
    const child = fork(WORKER_PATH, [], {
      env: {
        ...process.env,
        NODE_PATH: NODE_MODULES,
      },
      stdio: ["ignore", "ignore", "inherit", "ipc"],
    });

    /** @type {WorkerState} */
    const state = {
      process: child,
      busy: false,
      id: workerIndex,
    };

    child.on("message", (msg) => {
      const { id, ok, error } = /** @type {{ id: number; ok: boolean; error?: string }} */ (msg);
      const task = this.inFlight.get(id);
      if (task) {
        this.inFlight.delete(id);
        state.busy = false;
        if (ok) {
          task.resolve();
        } else {
          task.reject(new Error(error || "Transform worker failed"));
        }
      }
      this.drainQueue();
    });

    child.on("exit", (code) => {
      if (this.closed) return;
      console.warn(`Transform worker ${workerIndex} exited with code ${code}. Respawning...`);
      state.busy = false;
      this.workers = this.workers.filter((w) => w.id !== workerIndex);
      this.spawnWorker(workerIndex);
      this.drainQueue();
    });

    this.workers.push(state);
  }

  drainQueue() {
    if (this.closed || this.queue.length === 0) return;
    const idleWorker = this.workers.find((w) => !w.busy && w.process.connected);
    if (!idleWorker) return;

    const task = this.queue.shift();
    if (!task) return;

    idleWorker.busy = true;
    this.inFlight.set(task.id, task);
    idleWorker.process.send({ id: task.id, parcelDir: task.parcelDir });
  }

  /**
   * Execute transform scripts for a parcel directory using a warm worker.
   * @param {string} parcelDir
   * @returns {Promise<void>}
   */
  run(parcelDir) {
    if (this.closed) {
      return Promise.reject(new Error("Transform pool is closed"));
    }
    return new Promise((res, rej) => {
      const id = ++this.taskIdCounter;
      this.queue.push({
        id,
        parcelDir,
        resolve: res,
        reject: rej,
      });
      this.drainQueue();
    });
  }

  /**
   * Close all worker processes in the pool.
   */
  close() {
    this.closed = true;
    for (const worker of this.workers) {
      try {
        worker.process.kill();
      } catch {}
    }
    this.workers = [];
    this.queue = [];
  }
}
