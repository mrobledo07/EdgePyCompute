export class Stopwatch {
  constructor() {
    this.startTime = null;
    this.endTime = null;
    this.running = false;
  }

  start() {
    if (this.running) {
      console.warn("⏱ Stopwatch is already running.");
      return;
    }
    this.running = true;
    this.startTime = performance.now();
    console.log("🟢 Stopwatch started");
  }

  stop() {
    if (!this.running) {
      console.warn("🛑 Stopwatch is not running.");
      return;
    }
    this.endTime = performance.now();
    this.running = false;
    console.log("🔴 Stopwatch stopped");
  }

  getDuration() {
    if (this.running) {
      return (performance.now() - this.startTime) / 1000; // seconds
    } else if (this.startTime && this.endTime) {
      return (this.endTime - this.startTime) / 1000;
    } else {
      return 0;
    }
  }
}
