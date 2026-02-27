class InMemoryQueue {
  constructor({ worker, concurrency = 1 }) {
    this.worker = worker;
    this.concurrency = concurrency;
    this.running = 0;
    this.queue = [];
    this.stopped = false;
  }

  push(job) {
    if (this.stopped) return Promise.reject(new Error('Queue stopped'));
    return new Promise((resolve, reject) => {
      this.queue.push({ job, resolve, reject });
      this._drain();
    });
  }

  async _runOne(item) {
    this.running += 1;
    try {
      const result = await this.worker(item.job);
      item.resolve(result);
    } catch (error) {
      item.reject(error);
    } finally {
      this.running -= 1;
      this._drain();
    }
  }

  _drain() {
    while (!this.stopped && this.running < this.concurrency && this.queue.length > 0) {
      const item = this.queue.shift();
      this._runOne(item);
    }
  }

  async shutdown() {
    this.stopped = true;
    while (this.running > 0 || this.queue.length > 0) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }
}

module.exports = { InMemoryQueue };
