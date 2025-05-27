export class Semaphore {
  private permits: number;
  private waiting: Array<() => void> = [];

  constructor(permits: number) {
    this.permits = permits;
  }

  async acquire(): Promise<void> {
    return new Promise((resolve) => {
      if (this.permits > 0) {
        this.permits--;
        resolve();
      } else {
        this.waiting.push(resolve);
      }
    });
  }

  release(): void {
    this.permits++;
    if (this.waiting.length > 0) {
      const resolve = this.waiting.shift()!;
      this.permits--;
      resolve();
    }
  }

  get availablePermits(): number {
    return this.permits;
  }

  get waitingCount(): number {
    return this.waiting.length;
  }
}

export class AsyncQueue<T, R = void> {
  private queue: T[] = [];
  private processing = false;
  private processor: (item: T) => Promise<R>;
  private concurrency: number;
  private running = 0;

  constructor(processor: (item: T) => Promise<R>, concurrency: number = 1) {
    this.processor = processor;
    this.concurrency = concurrency;
  }

  async add(item: T): Promise<void> {
    this.queue.push(item);
    this.process();
  }

  async addBatch(items: T[]): Promise<void> {
    this.queue.push(...items);
    this.process();
  }

  async processItem(item: T): Promise<R> {
    return this.processor(item);
  }

  private async process(): Promise<void> {
    if (this.running >= this.concurrency) {
      return;
    }

    while (this.queue.length > 0 && this.running < this.concurrency) {
      const item = this.queue.shift()!;
      this.running++;

      this.processor(item)
        .catch((error) => {
          console.error("Queue processor error:", error);
        })
        .finally(() => {
          this.running--;
          this.process(); // Process next item
        });
    }
  }

  get queueLength(): number {
    return this.queue.length;
  }

  get activeCount(): number {
    return this.running;
  }
}
