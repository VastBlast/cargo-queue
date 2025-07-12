import { delay, randomInt } from './helpers';

export interface Task<T, R> {
    data: T;
    promise: {
        resolve: (value: R | PromiseLike<R>) => void;
        reject: (reason?: unknown) => void;
    }
}

export interface CargoQueueOptions<T, R> {
    function: (tasks: Task<T, R>[]) => Promise<R>;
    max_tasks_per_cargo?: number;
    wait_time_ms?: number;
    concurrency?: number;
}

export class CargoQueue<T, R> {
    private tasks: Task<T, R>[];
    private process_promises: Record<number, Promise<void>>;

    // options
    private function: (tasks: Task<T, R>[]) => Promise<R>;
    private max_tasks_per_cargo: number;
    private wait_time_ms: number;
    private concurrency: number;

    constructor(options: CargoQueueOptions<T, R>) {
        // options
        this.function = options.function;
        this.max_tasks_per_cargo = options.max_tasks_per_cargo || 10;
        this.wait_time_ms = options.wait_time_ms ?? 100;
        this.concurrency = options.concurrency || 1;

        this.tasks = [];
        this.process_promises = {};
    }

    async run(data: T): Promise<R> {
        const promise = new Promise<R>((resolve, reject) => {
            const task: Task<T, R> = { data, promise: { resolve, reject } };
            this.tasks.push(task);
        });

        const tryProcess = async (): Promise<void> => {
            if (Object.keys(this.process_promises).length < this.concurrency) {
                let id: number;
                do {
                    id = randomInt(0, Number.MAX_SAFE_INTEGER);
                } while (this.process_promises[id]); // make sure id is unique

                //this.process_promises[id] = this.process(id);
                const processPromise = this.process();
                this.process_promises[id] = processPromise;
                processPromise.finally(() => {
                    delete this.process_promises[id];
                });
            } else {
                await Promise.race(Object.values(this.process_promises));
                tryProcess();
            }
        };

        if (this.tasks.length === 1 || (this.tasks.length > 0 && this.tasks.length % this.max_tasks_per_cargo === 0)) {
            await delay(this.wait_time_ms);
            tryProcess();
        }

        return await promise;
    }

    private async process(): Promise<void> {
        const tasks: Task<T, R>[] = [];
        const length = Math.min(this.max_tasks_per_cargo, this.tasks.length);
        for (let i = 0; i < length; i++) {
            const task = this.tasks.shift();
            if (!task) break;
            tasks.push(task);
        }

        if (tasks.length > 0) {
            try {
                const out = await this.function(tasks);
                for (let i = 0; i < tasks.length; i++) {
                    tasks[i].promise.resolve(out);
                }
            } catch (e) {
                for (let i = 0; i < tasks.length; i++) {
                    tasks[i].promise.reject(e);
                }
            }
        }
    }
}
