package java.intellistream.chc.database.manage;

import java.intellistream.chc.database.manage.handle.Handle;
import java.util.concurrent.*;

/**
 * ThreadPoolService manages the worker threads
 */
public class ThreadPoolService {
    private final ConcurrentHashMap<Long, WorkerThread> threadMap;
    private final int threadPoolSize;
    private int threadPointer = 0;

    public ThreadPoolService(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
        this.threadMap = new ConcurrentHashMap<>();
        // initialize worker threads
        for (int i = 0; i < threadPoolSize; i++) {
            WorkerThread thread = new WorkerThread();
            this.threadMap.put(thread.getId(), thread);
        }
    }

    /**
     * Pick a thread in a round-robin manner
     * @return thread id
     */
    public long pickSingleThread() {
        long threadId = this.threadMap.keySet().toArray(new Long[0])[this.threadPointer];
        this.threadPointer = (this.threadPointer + 1) % this.threadPoolSize;
        return threadId;
    }

    /**
     * Submit the task to a thread
     * @param task task to be submitted
     * @param toThreadId thread id to submit the task to
     */
    public void submit(Handle task, long toThreadId) {
        this.threadMap.get(toThreadId).submit(task);
    }

    /**
     * WorkerThread polls the task from the task queue and executes it
     */
    private static class WorkerThread extends Thread {
        private final BlockingQueue<Runnable> taskQueue;

        public void submit(Runnable task) {
            this.taskQueue.add(task);
        }

        public WorkerThread() {
            this.setName("WorkerThread-" + this.getId());
            this.taskQueue = new LinkedBlockingQueue<>();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Runnable task = this.taskQueue.take();
                    task.run();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
