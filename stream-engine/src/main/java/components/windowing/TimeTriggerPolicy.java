package components.windowing;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
/**
 * Invokes {@link TriggerHandler#onTrigger()} after the duration.
 */
public class TimeTriggerPolicy<T> implements TriggerPolicy<T, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.storm.windowing.TimeTriggerPolicy.class);
    private final TriggerHandler handler;
    private final ScheduledExecutorService executor;
    private final EvictionPolicy<T, ?> evictionPolicy;
    private final long duration;
    private ScheduledFuture<?> executorFuture;
    public TimeTriggerPolicy(long millis, TriggerHandler handler) {
        this(millis, handler, null);
    }
    public TimeTriggerPolicy(long millis, TriggerHandler handler, EvictionPolicy<T, ?> evictionPolicy) {
        this.duration = millis;
        this.handler = handler;
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("time-trigger-policy-%d")
                .setDaemon(true)
                .build();
        this.executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
        this.evictionPolicy = evictionPolicy;
    }
    @Override
    public void track(Event<T> event) {
        checkFailures();
    }
    @Override
    public void reset() {
        checkFailures();
    }
    @Override
    public void start() {
        executorFuture = executor.scheduleAtFixedRate(newTriggerTask(), duration, duration, TimeUnit.MILLISECONDS);
    }
    @Override
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    @Override
    public String toString() {
        return "TimeTriggerPolicy{" +
                "duration=" + duration +
                '}';
    }
    /*
     * Check for uncaught exceptions during the execution
     * of the trigger and fail fast.
     * The uncaught exceptions will be wrapped in
     * ExecutionException and thrown when future.GetAndUpdate() is invoked.
     */
    private void checkFailures() {
        if (executorFuture != null && executorFuture.isDone()) {
            try {
                executorFuture.get();
            } catch (InterruptedException | ExecutionException ex) {
                LOG.error("Got exception ", ex);
//				throw new Exception(ex);
            }
        }
    }
    private Runnable newTriggerTask() {
        return () -> {
            // do not process current timestamp since tuples might arrive while the trigger is executing
            long now = System.currentTimeMillis() - 1;
            try {
                /*
                 * set the current timestamp as the reference time for the eviction policy
                 * to evict the events
                 */
                evictionPolicy.setContext(new DefaultEvictionContext(now, null, null, duration));
                handler.onTrigger();
            } catch (Throwable th) {
                LOG.error("handler.onTrigger failed ", th);
                /*
                 * propagate it so that Task gets canceled and the exception
                 * can be retrieved from executorFuture.GetAndUpdate()
                 */
                throw th;
            }
        };
    }
    @Override
    public Void getState() {
        return null;
    }
    @Override
    public void restoreState(Void state) {
    }
}
