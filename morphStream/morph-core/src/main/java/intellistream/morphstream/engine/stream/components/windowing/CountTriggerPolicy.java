package intellistream.morphstream.engine.stream.components.windowing;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A trigger that tracks input_event counts and calls back {@link TriggerHandler#onTrigger()}
 * when the count threshold is hit.
 *
 * @param <T> the type of input_event tracked by this policy.
 */
public class CountTriggerPolicy<T> implements TriggerPolicy<T, Integer> {
    private final int count;
    private final AtomicInteger currentCount;
    private final TriggerHandler handler;
    private final EvictionPolicy<T, ?> evictionPolicy;
    private boolean started;

    public CountTriggerPolicy(int count, TriggerHandler handler, EvictionPolicy<T, ?> evictionPolicy) {
        this.count = count;
        this.currentCount = new AtomicInteger();
        this.handler = handler;
        this.evictionPolicy = evictionPolicy;
        this.started = false;
    }

    @Override
    public void track(Event<T> event) {
        if (started && !event.isWatermark()) {
            if (currentCount.incrementAndGet() >= count) {
                evictionPolicy.setContext(new DefaultEvictionContext(System.currentTimeMillis()));
                handler.onTrigger();
            }
        }
    }

    @Override
    public void reset() {
        currentCount.set(0);
    }

    @Override
    public void start() {
        started = true;
    }

    @Override
    public void shutdown() {
        // NOOP
    }

    @Override
    public Integer getState() {
        return currentCount.get();
    }

    @Override
    public void restoreState(Integer state) {
        currentCount.set(state);
    }

    @Override
    public String toString() {
        return "CountTriggerPolicy{" +
                "count=" + count +
                ", currentCount=" + currentCount +
                ", started=" + started +
                '}';
    }
}
