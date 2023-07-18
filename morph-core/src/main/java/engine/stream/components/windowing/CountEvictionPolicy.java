package engine.stream.components.windowing;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An eviction policy that tracks input_event counts and can
 * evict based on a threshold count.
 *
 * @param <T> the type of input_event tracked by this policy.
 */
public class CountEvictionPolicy<T> implements EvictionPolicy<T, Long> {
    private final int threshold;
    private final AtomicLong currentCount;
    private EvictionContext context;

    public CountEvictionPolicy(int count) {
        this.threshold = count;
        this.currentCount = new AtomicLong();
    }

    @Override
    public Action evict(Event<T> event) {
        /*
         * atomically decrement the count if its greater than threshold and
         * return if the input_event should be evicted
         */
        while (true) {
            long curVal = currentCount.get();
            if (curVal > threshold) {
                if (currentCount.compareAndSet(curVal, curVal - 1)) {
                    return Action.EXPIRE;
                }
            } else {
                break;
            }
        }
        return Action.PROCESS;
    }

    @Override
    public void track(Event<T> event) {
        if (!event.isWatermark()) {
            currentCount.incrementAndGet();
        }
    }

    @Override
    public EvictionContext getContext() {
        return context;
    }

    @Override
    public void setContext(EvictionContext context) {
        this.context = context;
    }

    @Override
    public String toString() {
        return "CountEvictionPolicy{" +
                "threshold=" + threshold +
                ", currentCount=" + currentCount +
                '}';
    }

    @Override
    public void reset() {
        // NOOP
    }

    @Override
    public Long getState() {
        return currentCount.get();
    }

    @Override
    public void restoreState(Long state) {
        currentCount.set(state);
    }
}
