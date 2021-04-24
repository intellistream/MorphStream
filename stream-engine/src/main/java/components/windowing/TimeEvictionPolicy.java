package components.windowing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Eviction policy that evicts events based on time duration.
 */
public class TimeEvictionPolicy<T> implements EvictionPolicy<T, EvictionContext> {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.storm.windowing.TimeEvictionPolicy.class);
    private final int windowLength;
    private volatile EvictionContext evictionContext;
    private long delta;
    /**
     * Constructs a TimeEvictionPolicy that evicts events older
     * than the given window length in millis
     *
     * @param windowLength the duration in milliseconds
     */
    public TimeEvictionPolicy(int windowLength) {
        this.windowLength = windowLength;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public Action evict(Event<T> event) {
        long now = evictionContext == null ? System.currentTimeMillis() : evictionContext.getReferenceTime();
        long diff = now - event.getTimestamp();
        if (diff >= (windowLength + delta)) {
            return Action.EXPIRE;
        } else if (diff < 0) { // do not process events beyond current ts
            return Action.KEEP;
        }
        return Action.PROCESS;
    }
    @Override
    public void track(Event<T> event) {
        // NOOP
    }
    @Override
    public EvictionContext getContext() {
        return evictionContext;
    }
    @Override
    public void setContext(EvictionContext context) {
        EvictionContext prevContext = evictionContext;
        evictionContext = context;
        // compute window length adjustment (delta_long) to account for time drift
        if (context.getSlidingInterval() != null) {
            if (prevContext == null) {
                delta = Integer.MAX_VALUE; // consider all events for the set_executor_ready window
            } else {
                delta = context.getReferenceTime() - prevContext.getReferenceTime() - context.getSlidingInterval();
                if (Math.abs(delta) > 100) {
                    LOG.warn("Possible clock drift or long running computation in window; " +
                                    "Previous eviction time: {}, current eviction time: {}",
                            prevContext.getReferenceTime(),
                            context.getReferenceTime());
                }
            }
        }
    }
    @Override
    public void reset() {
        // NOOP
    }
    @Override
    public EvictionContext getState() {
        return evictionContext;
    }
    @Override
    public void restoreState(EvictionContext state) {
        this.evictionContext = state;
    }
    @Override
    public String toString() {
        return "TimeEvictionPolicy{" +
                "windowLength=" + windowLength +
                ", evictionContext=" + evictionContext +
                '}';
    }
}
