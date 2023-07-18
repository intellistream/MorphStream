package engine.stream.components.windowing;

/**
 * Context information that can be used by the eviction policy
 */
public interface EvictionContext {
    /**
     * Returns the reference time that the eviction policy could use to
     * evict the events. In the case of input_event time processing, this would be
     * the watermark time.
     *
     * @return the reference time in millis
     */
    Long getReferenceTime();

    /**
     * Returns the sliding count for count based windows
     *
     * @return the sliding count
     */
    Long getSlidingCount();

    /**
     * Returns the sliding interval for time based windows
     *
     * @return the sliding interval
     */
    Long getSlidingInterval();

    /**
     * Returns the current count of events in the queue up to the reference time
     * based on which count based evictions can be performed.
     *
     * @return the current count
     */
    Long getCurrentCount();
}
