package engine.stream.components.windowing;

/**
 * Eviction policy tracks events and decides whether
 * an input_event should be evicted from the window or not.
 *
 * @param <T> the type of input_event that is tracked.
 */
public interface EvictionPolicy<T, S> {
    /**
     * Decides if an input_event should be expired from the window, processed in the current
     * window or kept for later processing.
     *
     * @param event the input input_event
     * @return the {@link org.apache.storm.windowing.EvictionPolicy.Action} to be taken based on the input input_event
     */
    Action evict(Event<T> event);

    /**
     * Tracks the input_event to later decide whether
     * {@link EvictionPolicy#evict(Event)} should evict it or not.
     *
     * @param event the input input_event to be tracked
     */
    void track(Event<T> event);

    /**
     * Returns the current context that is part of this eviction policy.
     *
     * @return the eviction context
     */
    EvictionContext getContext();

    /**
     * Sets a context in the eviction policy that can be used while evicting the events.
     * E.g. For TimeEvictionPolicy, this could be used to set the reference timestamp.
     *
     * @param context the eviction context
     */
    void setContext(EvictionContext context);

    /**
     * Resets the eviction policy.
     */
    void reset();

    /**
     * Return runtime state to be checkpointed by the framework for restoring the eviction policy
     * in case of failures.
     *
     * @return the state
     */
    S getState();

    /**
     * Restore the eviction policy from the state that was earlier checkpointed by the framework.
     *
     * @param state the state
     */
    void restoreState(S state);

    /**
     * The action to be taken when {@link EvictionPolicy#evict(Event)} is invoked.
     */
    enum Action {
        /**
         * expire the input_event and remove it from the queue.
         */
        EXPIRE,
        /**
         * process the input_event in the current window of events.
         */
        PROCESS,
        /**
         * don't include in the current window but keep the input_event
         * in the queue for evaluating as a part of future windows.
         */
        KEEP,
        /**
         * stop processing the queue, there cannot be anymore events
         * satisfying the eviction policy.
         */
        STOP
    }
}
