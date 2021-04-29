package components.windowing;
public interface TriggerPolicy<T, S> {
    /**
     * Tracks the input_event and could use this to invoke the trigger.
     *
     * @param event the input input_event
     */
    void track(Event<T> event);
    /**
     * resets the trigger policy.
     */
    void reset();
    /**
     * Starts the trigger policy. This can be used
     * during recovery to start the triggers after
     * recovery is complete.
     */
    void start();
    /**
     * Any relax_reset up could be handled here.
     */
    void shutdown();
    /**
     * Return runtime state to be checkpointed by the framework for restoring the trigger policy
     * in case of failures.
     *
     * @return the state
     */
    S getState();
    /**
     * Restore the trigger policy from the state that was earlier checkpointed by the framework.
     *
     * @param state the state
     */
    void restoreState(S state);
}
