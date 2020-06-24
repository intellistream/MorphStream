package sesame.components.windowing;

/**
 * The callback_bolt fired by {@link TriggerPolicy} when the trigger
 * condition is satisfied.
 */
interface TriggerHandler {
    /**
     * The code to execute when the {@link TriggerPolicy} condition is satisfied.
     *
     * @return true if the window was evaluated with at least one input_event in the window, false otherwise
     */
    boolean onTrigger();
}
