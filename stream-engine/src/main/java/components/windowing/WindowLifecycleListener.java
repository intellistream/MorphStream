package components.windowing;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public interface WindowLifecycleListener<T> {
    /**
     * Called on expiry of events from the window due to {@link EvictionPolicy}
     *
     * @param events the expired events
     */
    void onExpiry(List<T> events);

    /**
     * Called on activation of the window due to the {@link TriggerPolicy}
     *
     * @param events        the list of current events in the window.
     * @param newEvents     the newly added events since last activation.
     * @param expired       the expired events since last activation.
     * @param referenceTime the reference (input_event or processing) time that resulted in activation
     */
    default void onActivation(List<T> events, List<T> newEvents, List<T> expired, Long referenceTime) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Called on activation of the window due to the {@link TriggerPolicy}. This is typically invoked when
     * the windows are persisted in state and is huge to be loaded entirely in memory.
     *
     * @param eventsIt      a supplier of iterator over the list of current events in the window
     * @param newEventsIt   a supplier of iterator over the newly added events since the last ativation
     * @param expiredIt     a supplier of iterator over the expired events since the last activation
     * @param referenceTime the reference (input_event or processing) time that resulted in activation
     */
    default void onActivation(Supplier<Iterator<T>> eventsIt, Supplier<Iterator<T>> newEventsIt, Supplier<Iterator<T>> expiredIt, Long referenceTime) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
