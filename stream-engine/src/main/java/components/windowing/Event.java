package components.windowing;

/**
 * An input_event is a wrapper object that gets stored in the window.
 *
 * @param <T> the type of the object thats wrapped. E.g Tuple
 */
public interface Event<T> {
    /**
     * The input_event timestamp in millis. This could be the time
     * when the source generated the tuple or the time
     * when the tuple was received by a bolt.
     *
     * @return the input_event timestamp in milliseconds.
     */
    long getTimestamp();

    /**
     * Returns the wrapped object, E.g. a tuple
     *
     * @return the wrapped object.
     */
    T get();

    /**
     * If this is a watermark input_event or not. Watermark events are used
     * for tracking time while processing input_event based ts.
     *
     * @return true if this is a watermark input_event
     */
    boolean isWatermark();
}
