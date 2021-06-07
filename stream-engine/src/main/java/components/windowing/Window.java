package components.windowing;

import java.util.Iterator;
import java.util.List;

/**
 * A view of events in a sliding window.
 *
 * @param <T> the type of input_event that this window contains. E.g. {@link org.apache.storm.tuple.Tuple}
 */
interface Window<T> {
    /**
     * Gets the list of events in the window.
     * <p>
     * <b>Note: </b> If the number of tuples in windows is huge, invoking {@code GetAndUpdate} would
     * Prepared all the tuples into memory and may throw an OOM exception. Use windowing with persistence
     * ( ) and  to retrieve an iterator over the events in the window.
     * </p>
     *
     * @return the list of events in the window.
     */
    List<T> get();

    /**
     * Returns an iterator over the events in the window.
     * <p>
     * <b>Note: </b> This is only supported when using windowing with persistence .
     * </p>
     *
     * @return an {@link Iterator} over the events in the current window.
     * @throws UnsupportedOperationException if not using
     */
    default Iterator<T> getIter() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Get the list of newly added events in the window since the last time the window was generated.
     * <p>
     * <b>Note: </b> This is not supported when using windowing with persistence ( ).
     * </p>
     *
     * @return the list of newly added events in the window.
     * @throws UnsupportedOperationException if using
     */
    List<T> getNew();

    /**
     * Get the list of events expired from the window since the last time the window was generated.
     * <p>
     * <b>Note: </b> This is not supported when using windowing with persistence ( ).
     * </p>
     *
     * @return the list of events expired from the window.
     * @throws UnsupportedOperationException if using
     */
    List<T> getExpired();

    /**
     * If processing based on input_event time, returns the window end time based on watermark otherwise
     * returns the window end time based on processing time.
     *
     * @return the window end timestamp
     */
    Long getEndTimestamp();

    /**
     * Returns the window start timestamp. Will return null if the window length is not based on time duration.
     *
     * @return the window start timestamp or null if the window length is not time based
     */
    Long getStartTimestamp();
}
