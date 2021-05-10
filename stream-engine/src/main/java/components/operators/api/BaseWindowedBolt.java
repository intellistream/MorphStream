package components.operators.api;
import common.collections.Configuration;
import org.slf4j.Logger;
import components.windowing.TimestampExtractor;
import components.windowing.TupleFieldTimestampExtractor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
public abstract class BaseWindowedBolt extends AbstractWindowedBolt {
    private static final long serialVersionUID = 8521068118350597004L;
    private final transient Map<String, Object> windowConfiguration;
    private TimestampExtractor timestampExtractor;
    protected BaseWindowedBolt(double w) {
        super(0, w);
        windowConfiguration = new HashMap<>();
    }
    public BaseWindowedBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, double branch_selectivity, double read_selectivity, boolean byP, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, branch_selectivity, read_selectivity, byP, event_frequency, w);
        windowConfiguration = new HashMap<>();
    }
    private BaseWindowedBolt withWindowLength(Count count) {
        if (count.value <= 0) {
            throw new IllegalArgumentException("Window length must be positive [" + count + "]");
        }
        windowConfiguration.put(Configuration.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, count.value);
        return this;
    }
    private BaseWindowedBolt withWindowLength(Duration duration) {
        if (duration.value <= 0) {
            throw new IllegalArgumentException("Window length must be positive [" + duration + "]");
        }
        windowConfiguration.put(Configuration.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, duration.value);
        return this;
    }
    private BaseWindowedBolt withSlidingInterval(Count count) {
        if (count.value <= 0) {
            throw new IllegalArgumentException("Sliding interval must be positive [" + count + "]");
        }
        windowConfiguration.put(Configuration.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT, count.value);
        return this;
    }
    private BaseWindowedBolt withSlidingInterval(Duration duration) {
        if (duration.value <= 0) {
            throw new IllegalArgumentException("Sliding interval must be positive [" + duration + "]");
        }
        windowConfiguration.put(Configuration.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, duration.value);
        return this;
    }
    /**
     * JumboTuple count based sliding window configuration.
     *
     * @param windowLength    the number of tuples in the window
     * @param slidingInterval the number of tuples after which the window slides
     */
    public BaseWindowedBolt withWindow(Count windowLength, Count slidingInterval) {
        return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
    }
    /**
     * JumboTuple count and time duration based sliding window configuration.
     *
     * @param windowLength    the number of tuples in the window
     * @param slidingInterval the time duration after which the window slides
     */
    public BaseWindowedBolt withWindow(Count windowLength, Duration slidingInterval) {
        return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
    }
    /**
     * Time duration and count based sliding window configuration.
     *
     * @param windowLength    the time duration of the window
     * @param slidingInterval the number of tuples after which the window slides
     */
    public BaseWindowedBolt withWindow(Duration windowLength, Count slidingInterval) {
        return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
    }
    /**
     * Time duration based sliding window configuration.
     *
     * @param windowLength    the time duration of the window
     * @param slidingInterval the time duration after which the window slides
     */
    public BaseWindowedBolt withWindow(Duration windowLength, Duration slidingInterval) {
        return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
    }
    /**
     * A tuple count based window that slides with every incoming tuple.
     *
     * @param windowLength the number of tuples in the window
     */
    public BaseWindowedBolt withWindow(Count windowLength) {
        return withWindowLength(windowLength).withSlidingInterval(new Count(1));
    }
    /**
     * A time duration based window that slides with every incoming tuple.
     *
     * @param windowLength the time duration of the window
     */
    public BaseWindowedBolt withWindow(Duration windowLength) {
        return withWindowLength(windowLength).withSlidingInterval(new Count(1));
    }
    /**
     * A count based tumbling window.
     *
     * @param count the number of tuples after which the window tumbles
     */
    public BaseWindowedBolt withTumblingWindow(Count count) {
        return withWindowLength(count).withSlidingInterval(count);
    }
    /**
     * A time duration based tumbling window.
     *
     * @param duration the time duration after which the window tumbles
     */
    public BaseWindowedBolt withTumblingWindow(Duration duration) {
        return withWindowLength(duration).withSlidingInterval(duration);
    }
    /**
     * Specify a field in the tuple that represents the timestamp as a long value_list. If this
     * field is not present in the incoming tuple, an {@link IllegalArgumentException} will be thrown.
     *
     * @param fieldName the name of the field that contains the timestamp
     */
    public BaseWindowedBolt withTimestampField(String fieldName) {
        return withTimestampExtractor(TupleFieldTimestampExtractor.of(fieldName));
    }
    /**
     * Specify the timestamp extractor implementation.
     *
     * @param timestampExtractor the {@link TimestampExtractor} implementation
     */
    private BaseWindowedBolt withTimestampExtractor(TimestampExtractor timestampExtractor) {
        if (this.timestampExtractor != null) {
            throw new IllegalArgumentException("Window is already configured with a timestamp extractor: " + timestampExtractor);
        }
        this.timestampExtractor = timestampExtractor;
        return this;
    }
    public TimestampExtractor getTimestampExtractor() {
        return timestampExtractor;
    }
    /**
     * Specify a stream id on which late tuples are going to be emitted. They are going to be accessible via the
     * {@link org.apache.storm.topology.WindowedBoltExecutor#LATE_TUPLE_FIELD} field.
     * It must be defined on a per-component basis, and in conjunction with the
     * {@link BaseWindowedBolt#withTimestampField}, otherwise {@link IllegalArgumentException} will be thrown.
     *
     * @param streamId the name of the stream used to emit late tuples on
     */
    public BaseWindowedBolt withLateTupleStream(String streamId) {
        windowConfiguration.put(Configuration.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM, streamId);
        return this;
    }
    /**
     * Specify the maximum time lag of the tuple timestamp in milliseconds. It means that the tuple timestamps
     * cannot be out of order by more than this amount.
     *
     * @param duration the max lag duration
     */
    public BaseWindowedBolt withLag(Duration duration) {
        windowConfiguration.put(Configuration.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS, duration.value);
        return this;
    }
    /**
     * Specify the watermark input_event generation interval. For tuple based timestamps, watermark events
     * are used to track the progress of time
     *
     * @param interval the interval at which watermark events are generated
     */
    public BaseWindowedBolt withWatermarkInterval(Duration interval) {
        windowConfiguration.put(Configuration.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS, interval.value);
        return this;
    }
    /**
     * Holds a count value_list for count based windows and sliding intervals.
     */
    public static class Count {
        public final int value;
        public Count(int value) {
            this.value = value;
        }
        /**
         * Returns a {@link Count} of given value_list.
         *
         * @param value the count value_list
         * @return the Count
         */
        public static Count of(int value) {
            return new Count(value);
        }
        @Override
        public String toString() {
            return "Count{" +
                    "value_list=" + value +
                    '}';
        }
    }
    /**
     * Holds a Time duration for time based windows and sliding intervals.
     */
    public static class Duration {
        public final int value;
        public Duration(int value, TimeUnit timeUnit) {
            this.value = (int) timeUnit.toMillis(value);
        }
        /**
         * Returns a {@link Duration} corresponding to the the given value_list in milli seconds.
         *
         * @param milliseconds the duration in milliseconds
         * @return the Duration
         */
        public static Duration of(int milliseconds) {
            return new Duration(milliseconds, TimeUnit.MILLISECONDS);
        }
        /**
         * Returns a {@link Duration} corresponding to the the given value_list in days.
         *
         * @param days the number of days
         * @return the Duration
         */
        public static Duration days(int days) {
            return new Duration(days, TimeUnit.DAYS);
        }
        /**
         * Returns a {@link Duration} corresponding to the the given value_list in hours.
         *
         * @param hours the number of hours
         * @return the Duration
         */
        public static Duration hours(int hours) {
            return new Duration(hours, TimeUnit.HOURS);
        }
        /**
         * Returns a {@link Duration} corresponding to the the given value_list in minutes.
         *
         * @param minutes the number of minutes
         * @return the Duration
         */
        public static Duration minutes(int minutes) {
            return new Duration(minutes, TimeUnit.MINUTES);
        }
        /**
         * Returns a {@link Duration} corresponding to the the given value_list in seconds.
         *
         * @param seconds the number of seconds
         * @return the Duration
         */
        public static Duration seconds(int seconds) {
            return new Duration(seconds, TimeUnit.SECONDS);
        }
        @Override
        public String toString() {
            return "Duration{" +
                    "value_list=" + value +
                    '}';
        }
    }
//	public Map<String, Object> getComponentConfiguration() {
//		return windowConfiguration;
//	}
}
