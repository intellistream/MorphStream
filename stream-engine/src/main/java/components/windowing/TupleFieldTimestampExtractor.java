package components.windowing;

import execution.runtime.tuple.impl.Tuple;

/**
 * A {@link org.apache.storm.windowing.TimestampExtractor} that extracts timestamp from a specific field in the tuple.
 */
public final class TupleFieldTimestampExtractor implements TimestampExtractor {
    private final String fieldName;

    private TupleFieldTimestampExtractor(String fieldName) {
        this.fieldName = fieldName;
    }

    public static TupleFieldTimestampExtractor of(String fieldName) {
        return new TupleFieldTimestampExtractor(fieldName);
    }

    @Override
    public long extractTimestamp(Tuple tuple) {
        return tuple.getLongByField(fieldName);
    }

    @Override
    public String toString() {
        return "TupleFieldTimestampExtractor{" +
                "fieldName='" + fieldName + '\'' +
                '}';
    }
}
