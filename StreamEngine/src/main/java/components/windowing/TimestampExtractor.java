package components.windowing;
import execution.runtime.tuple.impl.Tuple;
public interface TimestampExtractor {
    long extractTimestamp(Tuple tuple);
}
