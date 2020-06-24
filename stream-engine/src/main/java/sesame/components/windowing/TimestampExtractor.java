package sesame.components.windowing;

import sesame.execution.runtime.tuple.impl.Tuple;

public interface TimestampExtractor {
    long extractTimestamp(Tuple tuple);
}
