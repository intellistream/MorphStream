package engine.stream.components.windowing;

import engine.stream.execution.runtime.tuple.impl.Tuple;

public interface TimestampExtractor {
    long extractTimestamp(Tuple tuple);
}
