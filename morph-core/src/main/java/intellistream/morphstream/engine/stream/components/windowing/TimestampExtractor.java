package intellistream.morphstream.engine.stream.components.windowing;

import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;

public interface TimestampExtractor {
    long extractTimestamp(Tuple tuple);
}
