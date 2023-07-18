package engine.stream.components.windowing;

import engine.stream.execution.runtime.tuple.impl.Tuple;

/**
 * A {@link Window} that contains {@link Tuple} objects.
 */
public interface TupleWindow extends Window<Tuple> {
}
