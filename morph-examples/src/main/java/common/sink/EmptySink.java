package common.sink;

import intellistream.morphstream.engine.stream.components.operators.api.BaseSink;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.JumboTuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmptySink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(EmptySink.class);
    private static final long serialVersionUID = -2429018860900290157L;

    public EmptySink() {
        super(LOG);
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
    }
}
