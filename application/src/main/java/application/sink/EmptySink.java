package application.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.operators.api.BaseSink;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Tuple;

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
