package intellistream.morphstream.api.operator;

import intellistream.morphstream.engine.stream.components.operators.api.BaseSink;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;

public class ApplicationSink extends BaseSink {
    public ApplicationSink(Logger log) {
        super(log);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {

    }

    @Override
    protected Logger getLogger() {
        return null;
    }
}
