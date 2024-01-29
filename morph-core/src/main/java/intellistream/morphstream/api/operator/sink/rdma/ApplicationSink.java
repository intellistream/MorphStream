package intellistream.morphstream.api.operator.sink.rdma;

import intellistream.morphstream.api.input.FunctionMessage;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;

public class ApplicationSink extends AbstractSink {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSink.class);
    public ApplicationSink(String id, int fid) {
        super(id, LOG, fid);
    }
    public int i = 0;

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {
        Result result = (Result) in.getValue(0);
        try {
            MorphStreamEnv.get().rdmaWorkerManager().sendResults(this.thread_Id, new FunctionMessage(result.toString()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
