package intellistream.morphstream.api.operator.sink.rdma;

import intellistream.morphstream.api.input.FunctionMessage;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.RdmaWorkerManager;
import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BrokenBarrierException;

public class ApplicationSink extends AbstractSink {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSink.class);
    protected RdmaWorkerManager sender;
    public ApplicationSink(String id, int fid) {
        super(id, LOG, fid);
    }
    public int i = 0;

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {
        Result result = (Result) in.getValue(0);
        try {
            sender.send(new FunctionMessage(result.toString()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setSender(RdmaWorkerManager sender) {
        this.sender = sender;
    }
}
