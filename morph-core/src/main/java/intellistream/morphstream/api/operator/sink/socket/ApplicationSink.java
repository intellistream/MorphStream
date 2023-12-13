package intellistream.morphstream.api.operator.sink.socket;

import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;

public class ApplicationSink extends AbstractSink {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSink.class);
    protected ZMQ.Socket sender;
    public ApplicationSink(String id, int fid) {
        super(id, LOG, fid);
    }
    public int i = 0;

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {
        ZMsg msg = (ZMsg) in.getValue(0);
        Result result = (Result) in.getValue(1);
        ZFrame address = msg.pop();
        msg.destroy();
        address.send(sender, ZFrame.REUSE + ZFrame.MORE);
        ZFrame content = new ZFrame(result.toString());
        content.send(sender, ZFrame.REUSE);
        address.destroy();
        content.destroy();
    }
    public void setSender(ZMQ.Socket sender) {
        this.sender = sender;
    }

}
