package intellistream.morphstream.api.operator.bolt;

import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractSStoreBolt;
import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.FunctionDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;

public class SStoreBolt extends AbstractSStoreBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SStoreBolt.class);
    public AbstractSink sink;//If combo is enabled, we need to define a sink for the bolt
    public boolean isCombo = false;

    public SStoreBolt(String id, HashMap<String, FunctionDescription> txnDescriptionHashMap, int fid) {
        super(id, LOG, fid);
    }
    public SStoreBolt(String id, HashMap<String, FunctionDescription> txnDescriptionHashMap, int fid, AbstractSink sink) {
        super(id, LOG, fid);
        this.sink = sink;
        this.isCombo = true;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {

    }

}
