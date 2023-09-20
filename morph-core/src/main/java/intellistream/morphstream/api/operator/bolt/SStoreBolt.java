package intellistream.morphstream.api.operator.bolt;

import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractSStoreBolt;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;

public class SStoreBolt extends AbstractSStoreBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SStoreBolt.class);

    public SStoreBolt(HashMap<String, TxnDescription> txnDescriptionHashMap, int fid) {
        super(LOG, fid);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {

    }
}
