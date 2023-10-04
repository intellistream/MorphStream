package intellistream.morphstream.engine.stream.components.operators.api.bolt;

import intellistream.morphstream.engine.stream.components.operators.api.Operator;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.JumboTuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.TxnManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;

public abstract class AbstractTransactionalBolt extends AbstractBolt {
    private static final long serialVersionUID = -3899457584889441657L;
    protected int thread_Id;//Order of the thread in this operator
    protected int tthread;
    public TxnManager transactionManager;
    public AbstractTransactionalBolt(Logger log, int fid) {
        super(log, fid);
    }
}
