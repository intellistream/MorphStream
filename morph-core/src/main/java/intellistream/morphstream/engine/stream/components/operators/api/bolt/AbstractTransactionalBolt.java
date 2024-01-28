package intellistream.morphstream.engine.stream.components.operators.api.bolt;

import intellistream.morphstream.engine.txn.transaction.TxnManager;
import org.slf4j.Logger;

public abstract class AbstractTransactionalBolt extends AbstractBolt {
    private static final long serialVersionUID = -3899457584889441657L;
    protected int thread_Id;//Order of the thread in this operator
    protected int tthread;
    public TxnManager transactionManager;
    public AbstractTransactionalBolt(String id, Logger log, int fid) {
        super(id, log, fid);
    }
}
