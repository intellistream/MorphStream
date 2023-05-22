package common.bolts.transactional.ibwj;

import combo.SINKCombo;
import db.DatabaseException;
import execution.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.impl.ordered.TxnManagerOrderLockBlocking;

public class IBWJBolt_olb extends IBWJBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(IBWJBolt_olb.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public IBWJBolt_olb(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public IBWJBolt_olb(int fid) {
        super(LOG, fid, null);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerOrderLockBlocking(db.getStorageManager(),
                this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), transactionManager.stage);
    }
}
