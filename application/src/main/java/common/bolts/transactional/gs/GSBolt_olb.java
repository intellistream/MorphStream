package common.bolts.transactional.gs;

import common.sink.SINKCombo;
import execution.ExecutionGraph;
import faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.impl.ordered.TxnManagerOrderLockBlocking;

public class GSBolt_olb extends GSBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(GSBolt_olb.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public GSBolt_olb(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
        state = new ValueState();
    }

    public GSBolt_olb(int fid) {
        super(LOG, fid, null);
        state = new ValueState();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerOrderLockBlocking(db.getStorageManager(),
                this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }
}
