package intellistream.morphstream.examples.tsp.grepsumwindow.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.TxnManagerOrderLockBlocking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GSWBolt_olb extends GSWBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(GSWBolt_olb.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public GSWBolt_olb(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public GSWBolt_olb(int fid) {
        super(LOG, fid, null);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerOrderLockBlocking(db.getStorageManager(),
                this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }
}