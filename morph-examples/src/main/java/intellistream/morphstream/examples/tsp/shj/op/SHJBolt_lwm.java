package intellistream.morphstream.examples.tsp.shj.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.TxnManagerLWM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SHJBolt_lwm extends SHJBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(SHJBolt_lwm.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public SHJBolt_lwm(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public SHJBolt_lwm(int fid) {
        super(LOG, fid, null);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLWM(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }
}