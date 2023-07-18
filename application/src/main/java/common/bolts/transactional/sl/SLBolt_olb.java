package common.bolts.transactional.sl;

import combo.SINKCombo;
import engine.stream.components.context.TopologyContext;
import engine.stream.execution.ExecutionGraph;
import engine.stream.execution.runtime.collector.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import engine.txn.transaction.impl.ordered.TxnManagerOrderLockBlocking;

import java.util.Map;

/**
 * Combine Read-Write for nocc.
 */
public class SLBolt_olb extends SLBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(SLBolt_olb.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public SLBolt_olb(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public SLBolt_olb(int fid) {
        super(LOG, fid, null);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerOrderLockBlocking(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
    }
}
