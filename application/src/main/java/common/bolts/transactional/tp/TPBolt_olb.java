package common.bolts.transactional.tp;

import combo.SINKCombo;
import components.context.TopologyContext;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.impl.ordered.TxnManagerOrderLockBlocking;

import java.util.Map;

/**
 * Combine Read-Write for TStream.
 */
public class TPBolt_olb extends TPBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_olb.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public TPBolt_olb(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
        state = new ValueState();
    }

    public TPBolt_olb(int fid) {
        super(LOG, fid, null);
        state = new ValueState();
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID()
                , context.getGraph());
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerOrderLockBlocking(db.getStorageManager(),
                this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }
}
