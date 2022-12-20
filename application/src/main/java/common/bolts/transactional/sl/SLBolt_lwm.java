package common.bolts.transactional.sl;

import combo.SINKCombo;
import components.context.TopologyContext;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.impl.ordered.TxnManagerLWM;

import java.util.Map;

/**
 * Combine Read-Write for nocc.
 */
public class SLBolt_lwm extends SLBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(SLBolt_lwm.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public SLBolt_lwm(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public SLBolt_lwm(int fid) {
        super(LOG, fid, null);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLWM(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), this.context.getStageMap().get(this.fid));
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
    }
}
