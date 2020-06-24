package common.bolts.transactional.tp;
import common.sink.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.context.TopologyContext;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.collector.OutputCollector;
import sesame.faulttolerance.impl.ValueState;
import state_engine.transaction.dedicated.ordered.TxnManagerLWM;

import java.util.Map;
/**
 * Combine Read-Write for TStream.
 */
public class TPBolt_lwm extends TPBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_lwm.class);
    private static final long serialVersionUID = -5968750340131744744L;
    public TPBolt_lwm(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
        state = new ValueState();
    }
    public TPBolt_lwm(int fid) {
        super(LOG, fid, null);
        state = new ValueState();
    }
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID()
                , context.getThisTaskId(), context.getGraph());
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLWM(db.getStorageManager(),
                this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }
}
