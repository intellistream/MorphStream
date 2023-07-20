package intellistream.morphstream.examples.tsp.tollprocessing.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.TxnManagerLWM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Combine Read-Write for TStream.
 */
public class TPBolt_lwm extends TPBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_lwm.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public TPBolt_lwm(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public TPBolt_lwm(int fid) {
        super(LOG, fid, null);

    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID()
                , context.getGraph());
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLWM(db.getStorageManager(),
                this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }
}
