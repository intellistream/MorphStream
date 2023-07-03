package common.bolts.transactional.ob;

import combo.SINKCombo;
import components.context.TopologyContext;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.impl.ordered.TxnManagerLWM;

import java.util.Map;

public class OBBolt_lwm extends OBBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(OBBolt_lwm.class);

    public OBBolt_lwm(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public OBBolt_lwm(int fid) {
        super(LOG, fid, null);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        sink.configPrefix = this.getConfigPrefix();
        sink.prepare(config, context, collector);
        transactionManager = new TxnManagerLWM(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
    }
}
