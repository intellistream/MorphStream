package application.bolts.transactional.gs;


import application.sink.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.execution.ExecutionGraph;
import sesame.faulttolerance.impl.ValueState;
import state_engine.transaction.dedicated.ordered.TxnManagerLWM;

public class GSBolt_lwm extends GSBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(GSBolt_lwm.class);
    private static final long serialVersionUID = -5968750340131744744L;


    public GSBolt_lwm(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
        state = new ValueState();
    }

    public GSBolt_lwm(int fid) {
        super(LOG, fid, null);
        state = new ValueState();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        sink.configPrefix = this.getConfigPrefix();
        sink.prepare(config, context, collector);
        transactionManager = new TxnManagerLWM(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }


}
