package common.bolts.transactional.gsw;

import combo.SINKCombo;
import execution.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.impl.ordered.TxnManagerLWM;

public class WindowedGSBolt_lwm extends WindowedGSBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(WindowedGSBolt_lwm.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public WindowedGSBolt_lwm(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public WindowedGSBolt_lwm(int fid) {
        super(LOG, fid, null);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLWM(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }
}
