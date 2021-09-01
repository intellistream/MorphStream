package common.bolts.transactional.tp;

import combo.SINKCombo;
import common.param.lr.LREvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.context.TxnContext;
import transaction.impl.ordered.TxnManagerSStore;

import java.util.Map;

import static common.CONTROL.enable_log;
import static profiler.MeasureTools.*;

/**
 * Combine Read-Write for TStream.
 */
public class TPBolt_SSTORE extends TPBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_SSTORE.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public TPBolt_SSTORE(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public TPBolt_SSTORE(int fid) {
        super(LOG, fid, null);

    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        context.getGraph().topology.tableinitilizer.loadDB(thread_Id, context.getGraph().topology.spinlock,
                this.context.getNUMTasks());
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerSStore(db.getStorageManager(),
                this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

    @Override
    protected void LAL_PROCESS(long _bid) throws DatabaseException {
        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
        LREvent event = (LREvent) input_event;
        int _pid = (event).getPid();
        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        //ensures that locks are added in the input_event sequence order.
        LA_LOCK(_pid, 1, transactionManager, _bid, tthread);
        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        LAL(event, _bid, _bid);
        END_LOCK_TIME_MEASURE(thread_Id);
        LA_UNLOCKALL(transactionManager, tthread);
        END_WAIT_TIME_MEASURE(thread_Id);
        if (enable_log) LOG.trace(thread_Id + " finished event " + _bid + " with pid of: " + _pid);
    }
}
