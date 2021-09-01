package common.bolts.transactional.sl;

import combo.SINKCombo;
import common.param.TxnEvent;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.context.TxnContext;
import transaction.impl.ordered.TxnManagerSStore;

import java.util.Map;

import static profiler.MeasureTools.*;

/**
 * Combine Read-Write for nocc.
 */
public class SLBolt_sstore extends SLBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(SLBolt_sstore.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public SLBolt_sstore(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
        state = new ValueState();
    }

    public SLBolt_sstore(int fid) {
        super(LOG, fid, null);
        state = new ValueState();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerSStore(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        context.getGraph().topology.tableinitilizer.loadDB(thread_Id, context.getGraph().topology.spinlock,
                this.context.getNUMTasks());
    }

    @Override
    protected void LAL_PROCESS(long _bid) throws DatabaseException {
        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
        TxnEvent event = (TxnEvent) input_event;
        int _pid = (event).getPid();
        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        //ensures that locks are added in the input_event sequence order.
        LA_LOCK(_pid, event.num_p(), transactionManager, event.getBid_array(), _bid, tthread);
        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        if (event instanceof DepositEvent) {
            DEPOSITE_LOCK_AHEAD((DepositEvent) event, txn_context[0]);
        } else {
            TRANSFER_LOCK_AHEAD((TransactionEvent) event, txn_context[0]);
        }
        END_LOCK_TIME_MEASURE(thread_Id);
        LA_UNLOCK(_pid, event.num_p(), transactionManager, tthread);
        END_WAIT_TIME_MEASURE(thread_Id);
    }
}
