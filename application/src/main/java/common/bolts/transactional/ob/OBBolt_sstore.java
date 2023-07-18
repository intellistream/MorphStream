package common.bolts.transactional.ob;

import combo.SINKCombo;
import engine.txn.TxnEvent;
import engine.stream.components.context.TopologyContext;
import engine.txn.db.DatabaseException;
import engine.stream.execution.ExecutionGraph;
import engine.stream.execution.runtime.collector.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import engine.txn.transaction.context.TxnContext;
import engine.txn.transaction.impl.ordered.TxnManagerSStore;

import java.util.Map;

import static common.CONTROL.enable_log;
import static common.CONTROL.enable_states_partition;
import static engine.txn.profiler.MeasureTools.*;

public class OBBolt_sstore extends OBBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(OBBolt_sstore.class);

    public OBBolt_sstore(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public OBBolt_sstore(int fid) {
        super(LOG, fid, null);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerSStore(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
        if (!enable_states_partition) {
            if (enable_log) LOG.info("Please enable `enable_states_partition` for PAT scheme");
            System.exit(-1);
        }
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
//        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().GetAndUpdate(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
        context.getGraph().topology.tableinitilizer.loadDB(thread_Id, context.getGraph().topology.spinlock,
                this.context.getNUMTasks());
    }

    @Override
    protected void LAL_PROCESS(long _bid) throws DatabaseException {
        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
        TxnEvent event = (TxnEvent) input_event;
        int _pid = event.getPid();
        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        //ensures that locks are added in the input_event sequence order.
        LA_LOCK(_pid, event.num_p(), transactionManager, event.getBid_array(), _bid, tthread);
        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        LAL(event, _bid, _bid);
        END_LOCK_TIME_MEASURE(thread_Id);
        LA_UNLOCK(_pid, event.num_p(), transactionManager, tthread);
        END_WAIT_TIME_MEASURE(thread_Id);
    }
}
