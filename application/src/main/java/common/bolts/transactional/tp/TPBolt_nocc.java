package common.bolts.transactional.tp;

import common.param.lr.LREvent;
import combo.SINKCombo;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.dedicated.TxnManagerNoLock;

import java.util.Map;

import static common.CONTROL.combo_bid_size;
import static profiler.MeasureTools.BEGIN_ACCESS_TIME_MEASURE;
import static profiler.MeasureTools.END_ACCESS_TIME_MEASURE_ACC;

/**
 * Combine Read-Write for TStream.
 */
public class TPBolt_nocc extends TPBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public TPBolt_nocc(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
        state = new ValueState();
    }

    public TPBolt_nocc(int fid) {
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
        transactionManager = new TxnManagerNoLock(db.getStorageManager(),
                this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        nocc_execute(in);
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            txn_process((LREvent) input_event, i, _bid);
        }
    }

    private void txn_process(LREvent input_event, long i, long _bid) throws DatabaseException, InterruptedException {
        TXN_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success
        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        TXN_REQUEST_CORE(input_event);
        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }
}
