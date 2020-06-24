package common.bolts.transactional.sl;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import common.sink.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.context.TopologyContext;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.collector.OutputCollector;
import sesame.execution.runtime.tuple.impl.Tuple;
import sesame.faulttolerance.impl.ValueState;
import state_engine.DatabaseException;
import state_engine.transaction.dedicated.TxnManagerNoLock;

import java.util.Map;

import static common.CONTROL.combo_bid_size;
/**
 * Combine Read-Write for nocc.
 */
public class SLBolt_nocc extends SLBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SLBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;
    public SLBolt_nocc(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
        state = new ValueState();
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        nocc_execute(in);
    }
    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            if (input_event instanceof DepositEvent) {
                depo_txn_process((DepositEvent) input_event, i, _bid);
            } else {
                trans_txn_process((TransactionEvent) input_event, i, _bid);
            }
        }
    }
    private void trans_txn_process(TransactionEvent input_event, long i, long _bid) throws DatabaseException, InterruptedException {
        TRANSFER_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success contains index time and other overhead.
        TRANSFER_REQUEST_CORE(input_event);//time to access shared states.
    }
    private void depo_txn_process(DepositEvent input_event, long i, long _bid) throws DatabaseException, InterruptedException {
        DEPOSITE_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success contains index time and other overhead.
        DEPOSITE_REQUEST_CORE(input_event);//time to access shared states.
    }
}