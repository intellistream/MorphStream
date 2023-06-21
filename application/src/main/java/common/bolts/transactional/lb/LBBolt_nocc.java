package common.bolts.transactional.lb;

import combo.SINKCombo;
import common.param.lb.LBEvent;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.impl.TxnManagerNoLock;

import static common.CONTROL.combo_bid_size;

/**
 * Combine Read-Write for nocc.
 */
public class LBBolt_nocc extends LBBolt {
    private static final Logger LOG = LoggerFactory.getLogger(LBBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public LBBolt_nocc(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    //constructor without compose everything together.
    public LBBolt_nocc(int fid) {
        super(LOG, fid, null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), this.context.getStageMap().get(this.fid));
//        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        nocc_execute(in);
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            lb_txn_process((LBEvent) input_event, i, _bid);
        }
    }

    private void lb_txn_process(LBEvent input_event, double i, double _bid) throws DatabaseException, InterruptedException {
        if (input_event.isNewConn()) {
            LB_NOCC_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success contains index time and other overhead.
            LB_CORE(input_event);//time to access shared states.
        }
    }

}
