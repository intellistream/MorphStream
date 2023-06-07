package common.bolts.transactional.lb;

import combo.SINKCombo;
import db.DatabaseException;
import execution.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.impl.TxnManagerNoLock;

/**
 * Combine Read-Write for nocc.
 */
public class LBBolt_nocc extends LBBolt_Locks {
    private static final Logger LOG = LoggerFactory.getLogger(LBBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public LBBolt_nocc(int fid, SINKCombo sink) {
        super(fid, sink);
    }

    //constructor without compose everything together.
    public LBBolt_nocc(int fid) {
        super(fid, null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), transactionManager.stage);
    }

//    @Override
//    protected void write_txn_process(IBWJEvent event, double i, double _bid) throws DatabaseException, InterruptedException {
//        write_request(event, txn_context[(int) (i - _bid)]);//always success
//        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
//        WRITE_CORE(event);
//        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
////        transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
//    }
//
//    @Override
//    protected void read_txn_process(IBWJEvent event, double i, double _bid) throws DatabaseException, InterruptedException {
//        read_request(event, txn_context[(int) (i - _bid)]);//always success..
//        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
//        READ_CORE(event);
//        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
////        transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
//    }
//

}
