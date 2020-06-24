package application.bolts.transactional.gs;


import application.param.mb.MicroEvent;
import application.sink.SINKCombo;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.impl.Tuple;
import sesame.faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state_engine.DatabaseException;
import state_engine.transaction.dedicated.TxnManagerLock;

import static application.CONTROL.combo_bid_size;
import static state_engine.profiler.MeasureTools.*;


/**
 * Combine Read-Write for nocc.
 */
public class GSBolt_Locks extends GSBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GSBolt_Locks.class);
    private static final long serialVersionUID = -5968750340131744744L;
    public GSBolt_Locks(int fid, SINKCombo sink) {
        super(LOG, fid,sink);
        state = new ValueState();
    }

    protected void write_txn_process(MicroEvent event, long i, long _bid) throws DatabaseException, InterruptedException {
        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        boolean success = write_request(event, txn_context[(int) (i - _bid)]);
        END_LOCK_TIME_MEASURE_NOCC(thread_Id);//if success, lock-tp_core-index; if failed, lock -0-index;

        if (success) {
            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
            WRITE_CORE(event);
            END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
        } else {//being aborted.
            txn_context[(int) (i - _bid)].is_retry_ = true;
            BEGIN_ABORT_TIME_MEASURE(thread_Id);
            while (!write_request(event, txn_context[(int) (i - _bid)]) && !Thread.currentThread().isInterrupted()) ;
            END_ABORT_TIME_MEASURE_ACC(thread_Id);

            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
            WRITE_CORE(event);
            END_ACCESS_TIME_MEASURE_ACC(thread_Id);

            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
        }
    }

    protected void read_txn_process(MicroEvent event, long i, long _bid) throws DatabaseException, InterruptedException {

        boolean success = read_request(event, txn_context[(int) (i - _bid)]);

        if (success) {
            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
            READ_CORE(event);
            END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
        } else {//being aborted.
            txn_context[(int) (i - _bid)].is_retry_ = true;
            BEGIN_ABORT_TIME_MEASURE(thread_Id);
            while (!read_request(event, txn_context[(int) (i - _bid)])) ;
            END_ABORT_TIME_MEASURE_ACC(thread_Id);

            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
            READ_CORE(event);
            END_ACCESS_TIME_MEASURE_ACC(thread_Id);

            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
        }
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            MicroEvent event = (MicroEvent) input_event;
            boolean flag = event.READ_EVENT();
            if (flag) {
                read_txn_process(event, i, _bid);
            } else {
                write_txn_process(event, i, _bid);
            }
        }
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        nocc_execute(in);
    }
}
