package intellistream.morphstream.examples.tsp.grepsumwindow.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.grepsumwindow.events.GSWTxnEvent;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.impl.TxnManagerNoLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static intellistream.morphstream.engine.txn.profiler.MeasureTools.BEGIN_ACCESS_TIME_MEASURE;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.END_ACCESS_TIME_MEASURE_ACC;

/**
 * Combine Read-Write for nocc.
 */
public class GSWBolt_nocc extends GSWBolt_Locks {
    private static final Logger LOG = LoggerFactory.getLogger(GSWBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public GSWBolt_nocc(int fid, SINKCombo sink) {
        super(fid, sink);
    }

    //constructor without compose everything together.
    public GSWBolt_nocc(int fid) {
        super(fid, null);
    }

    @Override
    protected void write_txn_process(GSWTxnEvent event, long i, long _bid) throws DatabaseException, InterruptedException {
        write_request(event, txn_context[(int) (i - _bid)]);//always success
        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        WRITE_CORE(event);
        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
//        transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
    }

    @Override
    protected void read_txn_process(GSWTxnEvent event, long i, long _bid) throws DatabaseException, InterruptedException {
        read_request(event, txn_context[(int) (i - _bid)]);//always success..
        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        READ_CORE(event);
        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
//        transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }
}
