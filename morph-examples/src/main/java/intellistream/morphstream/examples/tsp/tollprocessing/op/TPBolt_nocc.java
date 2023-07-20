package intellistream.morphstream.examples.tsp.tollprocessing.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.tollprocessing.events.TPTxnEvent;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.impl.TxnManagerNoLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static intellistream.morphstream.configuration.CONTROL.combo_bid_size;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.BEGIN_ACCESS_TIME_MEASURE;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.END_ACCESS_TIME_MEASURE_ACC;

/**
 * Combine Read-Write for TStream.
 */
public class TPBolt_nocc extends TPBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public TPBolt_nocc(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public TPBolt_nocc(int fid) {
        super(LOG, fid, null);

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
            txn_process((TPTxnEvent) input_event, i, _bid);
        }
    }

    private void txn_process(TPTxnEvent input_event, long i, long _bid) throws DatabaseException, InterruptedException {
        TXN_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success
        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        TXN_REQUEST_CORE(input_event);
        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }
}
