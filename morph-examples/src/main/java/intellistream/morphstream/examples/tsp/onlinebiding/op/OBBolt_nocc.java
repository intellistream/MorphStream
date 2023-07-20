package intellistream.morphstream.examples.tsp.onlinebiding.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.onlinebiding.events.AlertTxnEvent;
import intellistream.morphstream.examples.tsp.onlinebiding.events.BuyingTxnEvent;
import intellistream.morphstream.examples.tsp.onlinebiding.events.ToppingTxnEvent;
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

public class OBBolt_nocc extends OBBolt {
    private static final Logger LOG = LoggerFactory.getLogger(OBBolt_nocc.class);

    public OBBolt_nocc(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        nocc_execute(in);
    }

    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            if (input_event instanceof BuyingTxnEvent) {
                buying_txn_process((BuyingTxnEvent) input_event, i, _bid);
            } else if (input_event instanceof AlertTxnEvent) {
                alert_txn_process((AlertTxnEvent) input_event, i, _bid);
            } else {
                topping_txn_process((ToppingTxnEvent) input_event, i, _bid);
            }
        }
    }

    private void topping_txn_process(ToppingTxnEvent input_event, long i, long _bid) throws DatabaseException, InterruptedException {
        TOPPING_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success
        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        TOPPING_REQUEST_CORE(input_event);
        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }

    private void alert_txn_process(AlertTxnEvent input_event, long i, long _bid) throws DatabaseException, InterruptedException {
        ALERT_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success
        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        ALERT_REQUEST_CORE(input_event);
        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }

    private void buying_txn_process(BuyingTxnEvent input_event, long i, long _bid) throws DatabaseException, InterruptedException {
        BUYING_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success
        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        BUYING_REQUEST_CORE(input_event);
        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }
}
