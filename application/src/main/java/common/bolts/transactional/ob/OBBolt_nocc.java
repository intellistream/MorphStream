package common.bolts.transactional.ob;

import combo.SINKCombo;
import common.param.ob.AlertEvent;
import common.param.ob.BuyingEvent;
import common.param.ob.ToppingEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.impl.TxnManagerNoLock;

import java.util.Map;

import static common.CONTROL.combo_bid_size;
import static profiler.MeasureTools.BEGIN_ACCESS_TIME_MEASURE;
import static profiler.MeasureTools.END_ACCESS_TIME_MEASURE_ACC;

public class OBBolt_nocc extends OBBolt {
    private static final Logger LOG = LoggerFactory.getLogger(OBBolt_nocc.class);

    public OBBolt_nocc(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), this.context.getStageMap().get(this.fid));
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        nocc_execute(in);
    }

    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            if (input_event instanceof BuyingEvent) {
                buying_txn_process((BuyingEvent) input_event, i, _bid);
            } else if (input_event instanceof AlertEvent) {
                alert_txn_process((AlertEvent) input_event, i, _bid);
            } else {
                topping_txn_process((ToppingEvent) input_event, i, _bid);
            }
        }
    }

    private void topping_txn_process(ToppingEvent input_event, double i, double _bid) throws DatabaseException, InterruptedException {
        TOPPING_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success
        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        TOPPING_REQUEST_CORE(input_event);
        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }

    private void alert_txn_process(AlertEvent input_event, double i, double _bid) throws DatabaseException, InterruptedException {
        ALERT_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success
        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        ALERT_REQUEST_CORE(input_event);
        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }

    private void buying_txn_process(BuyingEvent input_event, double i, double _bid) throws DatabaseException, InterruptedException {
        BUYING_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success
        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        BUYING_REQUEST_CORE(input_event);
        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }
}
