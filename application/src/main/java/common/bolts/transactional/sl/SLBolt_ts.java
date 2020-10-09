package common.bolts.transactional.sl;
import common.param.TxnEvent;
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
import state_engine.transaction.dedicated.ordered.TxnManagerTStream;
import state_engine.transaction.function.Condition;
import state_engine.transaction.function.DEC;
import state_engine.transaction.function.INC;
import state_engine.transaction.impl.TxnContext;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import state_engine.profiler.MeasureTools;

import static common.CONTROL.*;
import static common.constants.StreamLedgerConstants.Constant.NUM_ACCOUNTS;

public class SLBolt_ts extends SLBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SLBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    private final static double write_useful_time = 3316;//write-compute time pre-measured.
    ArrayDeque<TransactionEvent> transactionEvents;
    private int depositeEvents;
    public SLBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
        state = new ValueState();
    }
    public SLBolt_ts(int fid) {
        super(LOG, fid, null);
        state = new ValueState();
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id,
                NUM_ACCOUNTS, this.context.getThisComponent().getNumTasks());
        transactionEvents = new ArrayDeque<>();
    }
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
        // Aqif: For TStream taskId increases by 1 and executorId is always 0.
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (in.isMarker()) {
            int readSize = transactionEvents.size();

            MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);

            MeasureTools.BEGIN_TXN_PROCESSING_TIME_MEASURE(thread_Id);
            transactionManager.start_evaluate(thread_Id, in.getBID());//start lazy evaluation in transaction manager.
            MeasureTools.END_TXN_PROCESSING_TIME_MEASURE(thread_Id);

            MeasureTools.BEGIN_ACCESS_TIME_MEASURE(thread_Id);
            TRANSFER_REQUEST_CORE();
            MeasureTools.END_ACCESS_TIME_MEASURE_TS(thread_Id, readSize, write_useful_time, depositeEvents);

            MeasureTools.END_TXN_TIME_MEASURE_TS(thread_Id, write_useful_time * depositeEvents);//overhead_total txn time

            TRANSFER_REQUEST_POST();
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, readSize + depositeEvents);

            transactionEvents.clear();//all tuples in the holder is finished.
            if (enable_profile) {
                depositeEvents = 0;//all tuples in the holder is finished.
            }
        } else {
            execute_ts_normal(in);
        }
    }
    protected void PRE_TXN_PROCESS(long _bid, long timestamp) throws DatabaseException, InterruptedException {
        MeasureTools.BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
//            System.out.println("thread: "+thread_Id+", event_id: "+_bid);
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TxnEvent event = (TxnEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            if (event instanceof DepositEvent) {
                DEPOSITE_REQUEST_CONSTRUCT((DepositEvent) event, txnContext);
            } else {
                TRANSFER_REQUEST_CONSTRUCT((TransactionEvent) event, txnContext);
            }
        }
        MeasureTools.END_PRE_TXN_TIME_MEASURE(thread_Id);//includes post time deposite..
    }
    protected void TRANSFER_REQUEST_CONSTRUCT(TransactionEvent event, TxnContext txnContext) throws DatabaseException {
//        System.out.println(event.toString());
        String[] srcTable = new String[]{"accounts", "bookEntries"};
        String[] srcID = new String[]{event.getSourceAccountId(), event.getSourceBookEntryId()};

        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "accounts",
                event.getSourceAccountId(), event.src_account_value,//to be fill up.
                new DEC(event.getAccountTransfer()),
                srcTable, srcID,//condition source, condition id.
                new Condition(event.getMinAccountBalance(), event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);          //asynchronously return.

        transactionManager.Asy_ModifyRecord(txnContext,
                "bookEntries", event.getSourceBookEntryId()
                , new DEC(event.getBookEntryTransfer()),
                srcTable, srcID,
                new Condition(event.getMinAccountBalance(), event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);   //asynchronously return.

        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "accounts",
                event.getTargetAccountId(), event.dst_account_value,//to be fill up.
                new INC(event.getAccountTransfer()),
                srcTable, srcID//condition source, condition id.
                , new Condition(event.getMinAccountBalance(), event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);          //asynchronously return.

        transactionManager.Asy_ModifyRecord(txnContext,
                "bookEntries",
                event.getTargetBookEntryId(),
                new INC(event.getBookEntryTransfer()),
                srcTable, srcID,
                new Condition(event.getMinAccountBalance(), event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);   //asynchronously return.
        transactionEvents.add(event);
    }
    protected void DEPOSITE_REQUEST_CONSTRUCT(DepositEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        //it simply construct the operations and return.
        transactionManager.Asy_ModifyRecord(txnContext, "accounts", event.getAccountId(), new INC(event.getAccountTransfer()));// read and modify the account itself.
        transactionManager.Asy_ModifyRecord(txnContext, "bookEntries", event.getBookEntryId(), new INC(event.getBookEntryTransfer()));// read and modify the asset itself.
        MeasureTools.BEGIN_POST_TIME_MEASURE(thread_Id);
        DEPOSITE_REQUEST_POST(event);
        MeasureTools.END_POST_TIME_MEASURE_ACC(thread_Id);
        depositeEvents++;
    }
    private void TRANSFER_REQUEST_POST() throws InterruptedException {
        for (TransactionEvent event : transactionEvents) {
            TRANSFER_REQUEST_POST(event);
        }
    }
    private void TRANSFER_REQUEST_CORE() throws InterruptedException {
        for (TransactionEvent event : transactionEvents) {
            event.transaction_result = new TransactionResult(event, event.success[0],
                    event.src_account_value.getRecord().getValues().get(1).getLong(), event.dst_account_value.getRecord().getValues().get(1).getLong());
        }
    }
}
