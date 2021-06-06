package common.bolts.transactional.sl;

import common.param.TxnEvent;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import common.sink.SINKCombo;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import transaction.dedicated.ordered.TxnManagerTStream;
import transaction.function.Condition;
import transaction.function.DEC;
import transaction.function.INC;
import transaction.impl.TxnContext;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;

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
        int numberOfStates = 10 * config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches") * 5;
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id,
                numberOfStates, this.context.getThisComponent().getNumTasks());
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
//        MeasureTools.BEGIN_INDEX_TIME_MEASURE(txnContext.thread_Id);

        String[] srcTable = new String[]{"accounts", "bookEntries"};
        String[] srcID = new String[]{event.getSourceAccountId(), event.getSourceBookEntryId()};

        DEC decrement1 = new DEC(event.getAccountTransfer());
        Condition condition1 = new Condition(event.getMinAccountBalance(), event.getAccountTransfer(), event.getBookEntryTransfer());
        DEC decrement2 = new DEC(event.getBookEntryTransfer());
        Condition condition2 = new Condition(event.getMinAccountBalance(), event.getAccountTransfer(), event.getBookEntryTransfer());
        INC increment1 = new INC(event.getAccountTransfer());
        Condition condition3 = new Condition(event.getMinAccountBalance(), event.getAccountTransfer(), event.getBookEntryTransfer());
        INC increment2 = new INC(event.getBookEntryTransfer());
        Condition condition4 = new Condition(event.getMinAccountBalance(), event.getAccountTransfer(), event.getBookEntryTransfer());

//        MeasureTools.END_INDEX_TIME_MEASURE_ACC(txnContext.thread_Id, false);

        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "accounts",
                event.getSourceAccountId(), event.src_account_value,//to be fill up.
                decrement1,
                srcTable, srcID,//condition source, condition id.
                condition1,
                event.success);          //asynchronously return.

        transactionManager.Asy_ModifyRecord(txnContext,
                "bookEntries", event.getSourceBookEntryId()
                , decrement2,
                srcTable, srcID,
                condition2,
                event.success);   //asynchronously return.

        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "accounts",
                event.getTargetAccountId(), event.dst_account_value,//to be fill up.
                increment1,
                srcTable, srcID//condition source, condition id.
                , condition3,
                event.success);          //asynchronously return.

        transactionManager.Asy_ModifyRecord(txnContext,
                "bookEntries",
                event.getTargetBookEntryId(),
                increment2,
                srcTable, srcID,
                condition4,
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
