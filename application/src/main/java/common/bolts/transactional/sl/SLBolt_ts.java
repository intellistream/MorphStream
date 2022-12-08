package common.bolts.transactional.sl;

import combo.SINKCombo;
import common.param.TxnEvent;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import storage.SchemaRecord;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.DEC;
import transaction.function.INC;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE_ACC;

public class SLBolt_ts extends SLBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SLBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    //write-compute time pre-measured.
    ArrayDeque<TransactionEvent> transactionEvents;
    ArrayDeque<DepositEvent> depositEvents;

    public SLBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public SLBolt_ts(int fid) {
        super(LOG, fid, null);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        int numberOfStates = config.getInt("NUM_ITEMS");
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id,
                numberOfStates, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BFS"));
        transactionEvents = new ArrayDeque<>();
        depositEvents = new ArrayDeque<>();
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(transactionManager.getSchedulerContext(),
                context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
        // Aqif: For TStream taskId increases by 1 and executorId is always 0.
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (in.isMarker()) {
            int transSize = transactionEvents.size();
            int depoSize = depositEvents.size();
            int num_events = transSize + depoSize;
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    TRANSFER_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    TRANSFER_REQUEST_POST();
                    DEPOSITE_REQUEST_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);

                //all tuples in the holder is finished.
                transactionEvents.clear();
                depositEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);
        } else {
            execute_ts_normal(in);
        }
    }

    protected void PRE_TXN_PROCESS(double _bid, long timestamp) throws DatabaseException, InterruptedException {
        MeasureTools.BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
//            System.out.println("thread: "+thread_Id+", event_id: "+_bid);
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TxnEvent event = (TxnEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            if (event instanceof DepositEvent) {
                DEPOSITE_REQUEST_CONSTRUCT((DepositEvent) event, txnContext);
            } else if (event instanceof TransactionEvent) {
                TRANSFER_REQUEST_CONSTRUCT((TransactionEvent) event, txnContext);
            } else
                throw new UnknownError();
            MeasureTools.END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);
        }
    }

    protected void TRANSFER_REQUEST_CONSTRUCT(TransactionEvent event, TxnContext txnContext) throws DatabaseException {

        String[] accTable = new String[]{"accounts"};
        String[] astTable = new String[]{"bookEntries"};
        String[] accID = new String[]{event.getSourceAccountId()};
        String[] astID = new String[]{event.getSourceBookEntryId()};

        DEC decrement1 = new DEC(event.getAccountTransfer());
        Condition condition1 = new Condition(event.getMinAccountBalance(), event.getAccountTransfer());
        DEC decrement2 = new DEC(event.getBookEntryTransfer());
        Condition condition2 = new Condition(event.getMinAccountBalance(), event.getBookEntryTransfer());
        INC increment1 = new INC(event.getAccountTransfer());
        Condition condition3 = new Condition(event.getMinAccountBalance(), event.getAccountTransfer());
        INC increment2 = new INC(event.getBookEntryTransfer());
        Condition condition4 = new Condition(event.getMinAccountBalance(), event.getBookEntryTransfer());

        transactionManager.BeginTransaction(txnContext);
        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "accounts",
                event.getSourceAccountId(), event.src_account_value,//to be fill up.
                decrement1,
                accTable, accID,//condition source, condition id.
                condition1,
                event.success,
                "sl");          //asynchronously return.

        transactionManager.Asy_ModifyRecord(txnContext,
                "bookEntries", event.getSourceBookEntryId()
                , decrement2,
                astTable, astID,
                condition2,
                event.success,
                "sl");   //asynchronously return.

        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "accounts",
                event.getTargetAccountId(), event.dst_account_value,//to be fill up.
                increment1,
                accTable, accID//condition source, condition id.
                , condition3,
                event.success,
                "sl");          //asynchronously return.

        transactionManager.Asy_ModifyRecord(txnContext,
                "bookEntries",
                event.getTargetBookEntryId(),
                increment2,
                astTable, astID,
                condition4,
                event.success,
                "sl");   //asynchronously return.

        transactionManager.CommitTransaction(txnContext);

        transactionEvents.add(event);
    }

    protected void DEPOSITE_REQUEST_CONSTRUCT(DepositEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        //it simply construct the operations and return.
        transactionManager.BeginTransaction(txnContext);
        transactionManager.Asy_ModifyRecord(txnContext, "accounts", event.getAccountId(), new INC(event.getAccountTransfer()), "sl");// read and modify the account itself.
        transactionManager.Asy_ModifyRecord(txnContext, "bookEntries", event.getBookEntryId(), new INC(event.getBookEntryTransfer()), "sl");// read and modify the asset itself.
        transactionManager.CommitTransaction(txnContext);

        depositEvents.add(event);
    }

    private void DEPOSITE_REQUEST_POST() throws InterruptedException {
        for (DepositEvent event : depositEvents) {
            DEPOSITE_REQUEST_POST(event);
        }
    }

    private void TRANSFER_REQUEST_POST() throws InterruptedException {
        for (TransactionEvent event : transactionEvents) {
            TRANSFER_REQUEST_POST(event);
        }
    }

    private void TRANSFER_REQUEST_CORE() throws InterruptedException {
        for (TransactionEvent event : transactionEvents) {

            SchemaRecord srcAccountValueRecord = event.src_account_value.getRecord();
            SchemaRecord dstAccountValueRecord = event.dst_account_value.getRecord();

            if (srcAccountValueRecord == null) {
                if (enable_log) LOG.debug(event.getBid() + " | " + event.getSourceAccountId());
            }
            if (dstAccountValueRecord == null) {
                if (enable_log) LOG.debug(event.getBid() + " | " + event.getTargetAccountId());
            }

            if (srcAccountValueRecord != null && dstAccountValueRecord != null)
                event.transaction_result = new TransactionResult(event, event.success[0] == 4,
                        srcAccountValueRecord.getValues().get(1).getLong(),
                        dstAccountValueRecord.getValues().get(1).getLong());
        }
    }
}
