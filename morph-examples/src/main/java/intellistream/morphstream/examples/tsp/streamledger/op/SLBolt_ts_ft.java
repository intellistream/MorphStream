package intellistream.morphstream.examples.tsp.streamledger.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.streamledger.events.DepositTxnEvent;
import intellistream.morphstream.examples.tsp.streamledger.events.TransactionTxnEvent;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.durability.logging.LoggingResult.LoggingResult;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult.SnapshotResult;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.profiler.Metrics;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.transaction.function.Condition;
import intellistream.morphstream.engine.txn.transaction.function.DEC;
import intellistream.morphstream.engine.txn.transaction.function.INC;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.TxnManagerTStream;
import intellistream.morphstream.util.FaultToleranceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.*;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.*;

public class SLBolt_ts_ft extends SLBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SLBolt_ts_ft.class);
    private static final long serialVersionUID = -5968750340131744744L;
    //write-compute time pre-measured.
    ArrayDeque<TransactionTxnEvent> transactionTxnEvents;
    ArrayDeque<DepositTxnEvent> depositTxnEvents;

    public SLBolt_ts_ft(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public SLBolt_ts_ft(int fid) {
        super(LOG, fid, null);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        int numberOfStates = config.getInt("NUM_ITEMS");
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id,
                numberOfStates, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BFS"));
        transactionTxnEvents = new ArrayDeque<>();
        depositTxnEvents = new ArrayDeque<>();
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(transactionManager.getSchedulerContext(),
                context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
        // Aqif: For TStream taskId increases by 1 and executorId is always 0.
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {

        if (in.isMarker()) {
            int transSize = transactionTxnEvents.size();
            int depoSize = depositTxnEvents.size();
            int num_events = transSize + depoSize;
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    if (Objects.equals(in.getMarker().getMessage(), "snapshot")) {
                        BEGIN_SNAPSHOT_TIME_MEASURE(this.thread_Id);
                        this.db.asyncSnapshot(in.getMarker().getSnapshotId(), this.thread_Id, this.ftManager);
                        MeasureTools.END_SNAPSHOT_TIME_MEASURE(this.thread_Id);
                    } else if (Objects.equals(in.getMarker().getMessage(), "commit") || Objects.equals(in.getMarker().getMessage(), "commit_early")) {
                        MeasureTools.BEGIN_LOGGING_TIME_MEASURE(this.thread_Id);
                        this.db.asyncCommit(in.getMarker().getSnapshotId(), this.thread_Id, this.loggingManager);
                        MeasureTools.END_LOGGING_TIME_MEASURE(this.thread_Id);
                    } else if (Objects.equals(in.getMarker().getMessage(), "commit_snapshot") || Objects.equals(in.getMarker().getMessage(), "commit_snapshot_early")) {
                        MeasureTools.BEGIN_LOGGING_TIME_MEASURE(this.thread_Id);
                        this.db.asyncCommit(in.getMarker().getSnapshotId(), this.thread_Id, this.loggingManager);
                        MeasureTools.END_LOGGING_TIME_MEASURE(this.thread_Id);
                        BEGIN_SNAPSHOT_TIME_MEASURE(this.thread_Id);
                        this.db.asyncSnapshot(in.getMarker().getSnapshotId(), this.thread_Id, this.ftManager);
                        MeasureTools.END_SNAPSHOT_TIME_MEASURE(this.thread_Id);
                    }
                    TRANSFER_REQUEST_CORE();
                }
                if (Objects.equals(in.getMarker().getMessage(), "commit_early") || Objects.equals(in.getMarker().getMessage(), "commit_snapshot_early")) {
                    this.loggingManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new LoggingResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
                }
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    TRANSFER_REQUEST_POST();
                    DEPOSITE_REQUEST_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);
                if (Objects.equals(in.getMarker().getMessage(), "snapshot")) {
                    this.ftManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new SnapshotResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
                } else if (Objects.equals(in.getMarker().getMessage(), "commit")) {
                    this.loggingManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new LoggingResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
                } else if (Objects.equals(in.getMarker().getMessage(), "commit_snapshot")) {
                    this.ftManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new SnapshotResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
                    this.loggingManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new LoggingResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
                } else if (Objects.equals(in.getMarker().getMessage(), "commit_snapshot_early")) {
                    this.ftManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new SnapshotResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
                }
                transactionTxnEvents.clear();
                depositTxnEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);
            if (this.sink.lastTask >= 0 && in.getMarker().getSnapshotId() * this.tthread >= this.sink.lastTask) {
                if (!this.sink.stopRecovery) {
                    this.sink.stopRecovery = true;
                    MeasureTools.END_RECOVERY_TIME_MEASURE(this.thread_Id);
                    MeasureTools.END_REPLAY_MEASURE(this.thread_Id);
                }
            }
            if (this.sink.stopRecovery) {
                Metrics.RecoveryPerformance.stopRecovery[thread_Id] = true;//Change here is to measure time for entire epoch.
                Metrics.RecoveryPerformance.recoveryItems[thread_Id] = this.sink.lastTask - this.sink.startRecovery;
                this.transactionManager.switch_scheduler(thread_Id, in.getBID());
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
            if (event instanceof DepositTxnEvent) {
                DEPOSITE_REQUEST_CONSTRUCT((DepositTxnEvent) event, txnContext);
            } else if (event instanceof TransactionTxnEvent) {
                TRANSFER_REQUEST_CONSTRUCT((TransactionTxnEvent) event, txnContext);
            } else
                throw new UnknownError();
            MeasureTools.END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);
        }
    }

    protected void TRANSFER_REQUEST_CONSTRUCT(TransactionTxnEvent event, TxnContext txnContext) throws DatabaseException {

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
        transactionManager.Asy_WriteRecord_Cond(txnContext,
                "accounts",
                event.getSourceAccountId(), //to be fill up.
                accTable, accID,//condition source, condition id.
                null
        );          //asynchronously return.

        transactionManager.Asy_WriteRecord_Cond(txnContext,
                "bookEntries",
                event.getSourceBookEntryId(),
                astTable, astID, null
        );   //asynchronously return.

        transactionManager.Asy_WriteRecord_Cond(txnContext,
                "accounts",
                event.getTargetAccountId(), //to be fill up.
                accTable, accID,//condition source, condition id.
                null
        );          //asynchronously return.

        transactionManager.Asy_WriteRecord_Cond(txnContext,
                "bookEntries",
                event.getTargetBookEntryId(),
                astTable, astID, null
        );   //asynchronously return.

        transactionManager.CommitTransaction(txnContext);

        transactionTxnEvents.add(event);
    }

    protected void DEPOSITE_REQUEST_CONSTRUCT(DepositTxnEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        //it simply construct the operations and return.
        transactionManager.BeginTransaction(txnContext);
        transactionManager.Asy_WriteRecord(txnContext, "accounts", event.getAccountId(), null);// read and modify the account itself.
        transactionManager.Asy_WriteRecord(txnContext, "bookEntries", event.getBookEntryId(), null);// read and modify the asset itself.
        transactionManager.CommitTransaction(txnContext);

        depositTxnEvents.add(event);
    }

    private void DEPOSITE_REQUEST_POST() throws InterruptedException {
        for (DepositTxnEvent event : depositTxnEvents) {
            DEPOSITE_REQUEST_POST(event);
        }
    }

    private void TRANSFER_REQUEST_POST() throws InterruptedException {
        for (TransactionTxnEvent event : transactionTxnEvents) {
            TRANSFER_REQUEST_POST(event);
        }
    }

    private void TRANSFER_REQUEST_CORE() throws InterruptedException {
        for (TransactionTxnEvent event : transactionTxnEvents) {

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
