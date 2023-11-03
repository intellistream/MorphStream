package intellistream.morphstream.api.operator.bolt.ft;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.api.state.StateAccessDescription;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.state.StateObjectDescription;
import intellistream.morphstream.api.utils.MetaTypes;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractMorphStreamBolt;
import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingResult.LoggingResult;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult.SnapshotResult;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.profiler.RuntimeMonitor;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.util.FaultToleranceConstants;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.*;
import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.BEGIN_SNAPSHOT_TIME_MEASURE;

public class MorphStreamBoltFT extends AbstractMorphStreamBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamBoltFT.class);
    private final HashMap<String, TxnDescription> txnDescriptionMap;//Transaction flag -> TxnDescription. E.g. "transfer" -> transferTxnDescription
    private final ArrayDeque<TransactionalEvent> eventQueue;//Transactional events deque
    private final HashMap<Long, HashMap<String,StateAccess>> eventStateAccessesMap;//{Event.bid -> {stateAccessName -> stateAccess}}. In fact, this maps each event to its txn.
    private final HashMap<String, HashMap<String, Integer>> tableFieldIndexMap; //Table name -> {field name -> field index}
    public AbstractSink sink;//If combo is enabled, we need to define a sink for the bolt
    public boolean isCombo = false;
    private int currentBatchID = 1; //batchID starts from 1
    private final SynchronizedDescriptiveStatistics latencyStat = new SynchronizedDescriptiveStatistics(); //latency statistics of current batch
    private long batchStartTime = 0; //Timestamp of the first event in the current batch
    private boolean isNewBatch = true; //Whether the input event indicates a new batch
    public FTManager ftManager;
    public FTManager loggingManager;

    public MorphStreamBoltFT(String id, HashMap<String, TxnDescription> txnDescriptionMap, int fid) {
        super(id, LOG, fid);
        this.txnDescriptionMap = txnDescriptionMap;
        eventQueue = new ArrayDeque<>();
        eventStateAccessesMap = new HashMap<>();
        tableFieldIndexMap = MorphStreamEnv.get().databaseInitializer().getTableFieldIndexMap();
    }
    public MorphStreamBoltFT(String id, HashMap<String, TxnDescription> txnDescriptionMap, int fid, AbstractSink sink) {
        super(id, LOG, fid);
        this.sink = sink;
        this.isCombo = true;
        this.txnDescriptionMap = txnDescriptionMap;
        eventQueue = new ArrayDeque<>();
        eventStateAccessesMap = new HashMap<>();
        tableFieldIndexMap = MorphStreamEnv.get().databaseInitializer().getTableFieldIndexMap();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        this.ftManager = getContext().getFtManager();
        this.loggingManager = getContext().getLoggingManager();
    }

    protected void execute_ts_normal(Tuple in) throws DatabaseException {
        RuntimeMonitor.get().PREPARE_START_TIME_MEASURE(this.getOperatorID(), currentBatchID, thread_Id);
        PRE_EXECUTE(in);
        RuntimeMonitor.get().ACC_PREPARE_TIME_MEASURE(this.getOperatorID(), currentBatchID, thread_Id);
        PRE_TXN_PROCESS(_bid);
    }

    protected void PRE_EXECUTE(Tuple in) {
        if (enable_latency_measurement)
            operatorTimestamp = System.nanoTime();
        else
            operatorTimestamp = 0L;//
        _bid = in.getBID();
        input_event = in.getValue(0);
        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
    }

    @Override
    protected void PRE_TXN_PROCESS(long _bid) throws DatabaseException {
        RuntimeMonitor.get().PRE_EXE_START_TIME_MEASURE(this.getOperatorID(), currentBatchID, thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TransactionalEvent event = (TransactionalEvent) input_event;
            if (enable_latency_measurement) {
                event.setOperationTimestamp(operatorTimestamp);
            }
            Transaction_Request_Construct(event, txnContext);
            RuntimeMonitor.get().ACC_PRE_EXE_TIME_MEASURE(this.getOperatorID(), currentBatchID, thread_Id);
        }
    }

    protected void Transaction_Request_Construct(TransactionalEvent event, TxnContext txnContext) throws DatabaseException {
        TxnDescription txnDescription = txnDescriptionMap.get(event.getFlag());
        //Initialize state access map for each event
        eventStateAccessesMap.put(event.getBid(), new HashMap<>());
        transactionManager.BeginTransaction(txnContext);

        int stateAccessIndex = 0; // index of state access in the txn, used to generate StateAccessID (OperationID)
        //Each event triggers multiple state accesses
        for (Map.Entry<String, StateAccessDescription> descEntry: txnDescription.getStateAccessDescEntries()) {
            //Initialize state access based on state access description
            String stateAccessName = descEntry.getKey();
            StateAccessDescription stateAccessDesc = descEntry.getValue();
            StateAccess stateAccess = new StateAccess(event.getBid() + "_" + stateAccessIndex, this.getOperatorID(), event.getFlag(), stateAccessDesc);
            stateAccessIndex += 1;

            //Each state access involves multiple state objects
            for (StateObjectDescription stateObjDesc: stateAccessDesc.getStateObjDescList()) {
                StateObject stateObject = new StateObject(
                        stateObjDesc.getName(),
                        stateObjDesc.getType(),
                        stateObjDesc.getTableName(),
                        event.getKey(stateObjDesc.getTableName(), stateObjDesc.getKeyIndex()),
                        tableFieldIndexMap.get(stateObjDesc.getTableName())
                );
                stateAccess.addStateObject(stateObjDesc.getName(), stateObject);
                //Label writeRecord for easy reference
                if (stateObjDesc.getType() == MetaTypes.AccessType.WRITE) {
                    stateAccess.setWriteRecordName(stateObjDesc.getName());
                }
            }

            //Each state access involves multiple conditions (values that are not commonly shared among events)
            for (String valueName: stateAccessDesc.getValueNames()) {
                stateAccess.addValue(valueName, event.getValue(valueName));
            }

            eventStateAccessesMap.get(event.getBid()).put(stateAccessName, stateAccess);
            transactionManager.submitStateAccess(stateAccess, txnContext);
        }

        transactionManager.CommitTransaction(txnContext, currentBatchID);
        eventQueue.add(event);
    }

    protected void Transaction_Post_Process() {
        for (TransactionalEvent event : eventQueue) {
            Result udfResultReflect = null;
            try {
                //Invoke client defined post-processing UDF using Reflection
                Class<?> clientClass = Class.forName(MorphStreamEnv.get().configuration().getString("clientClassName"));
                if (Client.class.isAssignableFrom(clientClass)) {
                    Client clientObj = (Client) clientClass.getDeclaredConstructor().newInstance();
                    HashMap<String, StateAccess> stateAccesses = eventStateAccessesMap.get(event.getBid());
                    udfResultReflect = clientObj.postUDF(event.getFlag(), stateAccesses);
                }
                if (enable_latency_measurement) {
                    latencyStat.addValue(System.nanoTime() - event.getOperationTimestamp());
                }
                if (!isCombo) {
                    assert udfResultReflect != null;
                    collector.emit(event.getBid(), udfResultReflect.getTransactionalEvent());
                } else {
                    if (enable_latency_measurement) {
                        assert udfResultReflect != null;
                        sink.execute(new Tuple(event.getBid(), this.thread_Id, context,
                                new GeneralMsg<>(DEFAULT_STREAM_ID, udfResultReflect.getResults(), event.getOriginTimestamp())));
                    }
                }
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("Client class instantiation failed");
            } catch (NoSuchMethodException | InvocationTargetException e) {
                throw new RuntimeException("Client post UDF invocation failed");
            } catch (InterruptedException e) {
                throw new RuntimeException("Output emission interrupted");
            } catch (BrokenBarrierException | IOException | DatabaseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {

        if (in.isMarker()) {
            int numEvents = eventQueue.size();
            {
                transactionManager.start_evaluate(this.getOperatorID(), currentBatchID, numEvents, thread_Id, in.getBID());
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
            }
            if (Objects.equals(in.getMarker().getMessage(), "commit_early") || Objects.equals(in.getMarker().getMessage(), "commit_snapshot_early")) {
                this.loggingManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new LoggingResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
            }
            { // post-processing
                RuntimeMonitor.get().POST_START_TIME_MEASURE(this.getOperatorID(), currentBatchID, thread_Id);
                Transaction_Post_Process();
                RuntimeMonitor.get().POST_TIME_MEASURE(this.getOperatorID(), currentBatchID, thread_Id);
            }
            if (enable_latency_measurement) {
                isNewBatch = true;
                RuntimeMonitor.get().submitRuntimeData(this.getOperatorID(), currentBatchID, thread_Id, latencyStat, batchStartTime, System.nanoTime());
                latencyStat.clear();
            }
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
            eventQueue.clear();
            eventStateAccessesMap.clear();
            RuntimeMonitor.get().END_TOTAL_TIME_MEASURE(this.getOperatorID(), currentBatchID, thread_Id);
            currentBatchID += 1;
            if (isCombo) {
                sink.execute(in);
            }
            if (Objects.equals(in.getMarker().getMessage(), "pause")) { //TODO: Call stage.SOURCE_CONTROL to perform the following operations
//                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
//                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
//                getContext().stop_running();
            }
        } else {
            if (enable_latency_measurement) {
                if (isNewBatch) { //only executed by 1st event in a batch
                    isNewBatch = false;
                    batchStartTime = System.nanoTime();
                }
            }
            execute_ts_normal(in);
        }
    }

}
