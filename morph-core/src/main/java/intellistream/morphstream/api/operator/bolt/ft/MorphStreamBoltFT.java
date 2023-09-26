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
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingResult.LoggingResult;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult.SnapshotResult;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.util.FaultToleranceConstants;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
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
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.BEGIN_SNAPSHOT_TIME_MEASURE;

public class MorphStreamBoltFT extends AbstractMorphStreamBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamBoltFT.class);
    private final HashMap<String, TxnDescription> txnDescriptionMap;//Transaction flag -> TxnDescription. E.g. "transfer" -> transferTxnDescription
    private final ArrayDeque<TransactionalEvent> eventQueue;//Transactional events deque
    private final HashMap<Long, HashMap<String,StateAccess>> eventStateAccessesMap;//{Event.bid -> {stateAccessName -> stateAccess}}. In fact, this maps each event to its txn.
    private final HashMap<String, HashMap<String, Integer>> tableFieldIndexMap; //Table name -> {field name -> field index}
    public AbstractSink sink;//If combo is enabled, we need to define a sink for the bolt
    public boolean isCombo = false;
    public FTManager ftManager;
    public FTManager loggingManager;

    public MorphStreamBoltFT(HashMap<String, TxnDescription> txnDescriptionMap, int fid) {
        super(LOG, fid);
        this.txnDescriptionMap = txnDescriptionMap;
        eventQueue = new ArrayDeque<>();
        eventStateAccessesMap = new HashMap<>();
        tableFieldIndexMap = MorphStreamEnv.get().databaseInitializer().getTableFieldIndexMap();
    }
    public MorphStreamBoltFT(HashMap<String, TxnDescription> txnDescriptionMap, int fid, AbstractSink sink) {
        super(LOG, fid);
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

    protected void execute_ts_normal(Tuple in) throws DatabaseException, InterruptedException {
        MeasureTools.BEGIN_TOTAL_TIME_MEASURE_TS(thread_Id);
        PRE_EXECUTE(in);
        MeasureTools.END_PREPARE_TIME_MEASURE_ACC(thread_Id);
        PRE_TXN_PROCESS(_bid, timestamp);
    }

    protected void PRE_EXECUTE(Tuple in) {
        if (enable_latency_measurement)
            timestamp = in.getLong(1);
        else
            timestamp = 0L;//
        _bid = in.getBID();
        input_event = in.getValue(0);
        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
    }

    @Override
    protected void PRE_TXN_PROCESS(long _bid, long timestamp) throws DatabaseException, InterruptedException {
        MeasureTools.BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TransactionalEvent event = (TransactionalEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            Transaction_Request_Construct(event, txnContext);
            MeasureTools.END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);
        }
    }

    protected void Transaction_Request_Construct(TransactionalEvent event, TxnContext txnContext) throws DatabaseException {
        TxnDescription txnDescription = txnDescriptionMap.get(event.getFlag());
        //Initialize state access map for each event
        eventStateAccessesMap.put(event.getBid(), new HashMap<>());
        transactionManager.BeginTransaction(txnContext);

        //Each event triggers multiple state accesses
        for (Map.Entry<String, StateAccessDescription> descEntry: txnDescription.getStateAccessDescEntries()) {
            //Initialize state access based on state access description
            String stateAccessName = descEntry.getKey();
            StateAccessDescription stateAccessDesc = descEntry.getValue();
            StateAccess stateAccess = new StateAccess(event.getFlag(), stateAccessDesc);

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

        transactionManager.CommitTransaction(txnContext);
        eventQueue.add(event);
    }

    protected void Transaction_Post_Process() {
        for (TransactionalEvent event : eventQueue) {
            Result udfResultReflect;
            try {
                //Invoke client defined post-processing UDF using Reflection
                Class<?> clientClass = Class.forName(MorphStreamEnv.get().configuration().getString("clientClassName"));
                if (Client.class.isAssignableFrom(clientClass)) {
                    // Cast the class object to MyAbstractClass
                    Client clientObj = (Client) clientClass.getDeclaredConstructor().newInstance();
                    HashMap<String, StateAccess> stateAccesses = eventStateAccessesMap.get(event.getBid());
                    // Option 1: Invoke postUDF using Interface
                    udfResultReflect = clientObj.postUDF(stateAccesses);
                    // Option 2: Invoke postUDF using Method Reflection
//                    String postUDFName = txnDescriptionMap.get(event.getFlag()).getPostUDFName();
//                    Method postUDF = clientClass.getMethod(postUDFName, HashMap.class);
//                    udfResultReflect = (Result) postUDF.invoke(clientObj, stateAccesses);
                    //We can also use reflection to access fields in client class
                }
                if (!enable_app_combo) {
                    collector.emit(event.getBid(), udfResultReflect.getTransactionalEvent(), event.getTimestamp());
                } else {
                    if (enable_latency_measurement) {
                        //TODO: Define sink for bolt
//                        sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.transaction_result, event.getTimestamp())));
                    }
                }

            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Client class not found");
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("Fail to create instance for client class");
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Client post UDF method not found");
            } catch (InvocationTargetException e) {
                throw new RuntimeException("Client post UDF invocation failed");
            } catch (InterruptedException e) {
                throw new RuntimeException("Output emission interrupted");
            }
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {

        if (in.isMarker()) {
            int numEvents = eventQueue.size();
            {
                transactionManager.start_evaluate(thread_Id, in.getBID(), numEvents);
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
            {
                Transaction_Post_Process();
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
            {
                eventQueue.clear();
                eventStateAccessesMap.clear();
            }
        } else {
            execute_ts_normal(in);
        }
    }

    @Override
    public DescriptiveStatistics getLatencyStats() {
        return null;
    }

    @Override
    public double getThroughputStats() {
        return 0;
    }

}
