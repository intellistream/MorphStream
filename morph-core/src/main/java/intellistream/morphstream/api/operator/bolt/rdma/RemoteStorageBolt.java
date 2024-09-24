package intellistream.morphstream.api.operator.bolt.rdma;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.Function;
import intellistream.morphstream.api.state.FunctionDescription;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.state.StateObjectDescription;
import intellistream.morphstream.api.utils.MetaTypes;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractMorphStreamBolt;
import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Marker;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.transaction.FunctionDAGDescription;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;
import intellistream.morphstream.engine.txn.transaction.impl.distributed.TxnManagerRemoteStorage;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.combo_bid_size;
import static intellistream.morphstream.configuration.CONTROL.enable_latency_measurement;
import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;

public class RemoteStorageBolt extends AbstractMorphStreamBolt {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteStorageBolt.class);
    private final HashMap<String, FunctionDAGDescription> txnDescriptionMap;//Transaction flag -> TxnDescription. E.g. "transfer" -> transferTxnDescription
    private final ArrayDeque<TransactionalEvent> eventQueue;//Transactional events deque
    private final HashMap<Long, HashMap<String, Function>> eventStateAccessesMap;//{Event.bid -> {stateAccessName -> stateAccess}}. In fact, this maps each event to its txn.
    private final HashMap<String, HashMap<String, Integer>> tableFieldIndexMap; //Table name -> {field name -> field index}
    public AbstractSink sink;//If combo is enabled, we need to define a sink for the bolt
    public boolean isCombo = false;
    private int currentBatchID = 1; //batchID starts from 1
    private final SynchronizedDescriptiveStatistics latencyStat = new SynchronizedDescriptiveStatistics(); //latency statistics of current batch
    private int punctuation_interval;
    private boolean isNewBatch = true; //Whether the input event indicates a new batch
    public static final Client clientObj;
    static {
        try {
            Class<?> clientClass = Class.forName(MorphStreamEnv.get().configuration().getString("clientClassName"));
            clientObj = (Client) clientClass.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public RemoteStorageBolt(String id, HashMap<String, FunctionDAGDescription> txnDescriptionMap, int fid) {
        super(id, LOG, fid);
        this.txnDescriptionMap = txnDescriptionMap;
        eventQueue = new ArrayDeque<>();
        eventStateAccessesMap = new HashMap<>();
        tableFieldIndexMap = MorphStreamEnv.get().databaseInitializer().getTableFieldIndexMap();
    }
    public RemoteStorageBolt(String id, HashMap<String, FunctionDAGDescription> txnDescriptionMap, int fid, AbstractSink sink) {
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
        transactionManager = new TxnManagerRemoteStorage(thisTaskId, this.context.getThisComponent().getNumTasks(), config.getString("scheduler"));
        punctuation_interval = config.getInt("totalBatch");
    }

    protected void execute_ts_normal(Tuple in) throws DatabaseException {
        PRE_EXECUTE(in);
        PRE_TXN_PROCESS(_bid);
    }

    protected void PRE_EXECUTE(Tuple in) {
        input_event = (TransactionalEvent) in.getValue(0);
        _bid = input_event.getBid();
        txn_context[0] = new FunctionContext(thread_Id, _bid);
    }

    @Override
    protected void PRE_TXN_PROCESS(long _bid) throws DatabaseException {
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TransactionalEvent event = input_event;
            Transaction_Request_Construct(event, txn_context[0]);
        }
    }

    protected void Transaction_Request_Construct(TransactionalEvent event, FunctionContext functionContext) throws DatabaseException {
        FunctionDAGDescription functionDAGDescription = txnDescriptionMap.get(event.getFlag());
        functionContext.setTransactionCombo(functionDAGDescription.getTransactionCombo());
        //Initialize state access map for each event
        eventStateAccessesMap.put(event.getBid(), new HashMap<>());
        transactionManager.BeginTransaction(functionContext);

        int stateAccessIndex = 0; // index of state access in the txn, used to generate StateAccessID (OperationID)
        //Each event triggers multiple state accesses
        for (Map.Entry<String, FunctionDescription> descEntry: functionDAGDescription.getFunctionDescEntries()) {
            //Initialize state access based on state access description
            String stateAccessName = descEntry.getKey();
            FunctionDescription stateAccessDesc = descEntry.getValue();
            Function function = new Function(event.getBid() + "_" + stateAccessIndex, this.getOperatorID(), event.getFlag(), stateAccessDesc);
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
                function.addStateObject(stateObjDesc.getName(), stateObject);
                //Label writeRecord for easy reference
                if (stateObjDesc.getType() == MetaTypes.AccessType.WRITE) {
                    function.setWriteRecordName(stateObjDesc.getName());
                }
            }

            //Each state access involves multiple conditions (values that are not commonly shared among events)
            for (String paraName: stateAccessDesc.getParaNames()) {
                function.addPara(paraName, event.getPara(paraName));
            }

            eventStateAccessesMap.get(event.getBid()).put(stateAccessName, function);
            transactionManager.submitStateAccess(function, functionContext);
        }

        transactionManager.CommitTransaction(functionContext, currentBatchID);
        eventQueue.add(event);
    }

    protected void Transaction_Post_Process() {
        int delta = eventQueue.size() / punctuation_interval;
        int total = eventQueue.size();
        for (int i = 0; i < punctuation_interval; i++) {
            int leftBound = i * delta;
            int rightBound;
            if (i == punctuation_interval - 1) {//last thread
                rightBound = total;
            } else {
                rightBound = (i + 1) * delta;
            }
            for (int j = leftBound; j < rightBound; j++) {
                TransactionalEvent event = eventQueue.poll();
                MeasureTools.WorkerFinishStartTime(this.thread_Id);
                Result udfResultReflect = null;
                try {
                    //Invoke client defined post-processing UDF using Reflection
                    HashMap<String, Function> stateAccesses = eventStateAccessesMap.get(event.getBid());
                    udfResultReflect = clientObj.postUDF(event.getBid(), event.getFlag(), stateAccesses);
                    if (!isCombo) {
                        collector.emit(event.getBid(), udfResultReflect.getTransactionalEvent());
                    } else {
                        sink.execute(new Tuple(this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, udfResultReflect)));
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException("Output emission interrupted");
                } catch (BrokenBarrierException | IOException | DatabaseException e) {
                    throw new RuntimeException(e);
                }
            }
            marker = new Tuple(this.thread_Id, context, new Marker(DEFAULT_STREAM_ID, -1, eventQueue.size(), 0, "punctuation"));
            try {
                sink.execute(marker);
            } catch (InterruptedException | DatabaseException | BrokenBarrierException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {

        if (in.isMarker()) {
            { // post-processing
                Transaction_Post_Process();
            }
            if (enable_latency_measurement) {
                isNewBatch = true;
                latencyStat.clear();
            }
            eventQueue.clear();
            eventStateAccessesMap.clear();
            currentBatchID += 1;
        } else {
            if (enable_latency_measurement) {
                if (isNewBatch) { //only executed by 1st event in a batch
                    isNewBatch = false;
                }
            }
            execute_ts_normal(in);
        }
    }
}