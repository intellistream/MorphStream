package intellistream.morphstream.api.operator.bolt.socket;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.input.InputSource;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.*;
import intellistream.morphstream.api.utils.MetaTypes;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractMorphStreamBolt;
import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.FunctionDescription;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMsg;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.*;
import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;

public class MorphStreamBolt extends AbstractMorphStreamBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamBolt.class);
    private final HashMap<String, FunctionDescription> txnDescriptionMap;//Transaction flag -> TxnDescription. E.g. "transfer" -> transferTxnDescription
    private final ArrayDeque<Tuple2<ZMsg,TransactionalEvent>> eventQueue;//Transactional events deque
    private final HashMap<Long, HashMap<String,StateAccess>> eventStateAccessesMap;//{Event.bid -> {stateAccessName -> stateAccess}}. In fact, this maps each event to its txn.
    private final HashMap<String, HashMap<String, Integer>> tableFieldIndexMap; //Table name -> {field name -> field index}
    public AbstractSink sink;//If combo is enabled, we need to define a sink for the bolt
    public boolean isCombo = false;
    private int currentBatchID = 1; //batchID starts from 1
    private final SynchronizedDescriptiveStatistics latencyStat = new SynchronizedDescriptiveStatistics(); //latency statistics of current batch
    private long batchStartTime = 0; //Timestamp of the first event in the current batch
    private boolean isNewBatch = true; //Whether the input event indicates a new batch
    protected ZMsg msg;

    public MorphStreamBolt(String id, HashMap<String, FunctionDescription> txnDescriptionMap, int fid) {
        super(id, LOG, fid);
        this.txnDescriptionMap = txnDescriptionMap;
        eventQueue = new ArrayDeque<>();
        eventStateAccessesMap = new HashMap<>();
        tableFieldIndexMap = MorphStreamEnv.get().databaseInitializer().getTableFieldIndexMap();
    }
    public MorphStreamBolt(String id, HashMap<String, FunctionDescription> txnDescriptionMap, int fid, AbstractSink sink) {
        super(id, LOG, fid);
        this.sink = sink;
        this.isCombo = true;
        this.txnDescriptionMap = txnDescriptionMap;
        eventQueue = new ArrayDeque<>();
        eventStateAccessesMap = new HashMap<>();
        tableFieldIndexMap = MorphStreamEnv.get().databaseInitializer().getTableFieldIndexMap();
    }

    protected void execute_ts_normal(Tuple in) throws DatabaseException {
        PRE_EXECUTE(in);
        PRE_TXN_PROCESS(_bid);
    }

    protected void PRE_EXECUTE(Tuple in) {
        msg = (ZMsg) in.getValue(0);
        input_event = InputSource.inputFromStringToTxnEvent(msg.getLast().toString());
        _bid = input_event.getBid();
        txn_context[0] = new FunctionContext(thread_Id, _bid);
    }

    @Override
    protected void PRE_TXN_PROCESS(long _bid) throws DatabaseException {
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            FunctionContext functionContext = new FunctionContext(thread_Id, i);
            TransactionalEvent event = input_event;
            Transaction_Request_Construct(event, functionContext);
        }
    }

    protected void Transaction_Request_Construct(TransactionalEvent event, FunctionContext functionContext) throws DatabaseException {
        FunctionDescription functionDescription = txnDescriptionMap.get(event.getFlag());
        //Initialize state access map for each event
        eventStateAccessesMap.put(event.getBid(), new HashMap<>());
        transactionManager.BeginTransaction(functionContext);

        int stateAccessIndex = 0; // index of state access in the txn, used to generate StateAccessID (OperationID)
        //Each event triggers multiple state accesses
        for (Map.Entry<String, StateAccessDescription> descEntry: functionDescription.getStateAccessDescEntries()) {
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
            transactionManager.submitStateAccess(stateAccess, functionContext);
        }

        transactionManager.CommitTransaction(functionContext, currentBatchID);
        eventQueue.add(new Tuple2<>(msg, event));
    }

    protected void Transaction_Post_Process() {
        for (Tuple2<ZMsg, TransactionalEvent> msgs : eventQueue) {
            TransactionalEvent event = msgs._2();
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
                    assert udfResultReflect != null;
                    sink.execute(new Tuple(this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, msgs._1(), udfResultReflect)));
                }
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException("Client class instantiation failed");
            } catch (NoSuchMethodException e) {
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
            { // state access
                transactionManager.start_evaluate(this.getOperatorID(), currentBatchID, numEvents, thread_Id, 0);
            }
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
                    batchStartTime = System.nanoTime();
                }
            }
            execute_ts_normal(in);
        }
    }

}
