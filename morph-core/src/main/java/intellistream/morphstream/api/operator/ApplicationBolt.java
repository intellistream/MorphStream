package intellistream.morphstream.api.operator;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.api.state.StateAccessDescription;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.state.StateObjectDescription;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.components.operators.api.TransactionalBolt;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.TxnManagerTStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.*;
import static intellistream.morphstream.engine.txn.profiler.Metrics.NUM_ITEMS;

public class ApplicationBolt extends TransactionalBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationBolt.class);
    private final HashMap<String, TxnDescription> txnDescriptionMap;//Transaction flag -> TxnDescription
    private final ArrayDeque<TransactionalEvent> eventQueue;//Transactional events deque
    private final HashMap<Integer, HashMap<String,StateAccess>> eventStateAccessesMap;//{Event.bid -> {stateAccessName -> stateAccess}}. In fact, this maps each event to its txn.

    public ApplicationBolt(HashMap<String, TxnDescription> txnDescriptionMap) {
        super(LOG, 0); //TODO: Check fid
        this.txnDescriptionMap = txnDescriptionMap;
        eventQueue = new ArrayDeque<>();
        eventStateAccessesMap = new HashMap<>();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        //TODO: Check txnManager inputs, such as fid
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(),
                thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
    }

    //TODO: loadDB is only called by the first bolt in topology. Refine this.
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        MorphStreamEnv.get().databaseInitialize().loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), false);
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
        //Only used in Lock-based CC algorithms
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

    protected void Transaction_Request_Construct(TransactionalEvent event, TxnContext txnContext) {
        String txnFlag = event.getFlag();
        TxnDescription txnDescription = txnDescriptionMap.get(txnFlag);
        //Initialize state access map for each event
        eventStateAccessesMap.put((int) event.getBid(), new HashMap<>());
        transactionManager.BeginTransaction(txnContext);
        //Each event triggers multiple state accesses
        for (Map.Entry<String, StateAccessDescription> descEntry: txnDescription.getStateAccessDescEntries()) {
            //Initialize state access based on state access description
            String stateAccessName = descEntry.getKey();
            StateAccessDescription stateAccessDesc = descEntry.getValue();
            StateAccess stateAccess = new StateAccess(stateAccessDesc);
            //Each state access involves multiple state objects
            for (Map.Entry<String, StateObjectDescription> entry: stateAccessDesc.getStateObjectEntries()) {
                StateObjectDescription stateObjDesc = entry.getValue();
                String stateObjKey = event.getKey(stateObjDesc.getTableName(), stateObjDesc.getKeyIndex());
                String stateObjValue = (String) event.getValue(stateObjDesc.getValueName());
                StateObject stateObject = new StateObject(stateObjDesc.getType(), stateObjDesc.getTableName(), stateObjKey, stateObjValue);
                stateAccess.addStateObject(entry.getKey(), stateObject);
            }
            eventStateAccessesMap.get((int) event.getBid()).put(stateAccessName, stateAccess);
            transactionManager.submitStateAccess(stateAccess, txnContext);
        }
        transactionManager.CommitTransaction(txnContext);
        eventQueue.add(event);
    }

    protected void Transaction_Post_Process() {
        for (TransactionalEvent event : eventQueue) {
            //Invoke client defined post-processing UDF using Reflection
            Result postUDFResult;
            try {
                Class<?> clientClass = Class.forName(MorphStreamEnv.get().configuration().getString("clientClassName"));
                Object clientObj = clientClass.getDeclaredConstructor().newInstance();
                HashMap<String,StateAccess> stateAccesses = eventStateAccessesMap.get((int) event.getBid());
                String postUDFName = txnDescriptionMap.get(event.getFlag()).getPostUDFName();
                Method postUDF = clientClass.getMethod(postUDFName, HashMap.class, TransactionalEvent.class);
                postUDFResult = (Result) postUDF.invoke(clientObj, stateAccesses, event);
                if (!enable_app_combo) {
                    collector.emit(event.getBid(), postUDFResult.getTransactionalEvent(), event.getTimestamp());
                } else {
                    if (enable_latency_measurement) {
                        //TODO: Define sink for bolt
//                            sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.transaction_result, event.getTimestamp())));
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
            MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
            {
                transactionManager.start_evaluate(thread_Id, in.getBID(), numEvents);
            }
            MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
            MeasureTools.BEGIN_POST_TIME_MEASURE(thread_Id);
            {
                Transaction_Post_Process();
            }
            MeasureTools.END_POST_TIME_MEASURE_ACC(thread_Id);
            eventQueue.clear();
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, numEvents);
        } else {
            execute_ts_normal(in);
        }
    }

}
