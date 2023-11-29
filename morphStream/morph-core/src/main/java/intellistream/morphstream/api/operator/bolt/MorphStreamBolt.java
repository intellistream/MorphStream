package intellistream.morphstream.api.operator.bolt;

import commonStorage.RequestTemplates;
import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractMorphStreamBolt;
import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.profiler.RuntimeMonitor;
import libVNFFrontend.NativeInterface;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.*;
import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;

public class MorphStreamBolt extends AbstractMorphStreamBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamBolt.class);
    private final HashMap<String, String[]> txnTemplates; //Transaction flag -> state access IDs. E.g. "transfer" -> {"srcTransfer", "destTransfer"}
    private final HashMap<String, String[]> saTemplates; //State access ID -> state objects.
    private final ArrayDeque<TransactionalEvent> eventQueue;//Transactional events deque
    private final HashMap<Long, HashMap<String, String[]>> eventStateAccessesMap;//{Event.bid -> {stateAccessName -> stateAccess}}. In fact, this maps each event to its txn.
    private final HashMap<String, HashMap<String, Integer>> tableFieldIndexMap; //Table name -> {field name -> field index}
    public AbstractSink sink;//If combo is enabled, we need to define a sink for the bolt
    public boolean isCombo = false;
    private int currentBatchID = 1; //batchID starts from 1
    private final SynchronizedDescriptiveStatistics latencyStat = new SynchronizedDescriptiveStatistics(); //latency statistics of current batch
    private long batchStartTime = 0; //Timestamp of the first event in the current batch
    private boolean isNewBatch = true; //Whether the input event indicates a new batch
    private final boolean useNativeLib = MorphStreamEnv.get().configuration().getBoolean("useNativeLib", false); //Push post results to: true -> c/c++ native function, false -> Output collector

    public MorphStreamBolt(String id, int fid) {
        super(id, LOG, fid);
        txnTemplates = RequestTemplates.sharedTxnTemplates;
        saTemplates = RequestTemplates.sharedSATemplates;
        eventQueue = new ArrayDeque<>();
        eventStateAccessesMap = new HashMap<>();
        tableFieldIndexMap = MorphStreamEnv.get().databaseInitializer().getTableFieldIndexMap();
    }
    public MorphStreamBolt(String id, int fid, AbstractSink sink) {
        super(id, LOG, fid);
        this.sink = sink;
        this.isCombo = true;
        txnTemplates = RequestTemplates.sharedTxnTemplates;
        saTemplates = RequestTemplates.sharedSATemplates;
        eventQueue = new ArrayDeque<>();
        eventStateAccessesMap = new HashMap<>();
        tableFieldIndexMap = MorphStreamEnv.get().databaseInitializer().getTableFieldIndexMap();
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
        String[] saIDs = txnTemplates.get(event.getFlag());
//        eventStateAccessesMap.put(event.getBid(), new HashMap<>());
        transactionManager.BeginTransaction(txnContext);

        for (String saID : saIDs) {
            String[] stateAccess = saTemplates.get(saID).clone();

            for (int i = 2; i < stateAccess.length; i += 4) {
                String tableName = stateAccess[i];
                String keyIndex = stateAccess[i + 1];
                String key = event.getKey(tableName, Integer.parseInt(keyIndex));
                stateAccess[i + 1] = key;
            }
            // txn-UDF and post-UDF needs a shared data structure to pass txn results to post-UDF. TODO: Improve this
//            eventStateAccessesMap.get(event.getBid()).put(saID, stateAccess);
            transactionManager.submitStateAccess(stateAccess, txnContext);
        }

        transactionManager.CommitTransaction(txnContext, currentBatchID);
        eventQueue.add(event);
    }

    protected void Transaction_Post_Process() {

        if (useNativeLib) {
            for (TransactionalEvent event : eventQueue) {
                NativeInterface.__txn_finished(event.getBid()); //Notify libVNF for txn completion
            }
        } else {
            for (TransactionalEvent event : eventQueue) {
                Result udfResultReflect = null;
                try {
                    //Invoke client defined post-processing UDF using Reflection
                    Class<?> clientClass = Class.forName(MorphStreamEnv.get().configuration().getString("clientClassName"));
                    if (Client.class.isAssignableFrom(clientClass)) {
                        Client clientObj = (Client) clientClass.getDeclaredConstructor().newInstance();
                        HashMap<String, String[]> stateAccesses = eventStateAccessesMap.get(event.getBid());
                        udfResultReflect = new Result();
                        //TODO: Pass txn result to post UDF
//                        udfResultReflect = clientObj.postUDF(event.getFlag(), stateAccesses);
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
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {

        if (in.isMarker()) {
            int numEvents = eventQueue.size();
            { // state access
                transactionManager.start_evaluate(this.getOperatorID(), currentBatchID, numEvents, thread_Id, in.getBID());
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
//            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, numEvents); //TODO: Double confirm for FT measurement
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
