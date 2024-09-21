package intellistream.morphstream.api.operator.bolt;

import commonStorage.RequestTemplates;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.input.ProactiveVNFRequest;
import intellistream.morphstream.transNFV.vnf.VNFManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractMorphStreamBolt;
import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static intellistream.morphstream.configuration.CONTROL.*;

public class MorphStreamBolt extends AbstractMorphStreamBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamBolt.class);
    private final HashMap<String, String[]> saTemplates; //State access ID -> state objects.
    private final ArrayDeque<ProactiveVNFRequest> eventQueue; //Transactional events deque
    public AbstractSink sink;//If combo is enabled, we need to define a sink for the bolt
    public boolean isCombo = false;
    private final int boltThreadCount = MorphStreamEnv.get().configuration().getInt("tthread");
    private final int totalRequests = MorphStreamEnv.get().configuration().getInt("totalEvents");
    private final int expRequestCount = totalRequests / boltThreadCount;
    private int requestCounter;
    private static final int numInstance = MorphStreamEnv.get().configuration().getInt("numInstances");
    private static final ConcurrentHashMap<Integer, Integer> instanceFinishReqCounter = new ConcurrentHashMap<>();
    private static final AtomicInteger totalFinishedReqCounter = new AtomicInteger(0);


    public MorphStreamBolt(String id, int fid) {
        super(id, LOG, fid);
        saTemplates = RequestTemplates.sharedSATemplates;
        eventQueue = new ArrayDeque<>();
        for (int i=0; i<numInstance; i++) {
            instanceFinishReqCounter.put(i, 0);
        }
    }
    public MorphStreamBolt(String id, int fid, AbstractSink sink) {
        super(id, LOG, fid);
        this.sink = sink;
        this.isCombo = true;
        saTemplates = RequestTemplates.sharedSATemplates;
        eventQueue = new ArrayDeque<>();
        for (int i=0; i<numInstance; i++) {
            instanceFinishReqCounter.put(i, 0);
        }
    }

    protected void execute_ts_normal(Tuple in) throws DatabaseException {
        PRE_EXECUTE(in);
        PRE_TXN_PROCESS(_bid);
    }

    protected void PRE_EXECUTE(Tuple in) {
        if (enable_latency_measurement)
            operatorTimestamp = System.nanoTime();
        else
            operatorTimestamp = 0L;
        _bid = in.getBID();
        input_event = in.getValue(0);
    }

    @Override
    protected void PRE_TXN_PROCESS(long _bid) throws DatabaseException {
        ProactiveVNFRequest event = (ProactiveVNFRequest) input_event;
        TxnContext txnContext = new TxnContext(thread_Id, this.fid, _bid, ((TransactionalEvent) input_event).getTxnRequestID());
        if (enable_latency_measurement) {
            event.setOperationTimestamp(operatorTimestamp);
        }
        Transaction_Request_Construct(event, txnContext);
    }

    protected void Transaction_Request_Construct(ProactiveVNFRequest event, TxnContext txnContext) throws DatabaseException {
        transactionManager.BeginTransaction(txnContext);
        transactionManager.submitStateAccess(event.getVnfRequest(), txnContext);
        transactionManager.CommitTransaction(txnContext);
        eventQueue.add(event);
    }

    protected void Transaction_Post_Process() {
        for (ProactiveVNFRequest event : eventQueue) {
            VNFManager.getInstance(event.getVnfRequest().getInstanceID()).submitFinishedRequest(event.getVnfRequest());
            requestCounter++;
        }
        if (requestCounter == expRequestCount) {
            System.out.println("TPG CC processed all " + requestCounter + " requests.");
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {
        if (in.isMarker()) {
            int numEvents = eventQueue.size();
            { // state access
                transactionManager.start_evaluate(this.getOperatorID(), numEvents, thread_Id, in.getBID());
            }
            { // post-processing
                Transaction_Post_Process();
            }
            eventQueue.clear();
            if (isCombo) {
                sink.execute(in);
            }
        } else {
            execute_ts_normal(in);
        }

    }

}
