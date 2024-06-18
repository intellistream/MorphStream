package intellistream.morphstream.api.operator.bolt;

import commonStorage.RequestTemplates;
import communication.dao.VNFRequest;
import intellistream.morphstream.transNFV.AdaptiveCCManager;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.input.TransactionalVNFEvent;
import intellistream.morphstream.transNFV.simVNF.VNFRunner;
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
    private final ArrayDeque<TransactionalVNFEvent> eventQueue; //Transactional events deque
    public AbstractSink sink;//If combo is enabled, we need to define a sink for the bolt
    public boolean isCombo = false;
    private final Map<Integer, Socket> instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
    private final int boltThreadCount = MorphStreamEnv.get().configuration().getInt("tthread");
    private final int totalRequests = MorphStreamEnv.get().configuration().getInt("totalEvents");
    private final int expRequestCount = totalRequests / boltThreadCount;
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static final ConcurrentHashMap<Integer, Object> instanceLocks = MorphStreamEnv.instanceLocks;
    private int requestCounter;
    private static final int numInstance = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
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
//        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid, ((TransactionalEvent) input_event).getTxnRequestID());
    }

    @Override
    protected void PRE_TXN_PROCESS(long _bid) throws DatabaseException {
        TransactionalVNFEvent event = (TransactionalVNFEvent) input_event;
        TxnContext txnContext = new TxnContext(thread_Id, this.fid, _bid, ((TransactionalEvent) input_event).getTxnRequestID());
        if (enable_latency_measurement) {
            event.setOperationTimestamp(operatorTimestamp);
        }
        Transaction_Request_Construct(event, txnContext);
    }

    protected void Transaction_Request_Construct(TransactionalVNFEvent event, TxnContext txnContext) throws DatabaseException {
        transactionManager.BeginTransaction(txnContext);
        String[] saData;

        if (communicationChoice == 1) {
            throw new UnsupportedOperationException("Communication choice 1 is not supported yet.");
        } else if (communicationChoice == 0) {
            saData = new String[]{String.valueOf(event.getVnfID()), String.valueOf(event.getSaID()), String.valueOf(event.getSaType()), "testTable", event.getTupleID(), String.valueOf(event.getInstanceID())}; //Hardcoded for preliminary study
        } else {
            throw new RuntimeException("Invalid communication choice");
        }

        transactionManager.submitStateAccess(saData, txnContext);
        transactionManager.CommitTransaction(txnContext);
        eventQueue.add(event);
    }

    protected void Transaction_Post_Process() {

        if (communicationChoice == 1) {
            throw new UnsupportedOperationException("Communication choice 1 is not supported yet.");

        } else if (communicationChoice == 0) {
            for (TransactionalVNFEvent event : eventQueue) {
                VNFRequest request = new VNFRequest((int) event.getTxnRequestID(), event.getInstanceID(),
                        Integer.parseInt(event.getTupleID()), event.getSaType(), event.getBid(), event.getPuncID(), 0, event.getVnfID(), event.getSaID());
                VNFRunner.getSender(event.getInstanceID()).submitFinishedRequest(request);
                requestCounter++;
            }
            if (requestCounter == expRequestCount) {
                System.out.println("TPG CC processed all " + requestCounter + " requests.");
            }
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
