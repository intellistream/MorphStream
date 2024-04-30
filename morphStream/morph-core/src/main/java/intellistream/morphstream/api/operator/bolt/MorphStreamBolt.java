package intellistream.morphstream.api.operator.bolt;

import commonStorage.RequestTemplates;
import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.input.TransactionalVNFEvent;
import intellistream.morphstream.api.input.simVNF.VNFManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractMorphStreamBolt;
import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

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
    private static final boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);
    private int requestCounter;

    public MorphStreamBolt(String id, int fid) {
        super(id, LOG, fid);
        saTemplates = RequestTemplates.sharedSATemplates;
        eventQueue = new ArrayDeque<>();
    }
    public MorphStreamBolt(String id, int fid, AbstractSink sink) {
        super(id, LOG, fid);
        this.sink = sink;
        this.isCombo = true;
        saTemplates = RequestTemplates.sharedSATemplates;
        eventQueue = new ArrayDeque<>();
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

        if (serveRemoteVNF) {
            //TODO: Note that here we bypass the transaction encapsulation, because the inputs are stateAccess UDF requests
            saData = new String[4]; //saID, saType, tableName, tupleID
            String[] saTemplate = saTemplates.get(event.getFlag()); //saID, saType, tableName
            saData[0] = saTemplate[0];
            saData[1] = saTemplate[1];
            saData[2] = saTemplate[2];
            saData[3] = event.getTupleID();

        } else {
            saData = new String[]{"0", String.valueOf(event.getSaType()), "testTable", event.getTupleID()}; //Hardcoded for preliminary study
        }

        transactionManager.submitStateAccess(saData, txnContext);
        transactionManager.CommitTransaction(txnContext);
        eventQueue.add(event);
    }

    protected void Transaction_Post_Process() {

        if (serveRemoteVNF) {
            for (TransactionalVNFEvent event : eventQueue) {
                try {
                    OutputStream out = instanceSocketMap.get(event.getInstanceID()).getOutputStream();
                    String combined =  4 + ";" + event.getTxnRequestID();
                    byte[] byteArray = combined.getBytes();
                    out.write(byteArray);
                    out.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (requestCounter == expRequestCount) {
                System.out.println("TPG CC processed all " + requestCounter + " requests.");
            }

        } else {
            for (TransactionalVNFEvent event : eventQueue) {
                VNFRequest request = new VNFRequest((int) event.getTxnRequestID(), event.getInstanceID(), Integer.parseInt(event.getTupleID()), event.getSaType(), event.getBid());
                VNFManager.getReceiver(event.getInstanceID()).submitFinishedRequest(request);
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
