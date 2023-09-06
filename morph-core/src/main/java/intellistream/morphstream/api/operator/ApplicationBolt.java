package intellistream.morphstream.api.operator;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.api.state.StateAccessDescription;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.state.StateObjectDescription;
import intellistream.morphstream.engine.stream.components.operators.api.TransactionalBolt;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

public class ApplicationBolt extends TransactionalBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationBolt.class);
    private HashMap<String, TxnDescription> TxnDescriptionHashMap;//Event -> TxnDescription
    public ApplicationBolt(HashMap<String, TxnDescription> txnDescriptionHashMap) {
        super(LOG, 0);
        this.TxnDescriptionHashMap = txnDescriptionHashMap;
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {

    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {

    }

    protected void Transaction_Request_Construct(TransactionalEvent event) {
        String eventType = event.getFlags();
        TxnDescription txnDescription = TxnDescriptionHashMap.get(eventType);
        for (StateAccessDescription accessDescription: txnDescription.getStateAccessMap().values()) {
            StateAccess stateAccess = new StateAccess(accessDescription);
            for (Map.Entry<String, StateObjectDescription> entry: accessDescription.getStateObjectMap().entrySet()) {
                String key = event.getKeyMap().get(entry.getValue().getKeyName());
                String value = (String) event.getValueMap().get(entry.getValue().getValueName());
                StateObject stateObject = new StateObject(entry.getValue().getTableName(), key, value);
                stateAccess.setStateObject(entry.getKey(), stateObject);
            }
            transactionManager.AccessRecord(stateAccess);
        }
    }

    //TODO: During post process, increment inputEvent's bid and pass downstream
}
