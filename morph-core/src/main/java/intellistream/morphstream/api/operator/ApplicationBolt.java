package intellistream.morphstream.api.operator;

import intellistream.morphstream.engine.stream.components.operators.api.TransactionalBolt;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class ApplicationBolt extends TransactionalBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationBolt.class);
    private HashMap<String, TxnDescription> TxnDescriptionHashMap;
    public ApplicationBolt(HashMap<String, TxnDescription> txnDescriptionHashMap) {
        super(LOG, 0);
        this.TxnDescriptionHashMap = txnDescriptionHashMap;
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {

    }

    //TODO: During post process, increment inputEvent's bid and pass downstream
}
