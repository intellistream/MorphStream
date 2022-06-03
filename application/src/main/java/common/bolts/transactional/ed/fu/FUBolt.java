package common.bolts.transactional.ed.fu;

import combo.SINKCombo;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;

public class FUBolt extends TransactionalBolt {
    SINKCombo sink; // the default "next bolt"
    Tuple tuple;

    public FUBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "trtxn"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }
}
