package common.bolts.transactional.ed.wug;

import combo.SINKCombo;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import org.slf4j.Logger;

public abstract class WUGBolt extends TransactionalBolt {

    SINKCombo sink; // the default "next bolt"

    public WUGBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_trg"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }
}
