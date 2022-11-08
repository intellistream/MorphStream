package common.bolts.transactional.ed.cug;

import combo.SINKCombo;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import org.slf4j.Logger;

public abstract class CUGBolt extends TransactionalBolt {

    SINKCombo sink; // the default "next bolt"

    public CUGBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_trg"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }
}
