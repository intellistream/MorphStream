package common.bolts.transactional.ed.tr;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TRBolt_sstore extends TRBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TRBolt_sstore.class);

    public TRBolt_sstore(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public TRBolt_sstore(int fid) {
        super(LOG, fid, null);
    }
}