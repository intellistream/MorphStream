package common.bolts.transactional.ed.tr;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TRBolt_olb extends TRBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TRBolt_olb.class);

    public TRBolt_olb(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public TRBolt_olb(int fid) {
        super(LOG, fid, null);
    }
}
