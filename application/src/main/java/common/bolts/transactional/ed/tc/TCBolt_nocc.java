package common.bolts.transactional.ed.tc;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCBolt_nocc extends TCBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TCBolt_nocc.class);

    public TCBolt_nocc(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public TCBolt_nocc(int fid) {
        super(LOG, fid, null);
    }
}
