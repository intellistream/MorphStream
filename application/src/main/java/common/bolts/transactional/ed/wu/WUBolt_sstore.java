package common.bolts.transactional.ed.wu;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WUBolt_sstore extends WUBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WUBolt_sstore.class);

    public WUBolt_sstore(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public WUBolt_sstore(int fid) {
        super(LOG, fid, null);
    }
}
