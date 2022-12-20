package common.bolts.transactional.ed.wu;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WUBolt_nocc extends WUBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WUBolt_nocc.class);

    public WUBolt_nocc(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public WUBolt_nocc(int fid) {
        super(LOG, fid, null);
    }
}
