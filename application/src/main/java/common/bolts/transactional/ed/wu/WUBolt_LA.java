package common.bolts.transactional.ed.wu;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WUBolt_LA extends WUBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WUBolt_LA.class);

    public WUBolt_LA(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public WUBolt_LA(int fid) {
        super(LOG, fid, null);
    }
}
