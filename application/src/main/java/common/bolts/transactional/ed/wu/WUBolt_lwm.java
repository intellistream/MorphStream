package common.bolts.transactional.ed.wu;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WUBolt_lwm extends WUBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WUBolt_lwm.class);

    public WUBolt_lwm(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public WUBolt_lwm(int fid) {
        super(LOG, fid, null);
    }
}
