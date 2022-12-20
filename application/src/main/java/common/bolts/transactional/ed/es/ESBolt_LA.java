package common.bolts.transactional.ed.es;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESBolt_LA extends ESBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ESBolt_LA.class);

    public ESBolt_LA(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public ESBolt_LA(int fid) {
        super(LOG, fid, null);
    }
}
