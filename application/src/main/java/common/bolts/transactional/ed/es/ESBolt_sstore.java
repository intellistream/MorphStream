package common.bolts.transactional.ed.es;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESBolt_sstore extends ESBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ESBolt_sstore.class);

    public ESBolt_sstore(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public ESBolt_sstore(int fid) {
        super(LOG, fid, null);
    }
}
