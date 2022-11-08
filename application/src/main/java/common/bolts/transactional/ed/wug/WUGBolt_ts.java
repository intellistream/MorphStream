package common.bolts.transactional.ed.wug;

import combo.SINKCombo;
import common.bolts.transactional.ed.trg.TRGBolt;
import common.param.ed.tr.TREvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;

public class WUGBolt_ts extends WUGBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WUGBolt_ts.class);
    ArrayDeque<TREvent> TRGEvents;

    public WUGBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public WUGBolt_ts(int fid) {
        super(LOG, fid, null);
    }
}
