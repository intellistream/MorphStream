package common.bolts.transactional.ed.cug;

import combo.SINKCombo;
import common.bolts.transactional.ed.trg.TRGBolt;
import common.param.ed.tr.TREvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;

public class CUGBolt_ts extends TRGBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CUGBolt_ts.class);
    ArrayDeque<TREvent> TRGEvents;

    public CUGBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public CUGBolt_ts(int fid) {
        super(LOG, fid, null);
    }
}
