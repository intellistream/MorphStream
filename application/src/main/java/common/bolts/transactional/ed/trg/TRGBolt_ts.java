package common.bolts.transactional.ed.trg;

import combo.SINKCombo;
import common.param.ed.tr.TREvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;


public class TRGBolt_ts extends TRGBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TRGBolt_ts.class);
    ArrayDeque<TREvent> TRGEvents;

    public TRGBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public TRGBolt_ts(int fid) {
        super(LOG, fid, null);
    }
}
