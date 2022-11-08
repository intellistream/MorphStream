package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
import common.bolts.transactional.ed.trg.TRGBolt;
import common.param.ed.tr.TREvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;

public class TCGBolt_ts extends TRGBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TCGBolt_ts.class);
    ArrayDeque<TREvent> TRGEvents;

    public TCGBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public TCGBolt_ts(int fid) {
        super(LOG, fid, null);
    }
}
