package common.bolts.transactional.ed.tc;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCBolt_ts extends TCBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TRBolt_ts.class);
    public TCBolt_ts(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TCBolt_ts(int fid){
        super(LOG,fid,null);
    }
}
