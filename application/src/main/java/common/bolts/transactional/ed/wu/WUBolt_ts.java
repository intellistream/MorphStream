package common.bolts.transactional.ed.wu;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WUBolt_ts extends WUBolt{
    private static final Logger LOG= LoggerFactory.getLogger(WUBolt_ts.class);
    public WUBolt_ts(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public WUBolt_ts(int fid){
        super(LOG,fid,null);
    }
}
