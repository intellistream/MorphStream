package common.bolts.transactional.ed.wu;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WUBolt_sstore extends WUBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TRBolt_ts.class);
    public WUBolt_sstore(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public WUBolt_sstore(int fid){
        super(LOG,fid,null);
    }
}
