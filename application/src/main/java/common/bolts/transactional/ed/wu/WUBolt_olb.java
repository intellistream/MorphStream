package common.bolts.transactional.ed.wu;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WUBolt_olb extends WUBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TRBolt_ts.class);
    public WUBolt_olb(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public WUBolt_olb(int fid){
        super(LOG,fid,null);
    }
}
