package common.bolts.transactional.ed.tc;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCBolt_olb extends TCBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TCBolt_olb.class);
    public TCBolt_olb(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TCBolt_olb(int fid){
        super(LOG,fid,null);
    }
}
