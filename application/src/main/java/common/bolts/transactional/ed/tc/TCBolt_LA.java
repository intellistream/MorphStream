package common.bolts.transactional.ed.tc;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCBolt_LA extends TCBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TCBolt_LA.class);
    public TCBolt_LA(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TCBolt_LA(int fid){
        super(LOG,fid,null);
    }
}
