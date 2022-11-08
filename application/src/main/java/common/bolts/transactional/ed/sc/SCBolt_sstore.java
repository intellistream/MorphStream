package common.bolts.transactional.ed.sc;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SCBolt_sstore extends SCBolt{
    private static final Logger LOG= LoggerFactory.getLogger(SCBolt_sstore.class);
    public SCBolt_sstore(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public SCBolt_sstore(int fid){
        super(LOG,fid,null);
    }
}
