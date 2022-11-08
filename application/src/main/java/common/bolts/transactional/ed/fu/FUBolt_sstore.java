package common.bolts.transactional.ed.fu;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FUBolt_sstore extends FUBolt{
    private static final Logger LOG= LoggerFactory.getLogger(FUBolt_sstore.class);
    public FUBolt_sstore(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public FUBolt_sstore(int fid){
        super(LOG,fid,null);
    }
}
