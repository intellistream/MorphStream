package common.bolts.transactional.ed.fu;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FUBolt_nocc extends FUBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TRBolt_ts.class);
    public FUBolt_nocc(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public FUBolt_nocc(int fid){
        super(LOG,fid,null);
    }
}
