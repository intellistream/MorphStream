package common.bolts.transactional.ed.cu;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CUBolt_nocc extends CUBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TRBolt_ts.class);
    public CUBolt_nocc(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public CUBolt_nocc(int fid){
        super(LOG,fid,null);
    }
}