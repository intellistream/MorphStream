package common.bolts.transactional.ed.cu;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CUBolt_olb extends CUBolt{
    private static final Logger LOG= LoggerFactory.getLogger(CUBolt_olb.class);
    public CUBolt_olb(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public CUBolt_olb(int fid){
        super(LOG,fid,null);
    }
}
