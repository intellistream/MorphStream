package common.bolts.transactional.ed.cu;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CUBolt_lwm extends CUBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TRBolt_ts.class);
    public CUBolt_lwm(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public CUBolt_lwm(int fid){
        super(LOG,fid,null);
    }
}