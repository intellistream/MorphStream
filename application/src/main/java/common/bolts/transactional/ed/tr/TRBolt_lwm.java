package common.bolts.transactional.ed.tr;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TRBolt_lwm extends TRBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TRBolt_lwm.class);
    public TRBolt_lwm(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TRBolt_lwm(int fid){
        super(LOG,fid,null);
    }
}
