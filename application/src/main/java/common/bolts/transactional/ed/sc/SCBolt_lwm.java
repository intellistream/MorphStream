package common.bolts.transactional.ed.sc;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SCBolt_lwm extends SCBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TRBolt_ts.class);
    public SCBolt_lwm(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public SCBolt_lwm(int fid){
        super(LOG,fid,null);
    }
}