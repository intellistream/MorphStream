package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCGBolt_lwm extends TCGBolt {
    private static final Logger LOG= LoggerFactory.getLogger(TCGBolt_lwm.class);
    public TCGBolt_lwm(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TCGBolt_lwm(int fid){
        super(LOG,fid,null);
    }
}
