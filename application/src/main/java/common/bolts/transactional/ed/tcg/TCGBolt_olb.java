package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCGBolt_olb extends TCGBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TCGBolt_olb.class);
    public TCGBolt_olb(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TCGBolt_olb(int fid){
        super(LOG,fid,null);
    }
}
