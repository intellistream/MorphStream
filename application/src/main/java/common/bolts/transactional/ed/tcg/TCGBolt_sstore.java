package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCGBolt_sstore extends TCGBolt {
    private static final Logger LOG= LoggerFactory.getLogger(TCGBolt_sstore.class);
    public TCGBolt_sstore(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TCGBolt_sstore(int fid){
        super(LOG,fid,null);
    }
}
