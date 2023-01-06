package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCGBolt_LA extends TCGBolt {
    private static final Logger LOG= LoggerFactory.getLogger(TCGBolt_LA.class);
    public TCGBolt_LA(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TCGBolt_LA(int fid){
        super(LOG,fid,null);
    }
}
