package common.bolts.transactional.ed.cu;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CUBolt_LA extends CUBolt {
    private static final Logger LOG= LoggerFactory.getLogger(CUBolt_LA.class);
    public CUBolt_LA(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public CUBolt_LA(int fid){
        super(LOG,fid,null);
    }
}
