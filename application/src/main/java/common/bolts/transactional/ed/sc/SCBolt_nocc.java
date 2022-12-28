package common.bolts.transactional.ed.sc;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SCBolt_nocc extends SCBolt {
    private static final Logger LOG= LoggerFactory.getLogger(SCBolt_nocc.class);
    public SCBolt_nocc(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public SCBolt_nocc(int fid){
        super(LOG,fid,null);
    }
}
