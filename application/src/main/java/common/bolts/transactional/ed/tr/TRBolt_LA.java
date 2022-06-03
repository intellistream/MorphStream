package common.bolts.transactional.ed.tr;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TRBolt_LA extends TRBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TRBolt_ts.class);
    public TRBolt_LA(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TRBolt_LA(int fid){
        super(LOG,fid,null);
    }
}
