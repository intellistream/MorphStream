package common.bolts.transactional.ed.es;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESBolt_nocc extends ESBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TRBolt_ts.class);
    public ESBolt_nocc(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public ESBolt_nocc(int fid){
        super(LOG,fid,null);
    }
}
