package common.bolts.transactional.ed.es;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESBolt_olb extends ESBolt{
    private static final Logger LOG= LoggerFactory.getLogger(ESBolt_olb.class);
    public ESBolt_olb(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public ESBolt_olb(int fid){
        super(LOG,fid,null);
    }
}
