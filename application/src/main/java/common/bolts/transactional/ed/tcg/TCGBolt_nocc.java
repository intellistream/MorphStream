package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCGBolt_nocc extends TCGBolt{
    private static final Logger LOG = LoggerFactory.getLogger(TCGBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public TCGBolt_nocc(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }

    public TCGBolt_nocc(int fid){
        super(LOG,fid,null);
    }
}
