package benchmark.datagenerator.apps.GS.TPGTxnGenerator;

import benchmark.datagenerator.DataGeneratorConfig;
import common.collections.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generator Config that is tunable for comprehensive experiments
 */
public class GSTPGDataGeneratorConfig extends DataGeneratorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(GSTPGDataGeneratorConfig.class);

    public int State_Access_Skewness;
    public int NUM_ACCESS;
    public int Ratio_of_Overlapped_Keys;
    public int Ratio_of_Transaction_Aborts;
    public int Transaction_Length;
    public int Ratio_of_Multiple_State_Access;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        NUM_ACCESS = config.getInt("NUM_ACCESS", 0);
        State_Access_Skewness = config.getInt("State_Access_Skewness", 0);
        Ratio_of_Overlapped_Keys = config.getInt("Ratio_of_Overlapped_Keys", 0);
        Ratio_of_Transaction_Aborts = config.getInt("Ratio_of_Transaction_Aborts", 0);
        Transaction_Length = config.getInt("Transaction_Length", 1);
        Ratio_of_Multiple_State_Access = config.getInt("Ratio_of_Multiple_State_Access",100);
    }
}