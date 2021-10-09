package benchmark.datagenerator.apps.SL.TPGTxnGenerator;

import benchmark.datagenerator.DataGeneratorConfig;
import common.collections.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generator Config that is tunable for comprehensive experiments
 */
public class TPGDataGeneratorConfig extends DataGeneratorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(TPGDataGeneratorConfig.class);

    public int Ratio_Of_Deposit;
    public int State_Access_Skewness;
    public int Ratio_of_Overlapped_Keys;


    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        Ratio_Of_Deposit = config.getInt("Ratio_Of_Deposit");
        State_Access_Skewness = config.getInt("State_Access_Skewness");
        Ratio_of_Overlapped_Keys = config.getInt("Ratio_of_Overlapped_Keys");
    }
}