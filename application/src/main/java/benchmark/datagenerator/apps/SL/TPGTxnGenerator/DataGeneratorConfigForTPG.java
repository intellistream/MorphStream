package benchmark.datagenerator.apps.SL.TPGTxnGenerator;

import benchmark.datagenerator.DataGeneratorConfig;
import common.collections.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generator Config that is tunable for comprehensive experiments
 */
public class DataGeneratorConfigForTPG extends DataGeneratorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorConfigForTPG.class);

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
    }
}