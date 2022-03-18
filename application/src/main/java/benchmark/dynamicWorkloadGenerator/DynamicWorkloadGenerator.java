package benchmark.dynamicWorkloadGenerator;

import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.DataGeneratorConfig;


/**
 * Generate dynamic workload
 * 1. workload configuration generation{@link DynamicDataGeneratorConfig}
 * 2. generate different workloads based on type, including
 * 3. control the rate of the workload shift by setting the number of events per workload configuration
 * Created by curry on 16/3/22.
 */
public abstract class DynamicWorkloadGenerator extends DataGenerator {
    protected DynamicDataGeneratorConfig dynamicDataConfig;
    public DynamicWorkloadGenerator(DynamicDataGeneratorConfig dynamicDataConfig) {
        super(dynamicDataConfig);
        this.dynamicDataConfig=dynamicDataConfig;
    }

    private String[] tranToDecisionConf() {
        return null;
    }

    @Override
    public void generateStream() {
        //Init the Configuration
        switchConfiguration();
        for (int tupleNumber = 0; tupleNumber < nTuples + dynamicDataConfig.getTotalThreads(); tupleNumber++) {
            if (tupleNumber%dynamicDataConfig.getCheckpoint_interval()* dynamicDataConfig.getShiftRate()* dynamicDataConfig.getTotalThreads() == 0) {
                if (dynamicDataConfig.nextDataGeneratorConfig()) {
                    switchConfiguration();
                }
            }
            generateTuple();
        }
    }
    public abstract void switchConfiguration();

    @Override
    public DynamicDataGeneratorConfig getDataConfig() {
        return dynamicDataConfig;
    }
}
