package intellistream.morphstream.examples.utils.datagen;

import java.util.ArrayList;
import java.util.List;


/**
 * Generate dynamic workload
 * 1. workload configuration generation{@link DynamicDataGeneratorConfig}
 * 2. generate different workloads based on type, including
 * 3. control the rate of the workload shift by setting the number of events per workload configuration
 * Created by curry on 16/3/22.
 */
public abstract class DynamicWorkloadGenerator extends DataGenerator {
    protected List<String> tranToDecisionConf = new ArrayList<>();
    protected DynamicDataGeneratorConfig dynamicDataConfig;

    public DynamicWorkloadGenerator(DynamicDataGeneratorConfig dynamicDataConfig) {
        super(dynamicDataConfig);
        this.dynamicDataConfig = dynamicDataConfig;
    }

    /**
     * Map the workload characteristics to the TPG properties
     * Each application may be different
     */
    public abstract void mapToTPGProperties();

    @Override
    public void generateStream() {
        //Init the Configuration
        for (int tupleNumber = 0; tupleNumber < nTuples + dynamicDataConfig.getTotalThreads(); tupleNumber++) {
            if (tupleNumber % (dynamicDataConfig.getCheckpoint_interval() * dynamicDataConfig.getShiftRate() * dynamicDataConfig.getTotalThreads()) == 0) {
                String type = dynamicDataConfig.nextDataGeneratorConfig();
                if (type != null) {
                    switchConfiguration(type);
                }
            }
            generateTuple();
        }
    }

    public abstract void switchConfiguration(String type);

    @Override
    public DynamicDataGeneratorConfig getDataConfig() {
        return dynamicDataConfig;
    }

    @Override
    public List<String> getTranToDecisionConf() {
        return tranToDecisionConf;
    }

    @Override
    public void generateTPGProperties() {
        String type = dynamicDataConfig.nextDataGeneratorConfig();
        while (type != null) {
            switchConfiguration(type);
            type = dynamicDataConfig.nextDataGeneratorConfig();
        }
    }
}
