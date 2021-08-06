package benchmark.datagenerator.apps.SL.OCScheduler;

import benchmark.datagenerator.DataGeneratorConfig;
import common.collections.Configuration;
import common.tools.ZipfGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generator Config that is tunable for comprehensive experiments
 */
public class DataGeneratorConfigForOC extends DataGeneratorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorConfigForOC.class);
    public Integer numberOfDLevels;
    public Boolean shufflingActive;
    public String fanoutDist;
    public String idGenType;
    public float[] dependenciesDistributionForLevels;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        this.numberOfDLevels = config.getInt("numberOfDLevels");
        this.shufflingActive = false;
        this.fanoutDist = config.getString("fanoutDist");
        this.idGenType = config.getString("idGenType");

        this.updateDependencyLevels();
    }

    public void updateDependencyLevels() {

        dependenciesDistributionForLevels = new float[numberOfDLevels];
        if (fanoutDist.equals("uniform")) {
            for (int index = 0; index < numberOfDLevels; index++) {
                dependenciesDistributionForLevels[index] = 1f / (numberOfDLevels * 1.0f);
            }
        } else if (fanoutDist.equals("zipfinv")) {
            ZipfGenerator zipf = new ZipfGenerator(numberOfDLevels, 1);
            for (int index = 0; index < numberOfDLevels; index++) {
                dependenciesDistributionForLevels[index] = (float) zipf.getProbability(numberOfDLevels - index);
            }
        } else if (fanoutDist.equals("zipf")) {
            ZipfGenerator zipf = new ZipfGenerator(numberOfDLevels, 1);
            for (int index = 0; index < numberOfDLevels; index++) {
                dependenciesDistributionForLevels[index] = (float) zipf.getProbability(index + 1);
            }
        } else if (fanoutDist.equals("zipfcenter")) {
            ZipfGenerator zipf = new ZipfGenerator(numberOfDLevels / 2, 1);
            for (int index = 0; index < numberOfDLevels / 2; index++) {
                dependenciesDistributionForLevels[index] = (float) zipf.getProbability(index + 1);
            }
            for (int index = numberOfDLevels / 2; index < numberOfDLevels; index++) {
                dependenciesDistributionForLevels[index] = (float) zipf.getProbability(numberOfDLevels - index);
            }
        } else {
            throw new UnsupportedOperationException("Invalid fanout scheme.");
        }

        LOG.info(String.format("totalEventsPerBatch: %d", tuplesPerBatch));
        LOG.info(String.format("numberOfBatches: %d", totalBatches));
        LOG.info(String.format("numberOfDLevels: %d", numberOfDLevels));
        LOG.info(String.format("rootFilePath: %s", rootPath));
    }
}