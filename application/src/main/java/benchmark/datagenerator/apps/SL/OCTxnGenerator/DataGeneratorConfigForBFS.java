package benchmark.datagenerator.apps.SL.OCTxnGenerator;

import benchmark.datagenerator.DataGeneratorConfig;
import common.collections.Configuration;
import common.tools.ZipfGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generator Config that is tunable for comprehensive experiments
 */
public class DataGeneratorConfigForBFS extends DataGeneratorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorConfigForBFS.class);
    private Integer numberOfDLevels;
    private Boolean shufflingActive;
    private String fanoutDist;
    private String idGenType;
    private float[] dependenciesDistributionForLevels;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        this.setNumberOfDLevels(config.getInt("numberOfDLevels"));
        this.setShufflingActive(false);
        this.setFanoutDist(config.getString("fanoutDist"));
        this.setIdGenType(config.getString("idGenType"));

        this.updateDependencyLevels();
    }

    public void updateDependencyLevels() {

        setDependenciesDistributionForLevels(new float[getNumberOfDLevels()]);
        if (getFanoutDist().equals("uniform")) {
            for (int index = 0; index < getNumberOfDLevels(); index++) {
                getDependenciesDistributionForLevels()[index] = 1f / (getNumberOfDLevels() * 1.0f);
            }
        } else if (getFanoutDist().equals("zipfinv")) {
            ZipfGenerator zipf = new ZipfGenerator(getNumberOfDLevels(), 1);
            for (int index = 0; index < getNumberOfDLevels(); index++) {
                getDependenciesDistributionForLevels()[index] = (float) zipf.getProbability(getNumberOfDLevels() - index);
            }
        } else if (getFanoutDist().equals("zipf")) {
            ZipfGenerator zipf = new ZipfGenerator(getNumberOfDLevels(), 1);
            for (int index = 0; index < getNumberOfDLevels(); index++) {
                getDependenciesDistributionForLevels()[index] = (float) zipf.getProbability(index + 1);
            }
        } else if (getFanoutDist().equals("zipfcenter")) {
            ZipfGenerator zipf = new ZipfGenerator(getNumberOfDLevels() / 2, 1);
            for (int index = 0; index < getNumberOfDLevels() / 2; index++) {
                getDependenciesDistributionForLevels()[index] = (float) zipf.getProbability(index + 1);
            }
            for (int index = getNumberOfDLevels() / 2; index < getNumberOfDLevels(); index++) {
                getDependenciesDistributionForLevels()[index] = (float) zipf.getProbability(getNumberOfDLevels() - index);
            }
        } else {
            throw new UnsupportedOperationException("Invalid fanout scheme.");
        }

        LOG.info(String.format("totalEventsPerBatch: %d", getTuplesPerBatch()));
        LOG.info(String.format("numberOfBatches: %d", getTotalBatches()));
        LOG.info(String.format("numberOfDLevels: %d", getNumberOfDLevels()));
        LOG.info(String.format("rootFilePath: %s", getRootPath()));
    }

    public Integer getNumberOfDLevels() {
        return numberOfDLevels;
    }

    public void setNumberOfDLevels(Integer numberOfDLevels) {
        this.numberOfDLevels = numberOfDLevels;
    }

    public Boolean getShufflingActive() {
        return shufflingActive;
    }

    public void setShufflingActive(Boolean shufflingActive) {
        this.shufflingActive = shufflingActive;
    }

    public String getFanoutDist() {
        return fanoutDist;
    }

    public void setFanoutDist(String fanoutDist) {
        this.fanoutDist = fanoutDist;
    }

    public String getIdGenType() {
        return idGenType;
    }

    public void setIdGenType(String idGenType) {
        this.idGenType = idGenType;
    }

    public float[] getDependenciesDistributionForLevels() {
        return dependenciesDistributionForLevels;
    }

    public void setDependenciesDistributionForLevels(float[] dependenciesDistributionForLevels) {
        this.dependenciesDistributionForLevels = dependenciesDistributionForLevels;
    }
}