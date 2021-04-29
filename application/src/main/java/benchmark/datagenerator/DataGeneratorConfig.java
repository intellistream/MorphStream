package benchmark.datagenerator;

import com.beust.jcommander.Parameter;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.tools.ZipfGenerator;

public class DataGeneratorConfig {

    public Integer tuplesPerBatch;
    public Integer totalBatches;
    public Integer numberOfDLevels;
    public Boolean shufflingActive;
    public Integer totalThreads;
    public String scheduler;
    public String fanoutDist;
    public String idGenType;

    public String rootPath;
    public String idsPath;

    public float[] dependenciesDistributionForLevels;

    public void initialize(Configuration config) {
        this.tuplesPerBatch = config.getInt("totalEventsPerBatch");
        this.totalBatches = config.getInt("numberOfBatches");
        this.numberOfDLevels = config.getInt("numberOfDLevels");
        this.shufflingActive = false;
        this.totalThreads = config.getInt("tthread");
        this.scheduler = config.getString("scheduler");
        this.fanoutDist = config.getString("fanoutDist");
        this.idGenType = config.getString("idGenType");
        this.rootPath = config.getString("rootFilePath");

        this.idsPath = this.rootPath;
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

        System.out.println("");
        System.out.println(String.format("totalEventsPerBatch: %d", tuplesPerBatch));
        System.out.println(String.format("numberOfBatches: %d", totalBatches));
        System.out.println(String.format("numberOfDLevels: %d", numberOfDLevels));
        System.out.println(String.format("rootFilePath: %s", rootPath));
    }

}