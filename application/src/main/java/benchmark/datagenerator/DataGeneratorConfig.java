package benchmark.datagenerator;

import common.collections.Configuration;

public abstract class DataGeneratorConfig {
    public Integer tuplesPerBatch;
    public Integer totalBatches;
    public Integer totalThreads;
    public String scheduler;
    public String rootPath;
    public int nKeyStates;
    public String idsPath;

    public void initialize(Configuration config) {
        this.tuplesPerBatch = config.getInt("totalEventsPerBatch");
        this.totalBatches = config.getInt("numberOfBatches");
        this.totalThreads = config.getInt("tthread");
        this.scheduler = config.getString("scheduler");
        this.rootPath = config.getString("rootFilePath");
        this.nKeyStates = config.getInt("NUM_ITEMS");
        this.idsPath = this.rootPath;
    }
}
