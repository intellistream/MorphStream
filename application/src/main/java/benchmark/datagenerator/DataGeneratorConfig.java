package benchmark.datagenerator;

import common.collections.Configuration;

public abstract class DataGeneratorConfig {
    private Integer tuplesPerBatch;
    private Integer totalBatches;
    private Integer totalThreads;
    private String scheduler;
    private String rootPath;
    private int nKeyStates;
    private String idsPath;
    private Boolean shufflingActive;

    public void initialize(Configuration config) {
        this.setTuplesPerBatch(config.getInt("totalEventsPerBatch"));
        this.setTotalBatches(config.getInt("numberOfBatches"));
        this.setTotalThreads(config.getInt("tthread"));
        this.setScheduler(config.getString("scheduler"));
        this.setRootPath(config.getString("rootFilePath"));
        this.setnKeyStates(config.getInt("NUM_ITEMS"));
        this.setIdsPath(this.getRootPath());
        this.setShufflingActive(false);
    }

    public Integer getTuplesPerBatch() {
        return tuplesPerBatch;
    }

    public void setTuplesPerBatch(Integer tuplesPerBatch) {
        this.tuplesPerBatch = tuplesPerBatch;
    }

    public Integer getTotalBatches() {
        return totalBatches;
    }

    public void setTotalBatches(Integer totalBatches) {
        this.totalBatches = totalBatches;
    }

    public Integer getTotalThreads() {
        return totalThreads;
    }

    public void setTotalThreads(Integer totalThreads) {
        this.totalThreads = totalThreads;
    }

    public String getScheduler() {
        return scheduler;
    }

    public void setScheduler(String scheduler) {
        this.scheduler = scheduler;
    }

    public String getRootPath() {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    public int getnKeyStates() {
        return nKeyStates;
    }

    public void setnKeyStates(int nKeyStates) {
        this.nKeyStates = nKeyStates;
    }

    public String getIdsPath() {
        return idsPath;
    }

    public void setIdsPath(String idsPath) {
        this.idsPath = idsPath;
    }

    public Boolean getShufflingActive() {
        return shufflingActive;
    }

    public void setShufflingActive(Boolean shufflingActive) {
        this.shufflingActive = shufflingActive;
    }
}
