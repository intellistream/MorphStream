package benchmark.datagenerator;

import common.collections.Configuration;
import common.collections.OsUtils;

public abstract class DataGeneratorConfig {
    private Integer totalEvents;
    private Integer totalThreads;
    private String scheduler;
    private String rootPath;
    private int nKeyStates;
    private String idsPath;
    private Boolean shufflingActive;

    public void initialize(Configuration config) {
        this.setTotalEvents(config.getInt("totalEvents"));
        if (config.getBoolean("multicoreEvaluation")) {
            this.setTotalThreads(config.getInt("maxThreads"));
        } else {
            this.setTotalThreads(config.getInt("tthread"));
        }
        this.setScheduler(config.getString("scheduler"));
        this.setRootPath(config.getString("rootFilePath") + OsUtils.OS_wrapper("inputs"));
        this.setnKeyStates(config.getInt("NUM_ITEMS"));
        this.setIdsPath(this.getRootPath());
        this.setShufflingActive(false);
    }

    public Integer getTotalEvents() {
        return totalEvents;
    }

    public void setTotalEvents(Integer totalEvents) {
        this.totalEvents = totalEvents;
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
