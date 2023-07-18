package benchmark.dynamicWorkloadGenerator;

import benchmark.datagenerator.DataGeneratorConfig;
import intellistream.morphstream.configuration.Configuration;

/**
 * Generate dynamic workload configuration
 * 1. workload configuration generation
 * Created by curry on 16/3/22.
 */
public class DynamicDataGeneratorConfig extends DataGeneratorConfig {
    public int State_Access_Skewness;
    public int NUM_ACCESS;
    public int Ratio_of_Overlapped_Keys;
    public int Ratio_of_Transaction_Aborts;
    public int Transaction_Length;
    public int Ratio_Of_Deposit;
    public int Ratio_Of_Buying;
    public boolean enableGroup;
    public int Ratio_of_Multiple_State_Access;
    public int Ratio_of_Non_Deterministic_State_Access;
    public String skewGroup;
    public int groupNum;
    public int Ratio_of_Transaction_Aborts_Highest;
    /* The type of the dynamic workload */
    private String[] phaseType;
    /* Application, the configuration maybe different for the same type of workload */
    private String app_name;
    /* Control the rate of the workload*/
    private int shiftRate;
    /* The workload is in which phase */
    private int phase;
    private int checkpoint_interval;

    public void initialize(Configuration config) {
        super.initialize(config);
        this.app_name = config.getString("app_name");
        this.shiftRate = config.getInt("shiftRate");
        this.phaseType = config.getString("workloadType").split(",");
        this.checkpoint_interval = config.getInt("checkpoint");
        enableGroup = config.getBoolean("isGroup");
        NUM_ACCESS = config.getInt("NUM_ACCESS", 0);
        State_Access_Skewness = config.getInt("State_Access_Skewness", 0);
        Ratio_of_Overlapped_Keys = config.getInt("Ratio_of_Overlapped_Keys", 0);
        Ratio_of_Transaction_Aborts = config.getInt("Ratio_of_Transaction_Aborts", 0);
        Transaction_Length = config.getInt("Transaction_Length", 1);
        Ratio_Of_Deposit = config.getInt("Ratio_Of_Deposit", 0);
        Ratio_Of_Buying = config.getInt("Ratio_Of_Buying", 0);
        Ratio_of_Multiple_State_Access = config.getInt("Ratio_of_Multiple_State_Access", 100);
        Ratio_of_Non_Deterministic_State_Access = config.getInt("Ratio_of_Non_Deterministic_State_Access", 0);
        phase = 0;
        if (enableGroup) {
            skewGroup = config.getString("skewGroup");
            groupNum = config.getInt("groupNum");
            Ratio_of_Transaction_Aborts_Highest = config.getInt("Ratio_of_Transaction_Aborts_Highest");
        }
    }

    public int getCheckpoint_interval() {
        return checkpoint_interval;
    }

    public int getShiftRate() {
        return shiftRate;
    }

    /**
     * Generate the configuration based on type and application
     *
     * @return
     */
    public String nextDataGeneratorConfig() {
        String phaseType;
        if (phase < this.phaseType.length) {
            phaseType = this.phaseType[phase];
            phase++;
        } else {
            return null;
        }
        switch (phaseType) {
            case "default":
            case "unchanging":
                return phaseType;
            case "Up_skew":
                if (this.State_Access_Skewness + 20 <= 100) {
                    this.State_Access_Skewness = this.State_Access_Skewness + 20;
                    return "skew";
                } else {
                    return "unchanging";
                }
            case "Down_skew":
                if (this.State_Access_Skewness - 20 >= 0) {
                    this.State_Access_Skewness = this.State_Access_Skewness - 20;
                    return "skew";
                } else {
                    return "unchanging";
                }
            case "Up_PD":
                if (this.app_name.equals("StreamLedger")) {
                    if (this.Ratio_Of_Deposit - 20 >= 0) {
                        if (this.Ratio_Of_Deposit > 45) {
                            this.Ratio_Of_Deposit = 45;
                        } else {
                            this.Ratio_Of_Deposit = this.Ratio_Of_Deposit - 20;
                        }
                        return "PD";
                    } else {
                        return "unchanging";
                    }
                }
            case "Down_PD":
                if (this.app_name.equals("StreamLedger")) {
                    if (this.Ratio_Of_Deposit + 30 <= 100) {
                        this.Ratio_Of_Deposit = this.Ratio_Of_Deposit + 30;
                        return "PD";
                    } else {
                        return "unchanging";
                    }
                }
            case "Up_abort":
                if (this.Ratio_of_Transaction_Aborts + 2000 <= 10000) {
                    this.Ratio_of_Transaction_Aborts = this.Ratio_of_Transaction_Aborts + 2000;
                    return "abort";
                } else {
                    return "unchanging";
                }
            case "Down_abort":
                if (this.Ratio_of_Transaction_Aborts - 2000 >= 0) {
                    this.Ratio_of_Transaction_Aborts = this.Ratio_of_Transaction_Aborts - 2000;
                    return "abort";
                } else {
                    return "unchanging";
                }
            default:
                return null;
        }
    }

    public String getApp() {
        return app_name;
    }

    //User defined
    private String setConfigurationForSL() {
        switch (phase) {
            case 0:
                phase++;
                return "default";
            case 1:
            case 4:
                phase++;
                return "unchanging";
            case 2:
            case 3:
                phase++;
                this.State_Access_Skewness = this.State_Access_Skewness + 30;
                return "skew";
            case 5:
            case 6:
            case 7:
                phase++;
                this.Ratio_Of_Deposit = this.Ratio_Of_Deposit + 20;
                return "PD";
            default:
                return null;
        }
    }

    private String setConfigurationForOB() {
        switch (phase) {
            case 0:
                phase++;
                return "default";
            case 1:
                phase++;
                this.State_Access_Skewness = 85;// 100
                return "skew";
            case 2:
                phase++;
                this.Ratio_of_Transaction_Aborts = 8000;// 10000
                return "abort";
            default:
                return null;
        }
    }

    private String setConfigurationForGS() {
        switch (phase) {
            case 0:
                phase++;
                return "default";
            case 1:
                phase++;
                this.Transaction_Length = 8;
                return "LD";
            case 2:
                phase++;
                this.Ratio_of_Transaction_Aborts = 8000;// 10000
                this.State_Access_Skewness = 85;
                return "isCyclic";
            case 3:
                phase++;
                return "complexity";
            default:
                return null;
        }
    }

    private String setConfigurationForTP() {
        if (phase == 0) {
            phase++;
            return "default";
        } else {
            return null;
        }
    }
}
