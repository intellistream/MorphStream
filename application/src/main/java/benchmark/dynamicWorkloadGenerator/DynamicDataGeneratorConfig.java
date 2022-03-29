package benchmark.dynamicWorkloadGenerator;

import benchmark.datagenerator.DataGeneratorConfig;
import common.collections.Configuration;
import utils.AppConfig;

/**
 * Generate dynamic workload configuration
 * 1. workload configuration generation
 * Created by curry on 16/3/22.
 */
public class DynamicDataGeneratorConfig extends DataGeneratorConfig {
    /* The type of the dynamic workload */
    private String type;
    /* Application, the configuration maybe different for the same type of workload */
    private String app;
    /* Control the rate of the workload*/
    private int shiftRate;
    /* The workload is in which phase */
    private int phase;
    private int checkpoint_interval;
    public int State_Access_Skewness;
    public int NUM_ACCESS;
    public int Ratio_of_Overlapped_Keys;
    public int Ratio_of_Transaction_Aborts;
    public int Transaction_Length;
    public int Ratio_Of_Deposit;
    public int Ratio_Of_Buying;

    public void initialize(Configuration config) {
        super.initialize(config);
        this.app = config.getString("application");
        this.type = config.getString("workloadType");
        this.shiftRate = config.getInt("shiftRate");
        this.checkpoint_interval = config.getInt("checkpoint");
        NUM_ACCESS = config.getInt("NUM_ACCESS", 0);
        State_Access_Skewness = config.getInt("State_Access_Skewness", 0);
        Ratio_of_Overlapped_Keys = config.getInt("Ratio_of_Overlapped_Keys", 0);
        Ratio_of_Transaction_Aborts = config.getInt("Ratio_of_Transaction_Aborts", 0);
        Transaction_Length = config.getInt("Transaction_Length", 1);
        Ratio_Of_Deposit = config.getInt("Ratio_Of_Deposit", 0);
        Ratio_Of_Buying=config.getInt("Ratio_Of_Buying",0);
        phase = 0;
    }

    public int getCheckpoint_interval() {
        return checkpoint_interval;
    }

    public int getShiftRate() {
        return shiftRate;
    }
    /**
     * Generate the configuration based on type and application
     * @return
     */
    public String nextDataGeneratorConfig() {
        switch (type){
            case "0" :
                return setConfigurationForSL();
            case "1" :
                return setConfigurationForOB();
            case "2" :
                return setConfigurationForGS();
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    public String getType() {
        return type;
    }

    public String getApp() {
        return app;
    }

    private String setConfigurationForSL() {
        switch (phase){
            case 0:
                phase++;
                return "default";
            case 1:
                phase++;
                this.State_Access_Skewness = 85;
                return "skew";
            case 2:
                phase++;
                this.Ratio_Of_Deposit = 80;
                return "PD";
            default:
                return null;
        }
    }
    private String setConfigurationForOB() {
        switch (phase){
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
        switch (phase){
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
}
