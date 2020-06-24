package sesame.optimization.model;
import common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
public class cache implements Serializable {
    private static final long serialVersionUID = -862200437569790847L;
    private final static Logger LOG = LoggerFactory.getLogger(cache.class);
    private cache backup;
    private double inputRate;
    private HashMap<String, Double> outputRate;//output rate @ each output stream.
    private double processRate;
    private double bounded_processRate;
    private double expected_processRate;
    private double Cycles;
    private double Memory;
    private double Trans;
    private HashMap<Integer, Double> sub_Cycles;
    private HashMap<Integer, Double> sub_inputRate;//<src, inputrate> r_i(s,c)
    private HashMap<Integer, HashMap<String, Double>> sub_outputRate;
    private HashMap<Integer, Double> sub_processRate;
    private HashMap<Integer, Double> sub_expected_processRate;
    private cache(cache _c) {
        this(_c.inputRate, _c.outputRate, _c.processRate, _c.sub_inputRate,
                _c.sub_outputRate, _c.sub_processRate, _c.Cycles, _c.Memory, _c.Trans, _c.sub_Cycles);
    }
    private cache(double inputRate, HashMap<String, Double> outputRate, double processRate,
                  HashMap<Integer, Double> sub_inputRate, HashMap<Integer, HashMap<String, Double>> sub_outputRate,
                  HashMap<Integer, Double> sub_processRate, double cycles, double memory, double trans, HashMap<Integer, Double> sub_Cycles) {
        LOG.info("Restore cache");
        this.inputRate = inputRate;
        this.outputRate = outputRate;
        this.processRate = processRate;
        this.Cycles = cycles;
        this.Memory = memory;
        this.Trans = trans;
        this.sub_inputRate = sub_inputRate;
        this.sub_outputRate = sub_outputRate;
        this.sub_processRate = sub_processRate;
        this.sub_Cycles = sub_Cycles;
    }
    public cache() {
        clean();
    }
    public double getMemory() {
        return Memory;
    }
    public void setMemory(double memory) {
        Memory = memory;
    }
    public double getInputRate() {
        return inputRate;
    }
    public void setInputRate(double inputRate) {
        this.inputRate = inputRate;
    }
    public void clearInputRate() {
        this.inputRate = -1;
    }
    public double getOutputRate(String ostream) {
        if (outputRate.isEmpty() || outputRate.get(ostream) == null) return -1;//cache not set.
        return outputRate.get(ostream);
    }
    public double getOutputRate() {
        return outputRate.get(Constants.DEFAULT_STREAM_ID);
    }
    public void setOutputRate(double outputRate) {
        this.outputRate.put(Constants.DEFAULT_STREAM_ID, outputRate);
    }
    public void setOutputRate(double outputRate, String ostream) {
        this.outputRate.put(ostream, outputRate);
    }
    public double getProcessRate() {
        return processRate;
    }
    public void setProcessRate(double processRate) {
//        if(Double.isNaN(processRate))
//            System.exit(-1);
        this.processRate = processRate;
    }
    public double getBounded_processRate() {
        return bounded_processRate;
    }
    public void setBounded_processRate(double bounded_processRate) {
        this.bounded_processRate = bounded_processRate;
    }
    public double getExpected_processRate() {
        return expected_processRate;
    }
    public void setExpected_processRate(double expected_processRate) {
        this.expected_processRate = expected_processRate;
    }
    public HashMap<Integer, Double> getSub_inputRate() {
        return sub_inputRate;
    }
    public void setSub_inputRate(HashMap<Integer, Double> sub_inputRate) {
        this.sub_inputRate = sub_inputRate;
    }
    public HashMap<Integer, HashMap<String, Double>> getSub_outputRate() {
        return sub_outputRate;
    }
    public void setSub_outputRate(HashMap<Integer, HashMap<String, Double>> sub_outputRate) {
        this.sub_outputRate = sub_outputRate;
    }
    public HashMap<Integer, Double> getSub_processRate() {
        return sub_processRate;
    }
    public void setSub_processRate(HashMap<Integer, Double> sub_processRate) {
        this.sub_processRate = sub_processRate;
    }
    public HashMap<Integer, Double> getSub_expected_processRate() {
        return sub_expected_processRate;
    }
    public void setSub_expected_processRate(HashMap<Integer, Double> sub_expected_processRate) {
        this.sub_expected_processRate = sub_expected_processRate;
    }
    public double getCycles() {
        return Cycles;
    }
    public void setCycles(double cycles) {
        Cycles = cycles;
    }
    public HashMap<Integer, Double> getSub_Cycles() {
        return sub_Cycles;
    }
    public void setSub_Cycles(HashMap<Integer, Double> sub_Cycles) {
        this.sub_Cycles = sub_Cycles;
    }
    public void clean_cycles() {
        Cycles = -1;
        sub_Cycles = new HashMap<>();
    }
    public void clean() {
        //LOG.info("clean_executorInformation input rate to be:" + inputRate);
        inputRate = -1;
        outputRate = new HashMap<>();
        processRate = -1;
        bounded_processRate = -1;
        expected_processRate = -1;
        Cycles = -1;
        Memory = -1;
        Trans = -1;
        sub_inputRate = new HashMap<>();
        sub_outputRate = new HashMap<>();
        sub_processRate = new HashMap<>();
        sub_expected_processRate = new HashMap<>();
        sub_Cycles = new HashMap<>();
    }
    public void clean_ExceptsubInput() {
        //LOG.info("clean_executorInformation input rate to be:" + inputRate);
        inputRate = -1;
        outputRate = new HashMap<>();
        processRate = -1;
        bounded_processRate = -1;
        expected_processRate = -1;
        Cycles = -1;
        sub_outputRate = new HashMap<>();
        sub_processRate = new HashMap<>();
        sub_expected_processRate = new HashMap<>();
        sub_Cycles = new HashMap<>();
    }
    public void store() {
        backup = new cache(this);
    }
    public void restore() {
        LOG.info("restore input rate to be:" + backup.inputRate);
        this.inputRate = backup.inputRate;
        this.outputRate = backup.outputRate;
        this.processRate = backup.processRate;
        this.sub_inputRate = backup.sub_inputRate;
        this.sub_outputRate = backup.sub_outputRate;
        this.sub_processRate = backup.sub_processRate;
        this.Cycles = backup.Cycles;
        this.Memory = backup.Memory;
        this.sub_Cycles = backup.sub_Cycles;
    }
    public boolean alert() {
        return !(this.inputRate != backup.inputRate) && this.outputRate == backup.outputRate && !(this.processRate != backup.processRate) && this.Cycles == backup.Cycles;
    }
    public double getTrans() {
        return Trans;
    }
    public void setTrans(double trans) {
        Trans = trans;
    }
}
