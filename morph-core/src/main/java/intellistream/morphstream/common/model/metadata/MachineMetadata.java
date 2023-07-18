package intellistream.morphstream.common.model.metadata;

import java.io.Serializable;

public class MachineMetadata implements Serializable {
    private static final long serialVersionUID = 293204186702629110L;
    private long timestamp;
    private String machineIP;
    /* values between [0, 1] */
    private double cpuIdleTime;
    private double freeMemoryPercent;

    public MachineMetadata() {
    }

    public MachineMetadata(long timestamp, String machineIP, double cpuIdleTime, double freeMemoryPercent) {
        this.timestamp = timestamp;
        this.machineIP = machineIP;
        this.cpuIdleTime = cpuIdleTime;
        this.freeMemoryPercent = freeMemoryPercent;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMachineIP() {
        return machineIP;
    }

    public void setMachineIP(String machineIP) {
        this.machineIP = machineIP;
    }

    public double getCpuIdleTime() {
        return cpuIdleTime;
    }

    public void setCpuIdleTime(double cpuIdleTime) {
        this.cpuIdleTime = cpuIdleTime;
    }

    public double getFreeMemoryPercent() {
        return freeMemoryPercent;
    }

    public void setFreeMemoryPercent(double freeMemoryPercent) {
        this.freeMemoryPercent = freeMemoryPercent;
    }

    @Override
    public String toString() {
        return "MachineMetadata{" + "timestamp=" + timestamp + ", machineIP=" + machineIP + ", cpuIdleTime=" + cpuIdleTime + ", freeMemoryPercent=" + freeMemoryPercent + '}';
    }
}