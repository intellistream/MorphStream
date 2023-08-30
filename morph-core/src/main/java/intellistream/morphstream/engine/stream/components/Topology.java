package intellistream.morphstream.engine.stream.components;

import intellistream.morphstream.common.constants.BaseConstants;
import intellistream.morphstream.common.io.Enums.platform.Platform;
import intellistream.morphstream.configuration.Constants;
import intellistream.morphstream.engine.stream.controller.input.InputStreamController;
import intellistream.morphstream.engine.stream.topology.TransactionTopology;
import intellistream.morphstream.engine.txn.db.Database;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.transaction.TableInitilizer;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * Class to d_record a Topology description
 * Topology build (user side) -> (system side) build children link.
 * -> Topology Compile to GetAndUpdate Brisk.execution graph -> link executor to each Brisk.topology component.
 */
public class Topology implements Serializable {
    private static final long serialVersionUID = 42L;
    /**
     * <Operator ID, Operator>
     */
    private final LinkedHashMap<String, TopologyComponent> records;
    public Database db;
    public TransactionTopology txnTopology;
    public SpinLock[] spinlock;
    public TableInitilizer tableinitilizer;
    private TopologyComponent sink;
    /**
     * global scheduler template..
     */
    private InputStreamController scheduler;
    private Platform p;

    public Topology() {
        //keep records in insertion order..
        records = new LinkedHashMap<>();
    }

    public Topology(Topology topology) {
        //keep records in insertion order..
        records = new LinkedHashMap<>();
        for (TopologyComponent topo : topology.getRecords().values()) {
            MultiStreamComponent component = new MultiStreamComponent(topo, this);//copy a new component
            addRecord(component);
            if (topo.type == Constants.sinkType) {
                this.sink = component;
            }
        }
        this.scheduler = topology.getScheduler();
        this.p = topology.getPlatform();
    }

    /**
     * Add element(spout/bolt) in Brisk.topology
     *
     * @param rec d_record
     */
    public void addRecord(TopologyComponent rec) {
        records.put(rec.getId(), rec);
    }

    public TopologyComponent getRecord(String componentID) {
        return records.get(componentID);
    }

    public LinkedHashMap<String, TopologyComponent> getRecords() {
        return records;
    }

    public void setSink(TopologyComponent sink) {
        this.sink = sink;
    }

    public TopologyComponent getComponent(String componentId) {
        return records.get(componentId);
    }

    public InputStreamController getScheduler() {
        return scheduler;
    }

    public void setScheduler(InputStreamController sequentialScheduler) {
        scheduler = sequentialScheduler;
    }

    public void clean_executorInformation() {
        for (TopologyComponent topo : getRecords().values()) {
            topo.clean();
        }
    }

    public void addMachine(Platform p) {
        this.p = p;
    }

    public Platform getPlatform() {
        return p;
    }

    public String getPrefix() {
        return records.get(BaseConstants.BaseComponent.SPOUT).getOp().getConfigPrefix();
    }
}
