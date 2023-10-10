package intellistream.morphstream.engine.stream.execution.runtime.collector.impl;

import intellistream.morphstream.engine.stream.components.TopologyComponent;

import java.util.HashMap;

public class MetaGroup {
    private final int taskId;
    HashMap<TopologyComponent, Meta> map = new HashMap<>();//every children op corresponds to one common.meta.

    public MetaGroup(int taskId) {
        this.taskId = taskId;
    }

    public Meta get(TopologyComponent childOP) {
        return map.get(childOP);
    }

    public void put(TopologyComponent childrenOP, Meta meta) {
        map.put(childrenOP, meta);
    }
}
