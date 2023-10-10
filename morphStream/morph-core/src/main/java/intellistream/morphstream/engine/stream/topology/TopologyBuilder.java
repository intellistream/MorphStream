package intellistream.morphstream.engine.stream.topology;

import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Constants;
import intellistream.morphstream.engine.stream.components.MultiStreamComponent;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.TopologyComponent;
import intellistream.morphstream.engine.stream.components.exception.InvalidIDException;
import intellistream.morphstream.engine.stream.components.grouping.Grouping;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractBolt;
import intellistream.morphstream.engine.stream.components.operators.api.spout.AbstractSpout;
import intellistream.morphstream.engine.stream.components.operators.api.window.AbstractWindowedBolt;
import intellistream.morphstream.engine.stream.components.operators.executor.BasicBoltBatchExecutor;
import intellistream.morphstream.engine.stream.components.operators.executor.BasicSpoutBatchExecutor;
import intellistream.morphstream.engine.stream.components.operators.executor.BasicWindowBoltBatchExecutor;
import intellistream.morphstream.engine.stream.components.operators.executor.IExecutor;
import intellistream.morphstream.engine.stream.components.streaminfo;
import intellistream.morphstream.engine.stream.controller.input.InputStreamController;
import intellistream.morphstream.engine.stream.controller.input.scheduler.SequentialScheduler;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Builder pattern for Topology_components class
 */
public class TopologyBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyBuilder.class);
    private final Topology topology;
    private final Set<String> idSet;

    public TopologyBuilder() {
        topology = new Topology();
        idSet = new HashSet<>();
    }

    /**
     * addOperation spout to Brisk.topology
     *
     * @param id
     * @param s
     * @throws InvalidIDException
     */
    public TopologyBuilder setSpout(String id, AbstractSpout s, int numTasks) throws InvalidIDException {
        if (idSet.contains(id)) {
            throw new InvalidIDException("ID already taken");
        }
        idSet.add(id);
        HashMap<String, streaminfo> output_streams;
        OutputFieldsDeclarer declarer = new OutputFieldsDeclarer();
        s.declareOutputFields(declarer);
        output_streams = declarer.getFieldsDeclaration();
        TopologyComponent topologyComponent;
        topologyComponent = new MultiStreamComponent
                (id, Constants.spoutType, new BasicSpoutBatchExecutor(s), numTasks, null, output_streams, null);
        topology.addRecord(topologyComponent);
        return this;
    }

    public TopologyBuilder setBolt(String id, AbstractBolt b, int numTasks, Grouping... groups) throws InvalidIDException {
        return setBolt(Constants.boltType, id, b, numTasks, groups);
    }

    /**
     * addOperation bolt with shuffle Grouping to parent
     *
     * @param id
     * @param b
     * @param numTasks
     * @param groups   :multiple groups.
     * @throws InvalidIDException
     * @since 0.0.5 it supports multiple Grouping to different output_streams.
     */
    private TopologyBuilder setBolt(char type, String id, AbstractBolt b, int numTasks, Grouping... groups) throws InvalidIDException {
        if (idSet.contains(id)) {
            throw new InvalidIDException("ID already taken");
        }
        for (Grouping g : groups) {
            if (!idSet.contains(g.getComponentId())) {
                throw new InvalidIDException("parent ID:" + g.getComponentId() + " does not exist");
            }
        }
        idSet.add(id);
        HashMap<String, streaminfo> output_streams;
        OutputFieldsDeclarer declarer = new OutputFieldsDeclarer();
        b.declareOutputFields(declarer);
        output_streams = declarer.getFieldsDeclaration();
        final BasicBoltBatchExecutor executor = new BasicBoltBatchExecutor(b);
        setBolt(type, id, executor, output_streams, numTasks, groups);
        return this;
    }

    public TopologyBuilder setBolt(String id, AbstractWindowedBolt bolt, int numTasks, Grouping... groups) throws InvalidIDException {
        return setBolt(Constants.boltType, id, bolt, numTasks, groups);
    }

    private TopologyBuilder setBolt(char type, String id, AbstractWindowedBolt b, int numTasks, Grouping... groups) throws InvalidIDException {
        if (idSet.contains(id)) {
            throw new InvalidIDException("ID already taken");
        }
        for (Grouping g : groups) {
            if (!idSet.contains(g.getComponentId())) {
                throw new InvalidIDException("parent ID:" + g.getComponentId() + " does not exist");
            }
        }
        idSet.add(id);
        HashMap<String, streaminfo> output_streams;
        OutputFieldsDeclarer declarer = new OutputFieldsDeclarer();
        b.declareOutputFields(declarer);
        output_streams = declarer.getFieldsDeclaration();
        final BasicWindowBoltBatchExecutor executor = new BasicWindowBoltBatchExecutor(b);
        setBolt(type, id, executor, output_streams, numTasks, groups);
        return this;
    }

    /**
     * @param type
     * @param id
     * @param b
     * @param output_streams
     * @param numTasks
     * @param groups
     * @return
     */
    private TopologyBuilder setBolt(char type, String id, IExecutor b, HashMap<String, streaminfo> output_streams, int numTasks, Grouping... groups) {
        ArrayList<String> input_streams = new ArrayList<>();
        for (Grouping g : groups) {
            input_streams.add(g.getStreamID());
        }
        TopologyComponent _thisComponent;
        _thisComponent = new MultiStreamComponent(id, type, b, numTasks, input_streams, output_streams, groups);
        topology.addRecord(_thisComponent);
        for (Grouping g : groups) {
            TopologyComponent parent = topology.getComponent(g.getComponentId());
            parent.setGrouping(id, g);
            parent.setChildren(_thisComponent, g);
            _thisComponent.setParents(parent, g);
        }
        if (type == Constants.sinkType) {
            topology.setSink(_thisComponent);
        }
        return this;
    }

    public TopologyBuilder setSink(String id, AbstractBolt b, int numTasks, Grouping... groups) throws InvalidIDException {
        return setBolt(Constants.sinkType, id, b, numTasks, groups);
    }

    public void setGlobalScheduler(InputStreamController scheduler) {
        topology.setScheduler(scheduler);
    }

    public Topology createTopology() {
        if (topology.getScheduler() == null) {
            if (CONTROL.enable_log)
                LOG.info("JumboTuple scheduler is not set, use default Brisk.execution.runtime.tuple scheduler instead!");
            topology.setScheduler(new SequentialScheduler());
        }
        return topology;
    }
}
