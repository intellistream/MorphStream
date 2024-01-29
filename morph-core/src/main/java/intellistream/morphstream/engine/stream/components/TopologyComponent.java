package intellistream.morphstream.engine.stream.components;

import intellistream.morphstream.configuration.Constants;
import intellistream.morphstream.engine.stream.components.grouping.Grouping;
import intellistream.morphstream.engine.stream.components.operators.executor.IExecutor;
import intellistream.morphstream.engine.stream.execution.ExecutionNode;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Fields;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.*;

/**
 * Helper class. SchemaRecord all necessary information about a spout/bolt.
 */
public abstract class TopologyComponent implements Serializable {
    private static final long serialVersionUID = 3815468233004784166L;
    public final char type;
    /**
     * One can register to multiple input streams.
     */
    public final ArrayList<String> input_streams;
    final Grouping[] groups;
    /**
     * One can emit multiple output_streams with different output data (and/or different fields).
     * < StreamID, Streaminfo >
     */
    @Getter
    final HashMap<String, streaminfo> output_streams;
    @Getter
    private final String id;
    private final IExecutor op; //this is the operator structure passed in from user application.
    /**
     * Each downstream operator may subscribe using different Grouping strategies.
     *
     * @since 0.0.5: source operatorId, streamId, Grouping.
     */
    private final HashMap<String, HashMap<String, Grouping>> grouping_to_downstream;
    public boolean toCompress = true;
    @Setter
    @Getter
    private int numTasks;
    // public final String parentID;//TODO: change this partition parent limitation.
    //TODO: Currently keep two lists for faster access purpose. Replace it by using global reference in future release.
    private ArrayList<ExecutionNode> executor;//executor of this operator is initiated after Brisk.execution graph is created.
    private ArrayList<Integer> executorID;//executor of this operator is initiated after Brisk.execution graph is created.

    TopologyComponent(HashMap<String, streaminfo> output_streams, String id, char type, IExecutor op, int numTasks, ArrayList<String> input_streams, Grouping... groups) {
        this.id = id;
        this.type = type;
        this.op = op;
        this.numTasks = numTasks;//number of threads to run this bolt.
        //   this.parentID = parentID;
        this.input_streams = input_streams;
        this.output_streams = output_streams;
        this.groups = groups;
        executor = new ArrayList<>();
        executorID = new ArrayList<>();
        grouping_to_downstream = new LinkedHashMap<>();
    }

    void clean() {
        executor = new ArrayList<>();
        executorID = new ArrayList<>();
    }

    public Grouping getGrouping_to_downstream(String downOp, String streamId) {
        return grouping_to_downstream.get(downOp).get(streamId);
    }

    public ArrayList<ExecutionNode> getExecutorList() {
        return executor;
    }

    public ArrayList<Integer> getExecutorIDList() {
        return executorID;
    }

    public IExecutor getOp() {
        return op;
    }

    public void link_to_executor(ExecutionNode vertex) {
        executor.add(vertex);
        executorID.add(vertex.getExecutorID());
    }

    public void setGrouping(String downOp, Grouping g) {
        String stream = g.getStreamID();
        this.grouping_to_downstream.computeIfAbsent(downOp, k -> new HashMap<>());
        this.grouping_to_downstream.get(downOp).put(stream, g);
    }

    public abstract Set<String> get_childrenStream();

    public abstract Set<String> get_parentsStream();

    public abstract boolean isLeadNode();

    public abstract void setChildren(TopologyComponent topologyComponent, Grouping g);

    public abstract void setParents(TopologyComponent topologyComponent, Grouping g);

    /**
     * @param sourceStreamId : streamId
     * @return
     */
    public Fields get_output_fields(String sourceStreamId) {
        return output_streams.get(sourceStreamId).getFields();
    }

    public abstract Set<String> getOutput_streamsIds();

    public Map<TopologyComponent, Grouping> getChildrenOfStream() {
        return getChildrenOfStream(Constants.DEFAULT_STREAM_ID);
    }

    public abstract Map<TopologyComponent, Grouping> getChildrenOfStream(String streamId);

    public abstract Map<TopologyComponent, Grouping> getParentsOfStream(String streamId);

    public abstract HashMap<String, Map<TopologyComponent, Grouping>> getParents();

    public abstract boolean isLeafNode();

    public int getFID() {
        return op.getStage();
    }
}
