package sesame.components;

import sesame.components.grouping.Grouping;
import sesame.components.operators.executor.IExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by shuhaozhang on 17/7/16.
 */
public class MultiStreamComponent extends TopologyComponent {


    private static final long serialVersionUID = -9089096545017261021L;
    /**
     * New added:
     * 1) children: records the downstream operator of the current operator of the streamId.
     * 2) executor: all the executors this operator own.
     * < StreamId, DownstreamOP >
     */
    private final HashMap<String, Map<TopologyComponent, Grouping>> children;
    /**
     * New added:
     * 1) parents: records the upstream operator of the current operator of the streamId.
     * 2) executor: all the executors this operator own.
     * < StreamId, upstreamOP >
     */
    private final HashMap<String, Map<TopologyComponent, Grouping>> parents;


    public MultiStreamComponent(String id, char type, IExecutor op, int numTasks, ArrayList<String> input_streams,
                                HashMap<String, streaminfo> output_streams, Grouping... groups) {
        super(output_streams, id, type, op, numTasks, input_streams, groups);
        children = new HashMap<>();
        parents = new HashMap<>();
    }

    public MultiStreamComponent(TopologyComponent topo, Topology topology) {
        super(topo.output_streams, topo.getId(), topo.type, topo.getOp(), topo.getNumTasks(), topo.input_streams, topo.groups);
        children = new HashMap<>();
        parents = new HashMap<>();
        if (groups != null) {
            for (Grouping g : groups) {
                TopologyComponent parent = topology.getComponent(g.getComponentId());
                parent.setGrouping(getId(), g);
                parent.setChildren(this, g);
                this.setParents(parent, g);
            }
        }
        this.toCompress = topo.toCompress;
    }



    /*children and Grouping to downstream structures are "upload" by children operator.*/

    @Override
    public Set<String> getOutput_streamsIds() {
        return output_streams.keySet();
    }

    public Map<TopologyComponent, Grouping> getChildrenOfStream(String streamId) {
        return children.get(streamId);
    }

    public Map<TopologyComponent, Grouping> getParentsOfStream(String streamId) {
        return parents.get(streamId);
    }

    @Override
    public HashMap<String, Map<TopologyComponent, Grouping>> getParents() {
        return parents;
    }

    @Override
    public boolean isLeafNode() {
        return children.isEmpty();
    }

    @Override
    public boolean isLeadNode() {
        return parents.isEmpty() && !this.children.isEmpty();
    }

    public void setChildren(TopologyComponent downOp, Grouping g) {
        String stream = g.getStreamID();
        Map<TopologyComponent, Grouping> childOpList = children.get(stream);
        if (childOpList == null) {
            childOpList = new HashMap<>();
        }
        childOpList.put(downOp, g);
        children.put(stream, childOpList);
    }

    public void setParents(TopologyComponent upOp, Grouping g) {
        String stream = g.getStreamID();
        Map<TopologyComponent, Grouping> parentOpList = parents.get(stream);
        if (parentOpList == null) {
            parentOpList = new HashMap<>();
        }
        parentOpList.put(upOp, g);
        parents.put(stream, parentOpList);
    }


    public boolean Output_multistream() {
        return (output_streams.size() > 1);
    }

    @Override
    public Set<String> get_childrenStream() {
        return children.keySet();
    }

    @Override
    public Set<String> get_parentsStream() {
        return parents.keySet();
    }

}
