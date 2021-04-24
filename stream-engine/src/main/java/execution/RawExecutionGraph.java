package execution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
/**
 * Created by I309939 on 11/8/2016.
 */
public class RawExecutionGraph implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(RawExecutionGraph.class);
    private static final long serialVersionUID = 5066635517599230626L;
    final ArrayList<ExecutionNode> executionNodeArrayList;
    int vertex_id = 0;//Each executor has its unique vertex id.
    RawExecutionGraph() {
        executionNodeArrayList = new ArrayList<>();
    }
    void addExecutor(ExecutionNode e) {
        executionNodeArrayList.add(e);
    }
    public ExecutionNode getExecutionNode(int id) {
        if (id == -1) {
            return null;
        }
        return executionNodeArrayList.get(id);
    }
    public ArrayList<ExecutionNode> getExecutionNodeArrayList() {
        return executionNodeArrayList;
    }
}
