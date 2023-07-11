package components.context;

import components.TopologyComponent;
import components.grouping.Grouping;
import controller.input.InputStreamController;
import db.Database;
import durability.ftmanager.FTManager;
import execution.ExecutionGraph;
import execution.ExecutionNode;
import execution.runtime.executorThread;
import execution.runtime.tuple.impl.Fields;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * A TopologyContext is created for each executor.
 * It is given to bolts and spouts in their "Loading" and "open"
 * methods, respectively. This object provides information about the component's
 * place within the Brisk.topology, such as Task ids, inputs and outputs, etc.
 * <profiling/>
 */
public class TopologyContext {

    private static ExecutionGraph graph;
    private static Database db;
    private static FTManager ftManager;
    private static FTManager loggingManager;
    private static HashMap<Integer, executorThread> threadMap;
    private final int _taskId;

    /**
     * Instead of Store Brisk.topology, we Store Brisk.execution graph directly!
     * This is a global access memory structure,
     */
    public TopologyContext(ExecutionGraph g, Database db, FTManager ftManager, FTManager loggingManager, ExecutionNode executor, HashMap<Integer, executorThread> threadMap) {

        TopologyContext.graph = g;
        TopologyContext.db = db;
        TopologyContext.threadMap = threadMap;
        TopologyContext.ftManager = ftManager;
        TopologyContext.loggingManager = loggingManager;
        this._taskId = executor.getExecutorID();
    }

    public Database getDb() {
        return db;
    }
    public FTManager getFtManager() {
        return ftManager;
    }

    public FTManager getLoggingManager() {
        return loggingManager;
    }

    public HashMap<String, Map<TopologyComponent, Grouping>> getThisSources() {
        return this.getComponent(this.getThisComponentId()).getParents();
    }

    public Map<TopologyComponent, Grouping> getSources(String componentId, String StreamId) {
        return this.getComponent(componentId).getParentsOfStream(StreamId);
    }

    public String getThisComponentId() {
        return this.getComponentId(this._taskId);
    }

    public TopologyComponent getThisComponent() {
        return this.getComponent(this._taskId);
    }

    private String getComponentId(int taskId) {
        return getComponent(taskId).getId();
    }

    public ExecutionGraph getGraph() {
        return graph;
    }

    /**
     * Gets the component for the specified Task id. The component maps
     * to a component specified for a AbstractSpout or GeneralParserBolt in the Brisk.topology definition.
     *
     * @param taskId the Task id
     * @return the Operator (Brisk.topology component) for the input taskid
     */
    public TopologyComponent getComponent(int taskId) {
        return graph.getExecutionNodeArrayList().get(taskId).operator;
    }

    public ExecutionNode getExecutor(int taskId) {
        return graph.getExecutionNode(taskId);
    }

    public ArrayList<Integer> getComponentTasks(TopologyComponent component) {
        return component.getExecutorIDList();
    }

    private TopologyComponent getComponent(String component) {
        return graph.topology.getComponent(component);
    }

    /**
     * Gets the declared output fields for the specified component.
     * TODO: Add multiple stream support in future.
     *
     * @since 0.0.4 the multiple stream feature is supported.
     */
    public Fields getComponentOutputFields(String componentId, String sourceStreamId) {
        TopologyComponent op = graph.topology.getComponent(componentId);
        return op.get_output_fields(sourceStreamId);
    }

    /**
     * Gets the Task id of this Task.
     *
     * @return the Task id
     */
    public int getThisTaskId() {
        return _taskId;
    }

    public InputStreamController getScheduler() {
        return graph.getGlobal_tuple_scheduler();
    }

    public int getThisTaskIndex() {
        return getThisTaskId();
    }

    public void wait_for_all() throws InterruptedException {
        for (int id : threadMap.keySet()) {
            if (id != getThisTaskId()) {
                threadMap.get(id).join(10000);
            }
        }
    }

    public void stop_running() {
        threadMap.get(getThisTaskId()).running = false;
        threadMap.get(getThisTaskId()).interrupt();
    }

    public void force_exist() {
        threadMap.get(getThisTaskId()).running = false;
        threadMap.get(getThisTaskId()).interrupt();
    }

    public void stop_runningALL() {
        for (int id : threadMap.keySet()) {
            if (id != getThisTaskId()) {
                threadMap.get(id).running = false;
                threadMap.get(id).interrupt();
            }
        }
    }

    public void force_existALL() {
        for (int id : threadMap.keySet()) {
            if (id != getThisTaskId()) {
//                threadMap.GetAndUpdate(id).running = false;
                while (threadMap.get(id).isAlive()) {
                    threadMap.get(id).running = false;
                    threadMap.get(id).interrupt();
                }
            }
        }
    }

    public void Sequential_stopAll() {
        //interrupt everyone.
        for (int id : threadMap.keySet()) {
            if (id != getThisTaskId()) {
                threadMap.get(id).running = false;
                threadMap.get(id).interrupt();
            }
        }
        force_existALL();
        //stop myself.
        threadMap.get(getThisTaskId()).running = false;
        threadMap.get(getThisTaskId()).interrupt();
    }

    public int getNUMTasks() {
        return this.getComponent(_taskId).getNumTasks();
    }

    public void join() throws InterruptedException {
        threadMap.get(this.getGraph().getSinkThread()).join();
    }
}
