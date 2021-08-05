package transaction.scheduler.tpg.struct;

import transaction.scheduler.tpg.PartitionStateManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Controller {
    public static HashMap<Integer, Partition> threadtoStateMapping = new HashMap<>(); // <threadId, list of states being responsible for>
    public static HashMap<String, PartitionStateManager> stateToThreadMapping = new HashMap<>(); // <State, thread>
    public static HashMap<Integer, PartitionStateManager> stateManagers = new HashMap<>();
    public static ExecutorService exec = Executors.newFixedThreadPool(2);//TODO: make it to be tunable


    public static void UpdateMapping(int thread_id, String operationChainKey) {
        var list = threadtoStateMapping.computeIfAbsent(thread_id, integer -> new Partition());
        list.add(operationChainKey);
        var operationStateManager = Controller.stateManagers.computeIfAbsent(thread_id, integer -> new PartitionStateManager(thread_id));
        stateToThreadMapping.put(operationChainKey, operationStateManager);
    }

    public static class Partition extends ArrayList<String> {
    }
}
