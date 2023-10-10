package intellistream.morphstream.engine.txn.durability.recovery.histroyviews;

import java.util.concurrent.ConcurrentHashMap;

import java.util.HashMap;
import java.util.List;

public class AllocationPlan {
    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, List<Integer>>> tableToPlan = new ConcurrentHashMap<>();

    public HashMap<String, List<Integer>> getPlanByThreadId(int threadId) {
        HashMap<String, List<Integer>> plan = new HashMap<>();
        for (String table : tableToPlan.keySet()) {
            plan.put(table, tableToPlan.get(table).get(threadId));
        }
        return plan;
    }

    public void addPlan(String table, int threadId, List<Integer> plan) {
        tableToPlan.putIfAbsent(table, new ConcurrentHashMap<>());
        tableToPlan.get(table).put(threadId, plan);
    }

    public boolean checkWhetherDifferentPartition(String table, int from, int to) {
        ConcurrentHashMap<Integer, List<Integer>> plans = tableToPlan.get(table);
        for (int threadId : plans.keySet()) {
            List<Integer> plan = plans.get(threadId);
            if (plan.contains(from) && plan.contains(to)) {
                return false;
            }
        }
        return true;
    }
}
