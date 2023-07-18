package engine.txn.durability.recovery.histroyviews;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HistoryViews {
    public ConcurrentHashMap<Long, AllocationPlan> allocationPlans = new ConcurrentHashMap<>();//Group to allocationPlan
    public ConcurrentHashMap<Long, AbortViews> abortViews = new ConcurrentHashMap<>();//Group to AbortViews
    public ConcurrentHashMap<Long, DependencyViews> groupToDependencyView = new ConcurrentHashMap<>();//Group to DependencyView
    public void addAbortId(int threadId, long bid, long groupId) {
        abortViews.putIfAbsent(groupId, new AbortViews());
        abortViews.get(groupId).addAbortId(threadId, bid);
    }
    public void addDependencies(long groupId, String table, String from, String to, long bid, Object v) {
        groupToDependencyView.putIfAbsent(groupId, new DependencyViews());
        groupToDependencyView.get(groupId).addDependencies(table, from, to, bid, v);
    }
    public boolean inspectAbortView(long groupId, int threadId, long bid) {
        if (!abortViews.containsKey(groupId)) {
            return false;
        }
        boolean isAbort = abortViews.get(groupId).inspectView(threadId, bid);
        return isAbort;
    }
    public int inspectAbortNumber(long groupId, int threadId) {
        if (!abortViews.containsKey(groupId)) {
            return 0;
        }
        return abortViews.get(groupId).threadToAbortList.get(threadId).size();
    }
    public Object inspectDependencyView(long groupId, String table, String from, String to, long bid) {
        if (!groupToDependencyView.containsKey(groupId))
            return null;
        return groupToDependencyView.get(groupId).inspectView(table, from, to, bid);
    }
    public HashMap<String, List<Integer>> inspectTaskPlacing(long groupId, int threadId) {
        if (!allocationPlans.containsKey(groupId))
            return null;
        return allocationPlans.get(groupId).getPlanByThreadId(threadId);
    }
    public boolean canInspectTaskPlacing(long groupId) {
        return allocationPlans.containsKey(groupId);
    }
    public long checkGroupId(long curId) {
        for (long groupId : this.groupToDependencyView.keySet()) {
            if (groupId > curId) {
                return groupId;
            }
        }
        return 0L;
    }
    public void addAllocationPlan(long groupId, String table, int threadId, List<Integer> plan) {
        this.allocationPlans.putIfAbsent(groupId, new AllocationPlan());
        this.allocationPlans.get(groupId).addPlan(table, threadId, plan);
    }
    public boolean checkWhetherDifferentPartition(long groupId, String table, String from, String to) {
        return this.allocationPlans.get(groupId).checkWhetherDifferentPartition(table, Integer.parseInt(from), Integer.parseInt(to));
    }
}
