package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SmartSharedWorkload implements IScheduler, OperationChain.IOnOperationChainChangeListener {

    private static int INDEPENDENT_OCS_WITH_DEPENDENTS = 0;
    private static int DEPENDENT_OCS_WITH_DEPENDENTS = 1;
    private static int DEPENDENT_OCS_WITH_NO_DEPENDENTS = 2;
    private static int INDEPENDENT_OCS = 3;

    private ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, List<OperationChain>>> dLevelBasedOCBuckets;
//    private ConcurrentHashMap<Integer, Integer> currentDLevelToProcess;
//    private int[] totalOcsSubmittedByThread;
    private int totalOcsSubmitted;
    private int totalProcessedOcs;

    private int totalThreads;
    private int maxDLevel;


    public SmartSharedWorkload(int tp) {
        dLevelBasedOCBuckets = new ConcurrentHashMap<>();
        dLevelBasedOCBuckets.put(INDEPENDENT_OCS_WITH_DEPENDENTS, new ConcurrentHashMap<>());
        dLevelBasedOCBuckets.put(DEPENDENT_OCS_WITH_DEPENDENTS, new ConcurrentHashMap<>());
        dLevelBasedOCBuckets.put(DEPENDENT_OCS_WITH_NO_DEPENDENTS, new ConcurrentHashMap<>());
        dLevelBasedOCBuckets.put(INDEPENDENT_OCS, new ConcurrentHashMap<>());
//        totalOcsSubmittedByThread = new int[tp];

        totalThreads = tp;
    }

    @Override
    public void submitOcs(int threadId, Collection<OperationChain> ocs) {

//        totalOcsSubmittedByThread[threadId] = ocs.size();
        synchronized (this) {
            totalOcsSubmitted+=ocs.size();
        }
        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dLevel = oc.getDependencyLevel();

            if(dLevel>maxDLevel)
                maxDLevel = dLevel;

            int category = getCategory(oc);

            ConcurrentHashMap<Integer, List<OperationChain>> ocsBucket = dLevelBasedOCBuckets.get(category);
            if(!ocsBucket.containsKey(dLevel))
                ocsBucket.put(dLevel, new ArrayList<>());

            int priority = 0;
            if(category==INDEPENDENT_OCS) {
                ocsBucket.get(0).add(oc); // we keep level fixed for all independent ocs
            } else if(category==DEPENDENT_OCS_WITH_NO_DEPENDENTS) {
                priority = oc.getIndependentOpsCount();
                insertInOrder(oc, priority, ocsBucket.get(dLevel));
            } else {
                priority = oc.getImmediateResolvableDependenciesCount();
                insertInOrder(oc, priority, ocsBucket.get(dLevel));
            }

        }
    }

    private int getCategory(OperationChain oc) {
        int category = DEPENDENT_OCS_WITH_DEPENDENTS;
        if(!oc.hasDependency() && !oc.hasDependents()) {
            category = INDEPENDENT_OCS;
        } else if(!oc.hasDependency() && oc.hasDependents()){
            category = INDEPENDENT_OCS_WITH_DEPENDENTS;
        } else if(oc.hasDependency() && !oc.hasDependents()){
            category = DEPENDENT_OCS_WITH_NO_DEPENDENTS;
        }
        return category;
    }

    public synchronized void insertInOrder(OperationChain oc, int priority, List<OperationChain> operationChains) {
        if(operationChains.size() == 0 || priority > operationChains.get(operationChains.size()-1).getPriority()) {
            operationChains.add(oc);
            return;
        }

        int insertionIndex, center;
        int start = 0;
        int end = operationChains.size()-1;

        while(true) {
            center = (start+end)/2;
            if(center==start || center == end) {
                insertionIndex = start;
                break;
            }
            if(priority <= operationChains.get(center).getPriority())
                end = center;
            else
                start = center;
        }

        while(operationChains.get(insertionIndex).getPriority() < priority)
            insertionIndex++;

        while(operationChains.get(insertionIndex).getPriority() == priority &&
                operationChains.get(insertionIndex).getIndependentOpsCount() > oc.getIndependentOpsCount())
            insertionIndex++;

        operationChains.add(insertionIndex, oc); // insert using second level priority
    }

    @Override
    public void onDependencyLevelChanged(OperationChain oc) {

    }

    @Override
    public OperationChain next(int threadId) {
        OperationChain oc = getNextOc(threadId);

//        while(oc==null) {
//            if(areAllOCsScheduled(threadId))
//                break;
//            currentDLevelToProcess[threadId]+=1;
//            oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
//        }
//        while(oc!=null && oc.hasDependency());
        return oc;
    }

    private synchronized OperationChain getNextOc(int threadId) {

//        if(dLevelBasedOCBuckets.get(INDEPENDENT_OCS_WITH_DEPENDENTS).)

//        List<OperationChain> ocs = dLevelBasedOCBuckets.get(dLevel);
//        OperationChain oc = null;
//        if(ocs!=null && ocs.size()>0) {
//            oc = ocs.remove(ocs.size()-1);
//        }
//        return oc;
        return null;
    }


    @Override
    public synchronized boolean areAllOCsScheduled(int threadId){
        return totalProcessedOcs == totalOcsSubmitted;
    }

    @Override
    public void reSchedule(int threadId, OperationChain oc) {
        throw new NotImplementedException();
    }

    @Override
    public boolean isReSchedulingEnabled() {
        return false;
    }

    @Override
    public void reset() {
//        for(int lop=0; lop<currentDLevelToProcess.length; lop++) {
//            currentDLevelToProcess[lop] = 0;
//        }
//        maxDLevel = 0;
    }

}


//
//package state_engine.transaction.scheduler;
//
//import state_engine.common.Operation;
//import state_engine.common.OperationChain;
//import sun.reflect.generics.reflectiveObjects.NotImplementedException;
//
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.List;
//import java.util.Queue;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentLinkedQueue;
//
//public class SmartSharedWorkload implements IScheduler {
//
//    private Queue<OperationChain> independentOcsWithDependents;
//    private Queue<OperationChain> dependentOcsWithDependents;
//    private Queue<OperationChain> independentOcs;
//    private Queue<OperationChain> dependentOcs;
//
//    private ConcurrentHashMap<Integer, List<OperationChain>> dLevelBasedOCBuckets;
//    private int[] currentDLevelToProcess;
//
//    private int totalThreads;
//    private int maxDLevel;
//
//    public SmartSharedWorkload(int tp) {
//        independentOcsWithDependents = new ConcurrentLinkedQueue<>();
//        independentOcs = new ConcurrentLinkedQueue<>();
//        dependentOcsWithDependents = new ConcurrentLinkedQueue<>();
//        dependentOcs = new ConcurrentLinkedQueue<>();
//
////        dLevelBasedOCBuckets = new ConcurrentHashMap();
//        currentDLevelToProcess = new int[tp];
//        totalThreads = tp;
//    }
//
//    @Override
//    public void submitOcs(int threadId, Collection<OperationChain> ocs) {
//
//        for (OperationChain oc : ocs) {
//
//            if(oc.hasDependency()) {
////                if(oc.hasDependency())
//            }
////            oc.updateDependencyLevel();
////            int dLevel = oc.getDependencyLevel();
////
////            if(dLevel>maxDLevel)
////                maxDLevel = dLevel;
//
////            if(!dLevelBasedOCBuckets.containsKey(dLevel))
////                dLevelBasedOCBuckets.put(dLevel, new ArrayList<>());
////            dLevelBasedOCBuckets.get(dLevel).add(oc);
//        }
//    }
//
//    @Override
//    public OperationChain next(int threadId) {
//
//        OperationChain oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
//        while(oc==null) {
//            if(areAllOCsScheduled(threadId))
//                break;
//            currentDLevelToProcess[threadId]+=1;
//            oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
//        }
//
//
//        return oc;
//    }
//
//    private synchronized OperationChain getNextOcToProcess() {
//        return null;
//    }
//
//    private synchronized OperationChain getOcForThreadAndDLevel(int threadId, int dLevel) {
////        List<OperationChain> ocs = dLevelBasedOCBuckets.get(dLevel);
//        OperationChain oc = null;
////        if(ocs!=null && ocs.size()>0) {
////            oc = ocs.remove(ocs.size()-1);
////        }
//        return oc;
//    }
//
//    @Override
//    public synchronized boolean areAllOCsScheduled(int threadId){
//        return false;
////                currentDLevelToProcess[threadId] == maxDLevel &&
////                dLevelBasedOCBuckets.get(maxDLevel).size()==0;
//    }
//
//    @Override
//    public void reSchedule(int threadId, OperationChain oc) {
//        throw new NotImplementedException();
//    }
//
//    @Override
//    public boolean isReSchedulingEnabled() {
//        return false;
//    }
//
//    @Override
//    public void reset() {
//        for(int lop=0; lop<currentDLevelToProcess.length; lop++) {
//            currentDLevelToProcess[lop] = 0;
//        }
//        maxDLevel = 0;
//    }
//}
