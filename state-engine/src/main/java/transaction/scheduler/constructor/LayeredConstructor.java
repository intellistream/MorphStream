package transaction.scheduler.constructor;

import common.OperationChain;
import index.high_scale_lib.ConcurrentHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class LayeredConstructor extends Constructor {

    protected ConcurrentHashMap<Integer, List<OperationChain>> LayeredOCBuckets;

    public LayeredConstructor(int totalThread) {
        LayeredOCBuckets = new ConcurrentHashMap<>();
    }

    /**
     * Specific construction method for layered constructor.
     * TODO: insert operation chains at the correct level of the LayeredOCBuckets.
     *
     * @param threadId
     * @param ocs
     */
    public void construction(int threadId, Collection<OperationChain> ocs) {
        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dLevel = oc.getDependencyLevel();

            if (localMaxDLevel < dLevel)
                localMaxDLevel = dLevel;

            if (!dLevelBasedOCBucketsPerThread.containsKey(dLevel))
                dLevelBasedOCBucketsPerThread.put(dLevel, new ArrayList<>());
            dLevelBasedOCBucketsPerThread.get(dLevel).add(oc);
        }
    }

    public void reset() {

    }
}
