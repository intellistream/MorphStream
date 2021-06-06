package transaction.scheduler;

import common.OperationChain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * Author: Aqif Hamid
 * Concrete impl of greedy smart scheduler
 */
public class GreedySmartScheduler implements IScheduler {

    private ConcurrentLinkedQueue<OperationChain> IsolatedOC;
    private ConcurrentLinkedQueue<OperationChain> OCwithChildren;

    private final ArrayList<OperationChain>[] IsolatedOCLocal;
    private final ArrayList<OperationChain>[] OCwithChildrenLocal;

    private AtomicInteger totalSubmitted;
    private AtomicInteger totalProcessed;

    Listener listener;

    public GreedySmartScheduler(int tp) {
        IsolatedOC = new ConcurrentLinkedQueue<>();
        OCwithChildren = new ConcurrentLinkedQueue<>();

        IsolatedOCLocal = new ArrayList[tp];
        OCwithChildrenLocal = new ArrayList[tp];
        for (int tId = 0; tId < tp; tId++) {
            IsolatedOCLocal[tId] = new ArrayList<>();
            OCwithChildrenLocal[tId] = new ArrayList<>();
        }

        totalSubmitted = new AtomicInteger(0);
        totalProcessed = new AtomicInteger(0);

        listener=new Listener(IsolatedOCLocal, OCwithChildrenLocal);
    }

    @Override
    public void submitOperationChains(int threadId, Collection<OperationChain> ocs) {

        for (OperationChain oc : ocs) {
            if (!oc.hasParents() && oc.hasChildren())
                OCwithChildren.add(oc);//operation chains with children
            else if (!oc.hasParents())
                IsolatedOC.add(oc);//isolated operation chains: no parents no children.
            else
                oc.Listen(listener);//operation chains with parents listen to listener.
        }
        totalSubmitted.addAndGet(ocs.size());
    }

    @Override
    public OperationChain nextOperationChain(int threadId) {
        OperationChain oc = getOcForThreadAndDLevel(threadId);
        while (oc == null) {
            if (finishedScheduling(threadId))
                break;
            oc = getOcForThreadAndDLevel(threadId);
        }
        if (oc != null)
            totalProcessed.incrementAndGet();
        return oc;
    }

    protected OperationChain getOcForThreadAndDLevel(int threadId) {

        OperationChain oc = OCwithChildren.poll();
        if (oc == null && OCwithChildrenLocal[threadId].size() > 0) {
            oc = OCwithChildrenLocal[threadId].remove(OCwithChildrenLocal[threadId].size() - 1);
            if (OCwithChildrenLocal[threadId].size() > 0)
                OCwithChildren.addAll(OCwithChildrenLocal[threadId]);
            OCwithChildrenLocal[threadId].clear();
        }

        if (oc == null)
            oc = IsolatedOC.poll();

        if (oc == null && IsolatedOCLocal[threadId].size() > 0) {
            oc = IsolatedOCLocal[threadId].remove(IsolatedOCLocal[threadId].size() - 1);
            if (IsolatedOCLocal[threadId].size() > 0)
                IsolatedOC.addAll(IsolatedOCLocal[threadId]);
            IsolatedOCLocal[threadId].clear();
        }
        return oc;
    }

    @Override
    public boolean finishedScheduling(int threadId) {
        return totalProcessed.get() == totalSubmitted.get();
    }

    @Override
    public void reset() {
        IsolatedOC = new ConcurrentLinkedQueue<>();
        OCwithChildren = new ConcurrentLinkedQueue<>();
        totalSubmitted = new AtomicInteger(0);
        totalProcessed = new AtomicInteger(0);
    }

}
