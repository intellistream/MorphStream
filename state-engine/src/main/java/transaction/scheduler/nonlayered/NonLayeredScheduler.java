package transaction.scheduler.nonlayered;

import common.OperationChain;
import transaction.scheduler.Scheduler;

import java.util.Collection;

public class NonLayeredScheduler extends Scheduler {

    NonLayeredContext context;

    @Override
    public void reset() {
        context.reset();
    }

    public NonLayeredScheduler(int tp) {
        context = new NonLayeredContext(tp);
    }

    @Override
    public void submitOperationChains(int threadId, Collection<OperationChain> ocs) {

        for (OperationChain oc : ocs) {
            if (!oc.hasParents() && oc.hasChildren())
                context.OCwithChildren.add(oc);//operation chains with children but with no parents.
            else if (!oc.hasParents())
                context.IsolatedOC.add(oc);//isolated operation chains: no parents no children.
            else
                oc.Listen(context.listener);//operation chains with parents add to listener to be resolved.
        }
        context.totalSubmitted.addAndGet(ocs.size());
    }

    @Override
    public OperationChain nextOperationChain(int threadId) {
        OperationChain oc = Distribute(threadId);
        while (oc == null) {
            if (finishedScheduling(threadId))
                break;
            oc = Distribute(threadId);
        }
        if (oc != null)
            context.totalProcessed.incrementAndGet();
        return oc;
    }

    @Override
    protected OperationChain Distribute(int threadId) {

        //1: First try to process oc with children but no parents.
        OperationChain oc = context.OCwithChildren.poll();
        if (oc == null && context.OCwithChildrenLocal[threadId].size() > 0) {
            //2: Then try to process oc with children and parents are resolved (added by listener).
            oc = context.OCwithChildrenLocal[threadId].remove(context.OCwithChildrenLocal[threadId].size() - 1);
            if (context.OCwithChildrenLocal[threadId].size() > 0)
                context.OCwithChildren.addAll(context.OCwithChildrenLocal[threadId]);
            context.OCwithChildrenLocal[threadId].clear();
        }

        //3: If failed, try to process oc with no childeren and no parents.
        if (oc == null)
            oc = context.IsolatedOC.poll();

        //4: If failed, try to process oc with no childeren and parents are resolved (added by listener).
        if (oc == null && context.IsolatedOCLocal[threadId].size() > 0) {
            oc = context.IsolatedOCLocal[threadId].remove(context.IsolatedOCLocal[threadId].size() - 1);
            if (context.IsolatedOCLocal[threadId].size() > 0)
                context.IsolatedOC.addAll(context.IsolatedOCLocal[threadId]);
            context.IsolatedOCLocal[threadId].clear();
        }
        return oc;
    }

    @Override
    public boolean finishedScheduling(int threadId) {
        return context.finished(threadId);
    }
}
