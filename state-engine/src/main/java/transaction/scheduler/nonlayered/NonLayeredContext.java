package transaction.scheduler.nonlayered;
import common.OperationChain;
import transaction.scheduler.SchedulerContext;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class NonLayeredContext extends SchedulerContext {
    public ConcurrentLinkedQueue<OperationChain> IsolatedOC;
    public ConcurrentLinkedQueue<OperationChain> OCwithChildren;

    public final ArrayList<OperationChain>[] IsolatedOCLocal;
    public final ArrayList<OperationChain>[] OCwithChildrenLocal;

    public AtomicInteger totalSubmitted;
    public AtomicInteger totalProcessed;

    Listener listener;

    public NonLayeredContext(int totalThreads) {
        IsolatedOC = new ConcurrentLinkedQueue<>();
        OCwithChildren = new ConcurrentLinkedQueue<>();

        IsolatedOCLocal = new ArrayList[totalThreads];
        OCwithChildrenLocal = new ArrayList[totalThreads];
        for (int tId = 0; tId < totalThreads; tId++) {
            IsolatedOCLocal[tId] = new ArrayList<>();
            OCwithChildrenLocal[tId] = new ArrayList<>();
        }

        totalSubmitted = new AtomicInteger(0);
        totalProcessed = new AtomicInteger(0);

        listener = new Listener(IsolatedOCLocal, OCwithChildrenLocal);
    }

    @Override
    protected void reset() {
        IsolatedOC = new ConcurrentLinkedQueue<>();
        OCwithChildren = new ConcurrentLinkedQueue<>();
        totalSubmitted = new AtomicInteger(0);
        totalProcessed = new AtomicInteger(0);
    }

    @Override
    public boolean finished(int threadId) {
        return totalProcessed.get() == totalSubmitted.get();
    }
}
