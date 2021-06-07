package transaction.scheduler.layered.nonhashed.rr;
import transaction.scheduler.layered.nonhashed.NonHashContext;

import java.util.function.Supplier;
public class RRContext<V> extends NonHashContext<V> {
    protected int[] indexOfNextOCToProcess;
    public RRContext(int totalThread, Supplier<V> supplier) {
        super(totalThread,supplier);
        indexOfNextOCToProcess = new int[totalThread];
        for (int threadId = 0; threadId < totalThread; threadId++) {
            indexOfNextOCToProcess[threadId] = threadId;
        }
    }

    @Override
    protected void reset() {
        super.reset();
        for (int threadId = 0; threadId < totalThreads; threadId++) {
            indexOfNextOCToProcess[threadId] = threadId;
        }
    }

    @Override
    public boolean finished(int threadId){
        return currentLevel[threadId] > maxDLevel;
    }
}
