package transaction.scheduler.layered.nonhashed.sw;
import transaction.scheduler.layered.nonhashed.NonHashContext;

import java.util.Collection;
import java.util.function.Supplier;
public class SWContext<V extends Collection> extends NonHashContext<V> {
    public SWContext(int totalThreads, Supplier<V> supplier) {
        super(totalThreads, supplier);
    }

    @Override
    public boolean finished(int threadId){
        return currentLevel[threadId] == maxDLevel &&
                layeredOCBucketGlobal.get(maxDLevel).isEmpty();
    }
}
