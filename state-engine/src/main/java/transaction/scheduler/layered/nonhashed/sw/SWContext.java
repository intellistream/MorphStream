package transaction.scheduler.layered.nonhashed.sw;
import transaction.scheduler.layered.nonhashed.NonHashContext;

import java.util.function.Supplier;
public class SWContext<V> extends NonHashContext<V> {
    public SWContext(int totalThreads, Supplier<V> supplier) {
        super(totalThreads, supplier);
    }
}
