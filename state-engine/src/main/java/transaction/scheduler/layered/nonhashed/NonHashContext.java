package transaction.scheduler.layered.nonhashed;
import transaction.scheduler.layered.LayeredContext;

import java.util.function.Supplier;
public class NonHashContext<V> extends LayeredContext<V> {
    public Integer maxDLevel;
    public NonHashContext(int totalThreads, Supplier<V> supplier) {
        super(totalThreads, supplier);
        this.maxDLevel = 0;
    }
    public void reset() {
        super.reset();
        maxDLevel = 0;
    }
}
