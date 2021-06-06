package transaction.scheduler;

import common.OperationChain;
import transaction.scheduler.constructor.Constructor;
import transaction.scheduler.distributor.Distributor;
import transaction.scheduler.picker.Picker;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

public abstract class Scheduler<C extends Constructor, P extends Picker, D extends Distributor> implements IScheduler {

    C constructor;
    P picker;
    D distributor;

    public Scheduler(int totalThread) {

    }

    public void initialize(Supplier<C> factory) {
        constructor = factory.get();
    }


    public void construction(int threadId, Collection<OperationChain> ocs) {
        constructor.construction(threadId, ocs);
    }

    public OperationChain pickUp(int threadId) {
        return picker.pickUp(threadId);
    }

    public void distribute(OperationChain OC) {
        distributor.distribute(OC);
    }

    public void reset() {
        constructor.reset();
        picker.reset();
        distributor.reset();
    }
}
