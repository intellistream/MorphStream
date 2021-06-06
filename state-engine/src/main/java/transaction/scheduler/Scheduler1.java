package transaction.scheduler;

import transaction.scheduler.constructor.LayeredConstructor;
import transaction.scheduler.distributor.RRDistributor;
import transaction.scheduler.picker.RandomPicker;

public class Scheduler1 extends Scheduler<LayeredConstructor, RandomPicker, RRDistributor> {

    public Scheduler1(int totalThread) {
        super(totalThread);
        this.initialize(() -> new LayeredConstructor(totalThread), () -> new RandomPicker(totalThread), () -> new RRDistributor(totalThread));
    }
}
