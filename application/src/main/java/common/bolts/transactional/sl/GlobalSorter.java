package common.bolts.transactional.sl;

import lock.PartitionedOrderLock;
import transaction.context.TxnEvent;

import java.util.concurrent.ConcurrentSkipListSet;

public class GlobalSorter {
    public static ConcurrentSkipListSet<TxnEvent> sortedEvents = new ConcurrentSkipListSet<>();

    public static void addEvent(TxnEvent event) {
        sortedEvents.add(event);
    }
}
