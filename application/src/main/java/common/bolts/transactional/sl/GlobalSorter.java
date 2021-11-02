package common.bolts.transactional.sl;

import common.param.TxnEvent;

import java.util.concurrent.ConcurrentSkipListSet;

public class GlobalSorter {
    public static ConcurrentSkipListSet<TxnEvent> sortedEvents = new ConcurrentSkipListSet<>();

    public static void addEvent(TxnEvent event) {
        sortedEvents.add(event);
    }
}
