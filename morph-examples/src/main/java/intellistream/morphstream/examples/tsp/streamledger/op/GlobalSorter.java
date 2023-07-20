package intellistream.morphstream.examples.tsp.streamledger.op;

import intellistream.morphstream.engine.txn.TxnEvent;

import java.util.concurrent.ConcurrentSkipListSet;

public class GlobalSorter {
    public static ConcurrentSkipListSet<TxnEvent> sortedEvents = new ConcurrentSkipListSet<>();

    public static void addEvent(TxnEvent event) {
        sortedEvents.add(event);
    }
}
