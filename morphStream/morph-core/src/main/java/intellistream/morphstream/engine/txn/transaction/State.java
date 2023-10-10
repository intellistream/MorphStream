package intellistream.morphstream.engine.txn.transaction;

import intellistream.morphstream.util.FastZipfGenerator;

import static intellistream.morphstream.configuration.CONTROL.enable_states_partition;

public class State {
    public static FastZipfGenerator shared_store;
    public static FastZipfGenerator[] partioned_store;

    public static void configure_store(double theta, int tthread, int numItems) {
        int floor_interval;
        if (enable_states_partition) {
            floor_interval = (int) Math.floor(numItems / (double) tthread);//NUM_ITEMS / tthread;
            partioned_store = new FastZipfGenerator[tthread];//overhead_total number of working threads.
            for (int i = 0; i < tthread; i++) {
                partioned_store[i] = new FastZipfGenerator(floor_interval, theta, i * floor_interval);
            }
        } else {
            shared_store = new FastZipfGenerator(numItems, theta, 0);
        }
    }
}
