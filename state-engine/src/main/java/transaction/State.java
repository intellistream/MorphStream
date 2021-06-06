package transaction;

import common.tools.FastZipfGenerator;

import static common.CONTROL.enable_states_partition;

public class State {
    public static FastZipfGenerator shared_store;
    public static FastZipfGenerator[] partioned_store;

    //    public static void initilize(Configuration config) {
//        double scale_factor = config.getDouble("scale_factor", 1);
//        double theta = config.getDouble("theta", 0);
//        int tthread = config.getInt("tthread", 0);
//        int floor_interval;
//        switch (config.getString("application")) {
//
//            case "OnlineBiding": {
//                configure_store(scale_factor, theta, tthread, NUM_ITEMS);
//                break;
//            }
//            case "StreamLedger": {
//                configure_store(scale_factor, theta, tthread, NUM_ACCOUNTS);
//                break;
//            }
//            case "GrepSum": {
//                configure_store(scale_factor, theta, tthread, NUM_ITEMS);
//                break;
//            }
//
//        }
//    }
    public static void configure_store(double scale_factor, double theta, int tthread, int numItems) {
        int floor_interval;
        if (enable_states_partition) {
            floor_interval = (int) Math.floor(numItems / (double) tthread);//NUM_ITEMS / tthread;
            partioned_store = new FastZipfGenerator[tthread];//overhead_total number of working threads.
            for (int i = 0; i < tthread; i++) {
                partioned_store[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval);
            }
        } else {
            shared_store = new FastZipfGenerator((int) (numItems * scale_factor), theta, 0);
        }
    }
}
