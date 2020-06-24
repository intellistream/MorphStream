package sesame.controller.output.partition.impl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
public final class TupleUtils {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.storm.utils.TupleUtils.class);
    private TupleUtils() {
        // No instantiation
    }
    //	public static boolean isTick(Tuple tuple) {
//		return tuple != null
//				&& Constants.SYSTEM_COMPONENT_ID.equals(tuple.getSourceComponent())
//				&& Constants.SYSTEM_TICK_STREAM_ID.equals(tuple.getSourceStreamId());
//	}
    public static <T> int chooseTaskIndex(List<T> keys, int numTasks) {
        return Math.floorMod(listHashCode(keys), numTasks);
    }
    public static <T> int chooseTaskIndex(int hashcode, int numTasks) {
        return Math.floorMod(hashcode, numTasks);
    }
    private static <T> int listHashCode(List<T> alist) {
        if (alist == null) {
            return 1;
        } else {
            return Arrays.deepHashCode(alist.toArray());
        }
    }
//	public static Map<String, Object> putTickFrequencyIntoComponentConfig(Map<String, Object> conf, int tickFreqSecs) {
//		if (conf == null) {
//			conf = new Config();
//		}
//
//		if (tickFreqSecs > 0) {
//			LOG.info("Enabling tick tuple with interval [{}]", tickFreqSecs);
//			conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFreqSecs);
//		}
//
//		return conf;
//	}
}
