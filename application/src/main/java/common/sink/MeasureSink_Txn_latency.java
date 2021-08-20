package common.sink;

import common.collections.OsUtils;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;

import java.io.*;

import static common.CONTROL.enable_log;

public class MeasureSink_Txn_latency extends MeasureSink_latency {
    @Override
    public void execute(Tuple input) {
        double results = helper.execute(input.getBID());
        if (isSINK) {// && cnt % 1E3 == 0
            long msgId = input.getBID();
            if (msgId < max_num_msg) {
                final long end = System.nanoTime();
                final long start = input.getLong(0);
                final long process_latency = end - start;//ns
                if (enable_log) LOG.debug("msgID:" + msgId + " is at:\t" + process_latency / 1E6 + "\tms");
                latency_map[(int) msgId] = process_latency;
                num_msg++;
            }
            if (results != 0) {
                this.setResults(results);
                if(enable_log) LOG.info("Sink finished:" + results);
                check();
            }
        }
    }

    /**
     * Only one sink will do the measure_end.
     */
    protected void check() {
        if (!profile) {
            for (int key = 0; key < max_num_msg; key++) {
                latency.addValue(latency_map[key]);
            }
            try {
                FileWriter f = null;
                f = new FileWriter(new File(metric_path + OsUtils.OS_wrapper("latency")));
                Writer w = new BufferedWriter(f);
                for (double percentile = 0.5; percentile <= 100.0; percentile += 0.5) {
                    w.write(latency.getPercentile(percentile) + "\n");
                }
                w.write("=======Details=======");
                w.write(latency.toString() + "\n");
                w.close();
                f.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(enable_log) LOG.info("Stop all threads sequentially");
            context.Sequential_stopAll();
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
