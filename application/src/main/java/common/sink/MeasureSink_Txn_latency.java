package common.sink;
import common.collections.OsUtils;
import org.slf4j.Logger;
import execution.runtime.tuple.impl.Tuple;

import java.io.*;
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
//				final Long stored_process_latency = latency_map.getOrDefault(msgId, 0L);
//				if (process_latency > stored_process_latency)//pick the worst.
//				{
                LOG.debug("msgID:" + msgId + " is at:\t" + process_latency / 1E6 + "\tms");
                latency_map[(int) msgId] = process_latency;
//				}
                num_msg++;
            }
            if (results != 0) {
                this.setResults(results);
                LOG.info("Sink finished:" + results);
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
//                LOG.info("=====Process latency of msg====");
                latency.addValue(latency_map[key]);
            }
            try {
//                Collections.sort(col_value);
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
            LOG.info("Stop all threads sequentially");
//			context.stop_runningALL();
            context.Sequential_stopAll();
//			try {
//				//Thread.sleep(10000);
//				context.wait_for_all();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			context.force_existALL();
//			context.stop_running();
//			try {
//				Thread.sleep(10000);//sync_ratio for all sink threads stop.
//			} catch (InterruptedException e) {
//				//e.printStackTrace();
//			}
        }
    }
    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
