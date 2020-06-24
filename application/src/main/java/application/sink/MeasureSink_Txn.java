package application.sink;

import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static application.CONTROL.enable_latency_measurement;

public class MeasureSink_Txn extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink_Txn.class);
    private static final long serialVersionUID = 5481794109405775823L;


    @Override
    public void execute(Tuple input) {
        double results;
        results = helper.execute(input.getBID());
        if (results != 0) {
            this.setResults(results);
            LOG.info("Sink finished:" + results);
            if (thisTaskId == graph.getSink().getExecutorID()) {
                measure_end(results);
            }
        }

        if (enable_latency_measurement)
            if (isSINK) {// && cnt % 1E3 == 0
                long msgId = input.getBID();
                if (msgId < max_num_msg) {
                    final long end = System.nanoTime();

                    try {
                        final long start = input.getLong(1);


                        final long process_latency = end - start;//ns
//				final Long stored_process_latency = latency_map.getOrDefault(msgId, 0L);
//				if (process_latency > stored_process_latency)//pick the worst.
//				{
//				LOG.debug("msgID:" + msgId + " is at:\t" + process_latency / 1E6 + "\tms");
                        latency_map.add( process_latency);
//				}
                    } catch (Exception e) {
                        System.nanoTime();
                    }
//                    num_msg++;
                }

            }

    }


}
