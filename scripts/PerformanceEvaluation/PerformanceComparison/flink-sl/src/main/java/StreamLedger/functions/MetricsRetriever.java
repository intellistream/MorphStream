package StreamLedger.functions;

import StreamLedger.data.TransactionResult;
import org.apache.flink.api.common.functions.RichMapFunction;

public class MetricsRetriever extends RichMapFunction<TransactionResult, String> {

    private long start_ts = System.nanoTime();
    private long num_txn;

    public MetricsRetriever() {
        start_ts = System.nanoTime();
        num_txn = 0;
    }

    @Override
    public String map(TransactionResult value) throws Exception {
        num_txn++;
        long end_ts = System.nanoTime();
        long throughput = num_txn * (1000_000_000L) / (end_ts - start_ts);
        return "finished measurement (events/s):\t" + throughput;
    }
}
