package StreamLedger;

import StreamLedger.data.DepositEvent;
import StreamLedger.data.TransactionEvent;
import StreamLedger.data.generator.SLTPGDataGeneratorSource;
import StreamLedger.functions.MetricsRetriever;
import StreamLedger.functions.TransferDepositHandler;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Either;

import java.util.Random;

public class StreamLedger {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<Either<DepositEvent, TransactionEvent>> depositsAndTransactions = env.addSource(
                new SLTPGDataGeneratorSource()).setParallelism(1);

        depositsAndTransactions
                .map(new TransferDepositHandler())
                .name("Transaction handler")
                .setParallelism(24)
                .map(new MetricsRetriever())
                .filter((FilterFunction<String>) value -> {
                    Random random = new Random();
                    return random.nextInt() % 1000 == 0;
                })
                .print();

        // trigger program execution
        env.execute();
    }
}
