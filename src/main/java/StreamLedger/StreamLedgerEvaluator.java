package StreamLedger;

import StreamLedger.data.DepositEvent;
import StreamLedger.data.TransactionEvent;
import StreamLedger.data.generator.DepositsThenTransactionsSource;
import StreamLedger.data.generator.SyntheticSources;
import StreamLedger.functions.DepositHandler;
import StreamLedger.functions.TransferDepositHandler;
import StreamLedger.functions.TransferHandler;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Either;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.api.RedissonRxClient;
import org.redisson.config.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPooled;

public class StreamLedgerEvaluator {
//    @Deprecated
//    public void run() throws Exception {
//        // Environment setup
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // create and add two data sources
//        SyntheticSources sources = SyntheticSources.create(env, 1);
//
//        // produce the deposits transaction stream
//        DataStream<DepositEvent> deposits = sources.deposits;
//
//        // produce transactions stream
//        DataStream<TransactionEvent> transfers = sources.transactions;
//
//
//        deposits.map(new DepositHandler())
//                .setParallelism(2)
//                .name("Deposit Handler");
//
//
//        transfers.map(new TransferHandler())
//                .setParallelism(2)
//                .name("Transfer Handler");
//    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<Either<DepositEvent, TransactionEvent>> depositsAndTransactions = env.addSource(
                new DepositsThenTransactionsSource(1)).setParallelism(1);

        depositsAndTransactions
                .map(new TransferDepositHandler())
                .name("Transaction handler")
                .setParallelism(2)
                .print();

        // trigger program execution
        env.execute();
    }

}
