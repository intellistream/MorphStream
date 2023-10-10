package StreamLedger.functions;

import StreamLedger.data.DepositEvent;
import StreamLedger.data.TransactionEvent;
import StreamLedger.data.TransactionResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Either;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import redis.clients.jedis.JedisPooled;

public class TransferDepositHandlerNoLock extends RichMapFunction<Either<DepositEvent, TransactionEvent>, TransactionResult> {

    private JedisPooled jedis;

    public TransferDepositHandlerNoLock() {
    }

    @Override
    public void open(Configuration parameters) {
        jedis = new JedisPooled("localhost", 6379);
    }

    @Override
    public TransactionResult map(Either<DepositEvent, TransactionEvent> depositOrTransferEvent) throws Exception {
        if (depositOrTransferEvent.isRight()) {
            return transfer(depositOrTransferEvent.right());
        } else {
            return deposit(depositOrTransferEvent.left());
        }
    }

    public TransactionResult transfer(TransactionEvent event) {
        TransactionResult transactionResult = null;

        final long sourceAccountBalance = get(event.getSourceAccountId());
        final long sourceAssetValue = get(event.getSourceBookEntryId());
        final long targetAccountBalance = get(event.getTargetAccountId());
        final long targetAssetValue = get(event.getTargetBookEntryId());

        // check the preconditions
        if (sourceAccountBalance > event.getMinAccountBalance()
                && sourceAccountBalance > event.getAccountTransfer()
                && sourceAssetValue > event.getBookEntryTransfer()) {

            // compute the new balances
            final long newSourceBalance = sourceAccountBalance - event.getAccountTransfer();
            final long newTargetBalance = targetAccountBalance + event.getAccountTransfer();
            final long newSourceAssets = sourceAssetValue - event.getBookEntryTransfer();
            final long newTargetAssets = targetAssetValue + event.getBookEntryTransfer();

            randomDelay();
            randomDelay();
            randomDelay();
            randomDelay();

            // write back the updated values
            set(event.getSourceAccountId(), newSourceBalance);
            set(event.getTargetAccountId(), newTargetBalance);
            set(event.getSourceBookEntryId(), newSourceAssets);
            set(event.getTargetBookEntryId(), newTargetAssets);

            transactionResult = new TransactionResult(event, true, newSourceBalance, newTargetBalance);
        } else {
            // emit result with unchanged balances and a flag to mark transaction as rejected
            transactionResult = new TransactionResult(event, false, sourceAccountBalance, targetAccountBalance);
        }
        return transactionResult;
    }

    public TransactionResult deposit(DepositEvent event) {
        long newAccountValue = get(event.getAccountId()) + event.getAccountTransfer();
        long newAssetValue = get(event.getBookEntryId()) + event.getBookEntryTransfer();

        randomDelay();
        randomDelay();

        set(event.getAccountId(), newAccountValue);
        set(event.getBookEntryId(), newAssetValue);

        return new TransactionResult();
    }

    public long get(String key) {
        String value = jedis.get(key);
        return jedis.get(key) == null ? 10_000_000L : Long.parseLong(value);
    }

    public void set(String key, long value) {
        jedis.set(key, String.valueOf(value));
    }

    public void randomDelay() {
        long start = System.nanoTime();
        while (System.nanoTime() - start < 10000) {
        }
    }
}
